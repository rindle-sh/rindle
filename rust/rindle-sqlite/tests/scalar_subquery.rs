//! `TableSource` as a build-time [`ScalarSource`] (`SCALAR-SUBQUERY-DESIGN.md`
//! §4.1–4.2, §8): unique-key **discovery** from the live SQLite schema (PK +
//! secondary `UNIQUE` indexes, with PARTIAL and expression indexes excluded) and the
//! point-lookup that folds a scalar subquery against real data.
//!
//! Number columns vend as `f64`, so a correlation value read back from SQLite folds
//! to `Lit::Number` (e.g. `issueID = 2` ⇒ `Number(2.0)`) — the same literal a
//! hand-written query would carry.

use std::rc::Rc;

use rusqlite::Connection;

use rindle::scalar::{resolve_scalars, ScalarCatalog, ScalarSource};
use rindle::value::ValueType;
use rindle::{
    Ast, BuildError, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation,
    ExistsOp, Lit, Op, SimpleCondition, ValuePosition,
};
use rindle_sqlite::{ColumnDef, TableSource};

fn col(name: &str, ty: ValueType) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        ty,
        optional: false,
    }
}

/// assignee(id, issueID, email); PK id (explicit UNIQUE index — a bare INTEGER
/// PRIMARY KEY is the rowid with no listed index). `extra_ddl` injects the index
/// under test. Rows: 7→issueID 2 / a@x.com, 8→issueID 5 / b@x.com.
fn assignee_source(extra_ddl: &str) -> (Rc<Connection>, TableSource) {
    let conn = Rc::new(Connection::open_in_memory().unwrap());
    conn.execute_batch(&format!(
        "CREATE TABLE assignee (id INTEGER NOT NULL, issueID INTEGER NOT NULL, email TEXT NOT NULL);
         CREATE UNIQUE INDEX assignee_pk ON assignee (id);
         {extra_ddl}
         INSERT INTO assignee VALUES (7, 2, 'a@x.com'), (8, 5, 'b@x.com');"
    ))
    .unwrap();
    let cols = vec![
        col("id", ValueType::Number),
        col("issueID", ValueType::Number),
        col("email", ValueType::String),
    ];
    let ts = TableSource::new(conn.clone(), "assignee", cols, vec![0]);
    (conn, ts)
}

/// A one-table catalog over the assignee source.
struct Cat(TableSource);
impl ScalarCatalog for Cat {
    fn source(&self, table: &str) -> Option<&dyn ScalarSource> {
        (table == "assignee").then_some(&self.0 as &dyn ScalarSource)
    }
}

/// `issue WHERE <op> EXISTS(assignee WHERE assignee.<child_col> = key)`, correlated
/// `assignee.issueID = issue.id`, flagged `scalar`.
fn scalar_exists_ast(op: ExistsOp, child_col: &str, key: Lit) -> Ast {
    Ast {
        table: "issue".into(),
        r#where: Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["issueID".into()],
                },
                subquery: Box::new(Ast {
                    table: "assignee".into(),
                    r#where: Some(Condition::Simple(SimpleCondition {
                        op: Op::Eq,
                        left: ValuePosition::Column {
                            name: child_col.into(),
                        },
                        right: ValuePosition::Literal { value: key },
                    })),
                    ..Default::default()
                }),
                system: None,
            },
            op,
            flip: None,
            scalar: Some(true),
            plan_id: None,
        })),
        ..Default::default()
    }
}

fn parent_eq(op: Op, lit: Lit) -> Condition {
    Condition::Simple(SimpleCondition {
        op,
        left: ValuePosition::Column { name: "id".into() },
        right: ValuePosition::Literal { value: lit },
    })
}

// --- unique-key discovery (§4.1–4.2) ---------------------------------------

#[test]
fn discovers_pk_and_secondary_unique() {
    let (_c, ts) = assignee_source("CREATE UNIQUE INDEX assignee_email ON assignee (email);");
    let keys = ScalarSource::unique_keys(&ts);
    assert!(keys.contains(&vec![0]), "PK discovered: {keys:?}");
    assert!(
        keys.contains(&vec![2]),
        "secondary UNIQUE(email) discovered: {keys:?}"
    );
}

#[test]
fn skips_partial_unique_index() {
    // A PARTIAL unique index guarantees uniqueness only within its predicate — not
    // soundly inlineable, so it must NOT appear as a unique key.
    let (_c, ts) = assignee_source(
        "CREATE UNIQUE INDEX assignee_email_p ON assignee (email) WHERE issueID > 0;",
    );
    let keys = ScalarSource::unique_keys(&ts);
    assert!(keys.contains(&vec![0]), "PK still discovered: {keys:?}");
    assert!(
        !keys.contains(&vec![2]),
        "partial unique excluded: {keys:?}"
    );
}

#[test]
fn skips_expression_unique_index() {
    // An expression key column reports a NULL name; the whole index is discarded
    // (else it would look narrower than it is — the unsound direction).
    let (_c, ts) =
        assignee_source("CREATE UNIQUE INDEX assignee_lower ON assignee (lower(email));");
    let keys = ScalarSource::unique_keys(&ts);
    assert_eq!(keys, vec![vec![0]], "only the PK survives: {keys:?}");
}

// --- the fold against real SQLite data --------------------------------------

#[test]
fn folds_pk_bound_exists() {
    // assignee 7 → issueID 2 ⇒ issue.id = 2.
    let cat = Cat(assignee_source("").1);
    let resolved = resolve_scalars(
        &scalar_exists_ast(ExistsOp::Exists, "id", Lit::Number(7.0)),
        &cat,
    )
    .unwrap();
    assert_eq!(resolved.r#where, Some(parent_eq(Op::Eq, Lit::Number(2.0))));
}

#[test]
fn folds_not_exists_to_is_not() {
    let cat = Cat(assignee_source("").1);
    let resolved = resolve_scalars(
        &scalar_exists_ast(ExistsOp::NotExists, "id", Lit::Number(7.0)),
        &cat,
    )
    .unwrap();
    assert_eq!(
        resolved.r#where,
        Some(parent_eq(Op::IsNot, Lit::Number(2.0)))
    );
}

#[test]
fn folds_via_a_discovered_secondary_unique_key() {
    // Bind `email` (a discovered UNIQUE, not the PK): assignee b@x.com → issueID 5.
    // Proves discovery feeds the resolver — a non-PK unique key enables the fold.
    let cat = Cat(assignee_source("CREATE UNIQUE INDEX assignee_email ON assignee (email);").1);
    let ast = scalar_exists_ast(ExistsOp::Exists, "email", Lit::Str("b@x.com".into()));
    let resolved = resolve_scalars(&ast, &cat).unwrap();
    assert_eq!(resolved.r#where, Some(parent_eq(Op::Eq, Lit::Number(5.0))));
}

#[test]
fn absent_row_folds_to_constant_false() {
    let cat = Cat(assignee_source("").1);
    let resolved = resolve_scalars(
        &scalar_exists_ast(ExistsOp::Exists, "id", Lit::Number(999.0)),
        &cat,
    )
    .unwrap();
    // EXISTS over no row ⇒ constant-false `0 = 1`.
    assert_eq!(
        resolved.r#where,
        Some(Condition::Simple(SimpleCondition {
            op: Op::Eq,
            left: ValuePosition::Literal {
                value: Lit::Number(0.0)
            },
            right: ValuePosition::Literal {
                value: Lit::Number(1.0)
            },
        }))
    );
}

#[test]
fn binding_a_non_unique_column_is_unsupported() {
    // `issueID` is not a unique key (only the email secondary, if present, is) ⇒
    // no full unique key bound ⇒ loud error.
    let cat = Cat(assignee_source("").1);
    let ast = scalar_exists_ast(ExistsOp::Exists, "issueID", Lit::Number(2.0));
    assert!(matches!(
        resolve_scalars(&ast, &cat),
        Err(BuildError::Unsupported(_))
    ));
}
