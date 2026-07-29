//! Golden-SQL tests for `build_select_query` (spec `05` §4.4 / §10.1) — direct
//! ports of the `query-builder.test.ts` cases. Each pins the exact SQL text AND
//! the ordered bind params, so a divergence from the JS lowering is caught as a
//! string diff. The load-bearing properties: the clause-emission order
//! (constraint → multiConstraints → start → filters → ORDER BY), the
//! nullable-aware start bound (`=`/`IS`, bare `<`/`>` vs `(… IS NULL OR …)`), and
//! the multiConstraint `IN` / `(…) IN VALUES` forms.

use rindle::change::{Basis, Constraint, MultiConstraint, Start};
use rindle::{Operand, OwnedValue, Sort, SqlCondition, SqlOp, ValueType};
use rindle_sqlite::query_builder::{build_select_query, CompiledQuery, SqliteParam};
use rindle_sqlite::ColumnDef;

// issue(id:number!, owner:number?, title:string!, closed:boolean!); PK=id.
const ID: usize = 0;
const OWNER: usize = 1;
const TITLE: usize = 2;
const CLOSED: usize = 3;

fn cols() -> Vec<ColumnDef> {
    vec![
        col("id", ValueType::Number, false),
        col("owner", ValueType::Number, true),
        col("title", ValueType::String, false),
        col("closed", ValueType::Boolean, false),
    ]
}
fn col(name: &str, ty: ValueType, optional: bool) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        ty,
        optional,
    }
}

/// `Float(x)` — a number column's owned value (the JS `number` is f64; an INTEGER
/// literal in a constraint/start is still a number, so it binds as REAL here).
fn n(x: f64) -> OwnedValue {
    OwnedValue::Float(x)
}

/// Render params to a stable debug list for assertion.
fn params(c: &CompiledQuery) -> Vec<String> {
    c.params.iter().map(|p| format!("{p:?}")).collect()
}
fn pstr(s: &str) -> String {
    format!("{:?}", SqliteParam::Text(s.into()))
}
fn pint(i: i64) -> String {
    format!("{:?}", SqliteParam::Int(i))
}
fn preal(f: f64) -> String {
    format!("{:?}", SqliteParam::Real(f))
}

fn build(
    constraint: Option<&Constraint>,
    multi: &[MultiConstraint],
    filters: Option<&SqlCondition>,
    order: Option<&Sort>,
    reverse: bool,
    start: Option<&Start>,
) -> CompiledQuery {
    build_select_query(
        "issue",
        &cols(),
        constraint,
        multi,
        filters,
        order,
        reverse,
        start,
    )
}

const ALL: &str = r#"SELECT "id", "owner", "title", "closed" FROM "issue""#;

// =========================================================================
// SELECT / constraint / ORDER BY basics
// =========================================================================

#[test]
fn select_all_no_where_no_order() {
    let q = build(None, &[], None, None, false, None);
    assert_eq!(q.sql, ALL);
    assert!(q.params.is_empty());
}

#[test]
fn constraint_emits_bare_equals() {
    let c: Constraint = vec![(OWNER, n(5.0))];
    let q = build(Some(&c), &[], None, None, false, None);
    assert_eq!(q.sql, format!(r#"{ALL} WHERE "owner" = ?"#));
    assert_eq!(params(&q), vec![preal(5.0)]);
}

#[test]
fn multi_column_constraint_is_anded() {
    let c: Constraint = vec![(OWNER, n(5.0)), (CLOSED, OwnedValue::Bool(true))];
    let q = build(Some(&c), &[], None, None, false, None);
    assert_eq!(
        q.sql,
        format!(r#"{ALL} WHERE "owner" = ? AND "closed" = ?"#)
    );
    assert_eq!(params(&q), vec![preal(5.0), pint(1)]);
}

#[test]
fn order_by_forward_and_reverse() {
    let order: Sort = vec![(ID, true)];
    let fwd = build(None, &[], None, Some(&order), false, None);
    assert_eq!(fwd.sql, format!(r#"{ALL} ORDER BY "id" asc"#));
    let rev = build(None, &[], None, Some(&order), true, None);
    assert_eq!(rev.sql, format!(r#"{ALL} ORDER BY "id" desc"#));
}

// =========================================================================
// Start bound — nullable-aware (§3.7) + mixed-direction (§3.4)
// =========================================================================

#[test]
fn start_after_single_nonoptional_uses_bare_gt() {
    let order: Sort = vec![(ID, true)];
    let start = Start {
        row: row4(10.0, 0.0, "", false),
        basis: Basis::After,
    };
    let q = build(None, &[], None, Some(&order), false, Some(&start));
    // `id` is non-nullable → the redundant sargable `"id" >= ?` seek prefix is
    // prepended ahead of the (non-sargable) OR-of-ANDs disjunction (§3.7 perf).
    assert_eq!(
        q.sql,
        format!(r#"{ALL} WHERE "id" >= ? AND (("id" > ?)) ORDER BY "id" asc"#)
    );
    assert_eq!(params(&q), vec![preal(10.0), preal(10.0)]);
}

#[test]
fn start_at_appends_all_equality_term() {
    let order: Sort = vec![(ID, true)];
    let start = Start {
        row: row4(10.0, 0.0, "", false),
        basis: Basis::At,
    };
    let q = build(None, &[], None, Some(&order), false, Some(&start));
    assert_eq!(
        q.sql,
        format!(r#"{ALL} WHERE "id" >= ? AND (("id" > ?) OR ("id" = ?)) ORDER BY "id" asc"#)
    );
    assert_eq!(params(&q), vec![preal(10.0), preal(10.0), preal(10.0)]);
}

#[test]
fn start_after_optional_column_wraps_is_null() {
    // owner is optional + asc → range '>' wraps as `(? IS NULL OR "owner" > ?)`
    // (value bound TWICE); id non-optional keeps bare `=` / `>`.
    // Regression guard: the LEADING column (owner) is nullable, so NO sargable seek
    // prefix is emitted — the query is the pure OR-of-ANDs, unchanged from the JS.
    let order: Sort = vec![(OWNER, true), (ID, true)];
    let start = Start {
        row: row4(10.0, 5.0, "", false),
        basis: Basis::After,
    };
    let q = build(None, &[], None, Some(&order), false, Some(&start));
    // Triple open paren: the OR-wrap, the i=0 group-wrap, and the range `IS NULL`
    // wrap — exactly as `gatherStartConstraints` nests them (query-builder.ts:374).
    assert_eq!(
        q.sql,
        format!(
            r#"{ALL} WHERE (((? IS NULL OR "owner" > ?)) OR ("owner" IS ? AND "id" > ?)) ORDER BY "owner" asc, "id" asc"#
        )
    );
    // owner=5 twice (the IS NULL + the >), owner=5 (the equality prefix), id=10.
    assert_eq!(
        params(&q),
        vec![preal(5.0), preal(5.0), preal(5.0), preal(10.0)]
    );
}

#[test]
fn start_after_mixed_direction_flips_operator_per_part() {
    // order id ASC, owner DESC. After: id>v OR (id=v AND owner<v). owner optional +
    // '<' → `("owner" IS NULL OR "owner" < ?)`.
    let order: Sort = vec![(ID, true), (OWNER, false)];
    let start = Start {
        row: row4(10.0, 5.0, "", false),
        basis: Basis::After,
    };
    let q = build(None, &[], None, Some(&order), false, Some(&start));
    // Leading column `id` is non-nullable → sargable `"id" >= ?` prefix; the nullable
    // tiebreak `owner` still gets its `(… IS NULL OR …)` wrap inside the disjunction.
    assert_eq!(
        q.sql,
        format!(
            r#"{ALL} WHERE "id" >= ? AND (("id" > ?) OR ("id" = ? AND ("owner" IS NULL OR "owner" < ?))) ORDER BY "id" asc, "owner" desc"#
        )
    );
    assert_eq!(
        params(&q),
        vec![preal(10.0), preal(10.0), preal(10.0), preal(5.0)]
    );
}

#[test]
fn reverse_flips_start_operators_too() {
    // id ASC, After, reverse=true → operator flips to '<', ORDER BY flips to desc.
    // The sargable prefix flips with it: effective-descending leading col → `"id" <= ?`.
    let order: Sort = vec![(ID, true)];
    let start = Start {
        row: row4(10.0, 0.0, "", false),
        basis: Basis::After,
    };
    let q = build(None, &[], None, Some(&order), true, Some(&start));
    assert_eq!(
        q.sql,
        format!(r#"{ALL} WHERE "id" <= ? AND (("id" < ?)) ORDER BY "id" desc"#)
    );
    assert_eq!(params(&q), vec![preal(10.0), preal(10.0)]);
}

#[test]
fn start_bound_sargable_prefix_only_on_leading_column() {
    // Two non-nullable sort columns. Only the LEADING one (`id`) is entailed by every
    // disjunct, so only it gets the redundant sargable seek prefix; `title` stays a
    // pure filter inside the OR-of-ANDs.
    let order: Sort = vec![(ID, true), (TITLE, true)];
    let start = Start {
        row: row4(10.0, 0.0, "hello", false),
        basis: Basis::After,
    };
    let q = build(None, &[], None, Some(&order), false, Some(&start));
    assert_eq!(
        q.sql,
        format!(
            r#"{ALL} WHERE "id" >= ? AND (("id" > ?) OR ("id" = ? AND "title" > ?)) ORDER BY "id" asc, "title" asc"#
        )
    );
    assert_eq!(
        params(&q),
        vec![preal(10.0), preal(10.0), preal(10.0), pstr("hello")]
    );
}

#[test]
fn start_bound_descending_leading_uses_le_prefix() {
    // Leading `id` sorted DESC (via the sort direction, not `reverse`): the seek
    // prefix flips to `<=`, matching group 0's `<`.
    let order: Sort = vec![(ID, false)];
    let start = Start {
        row: row4(10.0, 0.0, "", false),
        basis: Basis::After,
    };
    let q = build(None, &[], None, Some(&order), false, Some(&start));
    assert_eq!(
        q.sql,
        format!(r#"{ALL} WHERE "id" <= ? AND (("id" < ?)) ORDER BY "id" desc"#)
    );
    assert_eq!(params(&q), vec![preal(10.0), preal(10.0)]);
}

#[test]
fn start_bound_no_sargable_prefix_for_nullable_leading_single() {
    // A single nullable leading column: no bare sargable bound is sound (Zero's
    // `null < everything` + the `? IS NULL` degenerate), so the query is the pure
    // OR-of-ANDs — no `>=`/`<=` prefix.
    let order: Sort = vec![(OWNER, true)];
    let start = Start {
        row: row4(0.0, 5.0, "", false),
        basis: Basis::After,
    };
    let q = build(None, &[], None, Some(&order), false, Some(&start));
    // Triple paren: disjunction-wrap, single-group-wrap, and the range `IS NULL` wrap.
    assert_eq!(
        q.sql,
        format!(r#"{ALL} WHERE (((? IS NULL OR "owner" > ?))) ORDER BY "owner" asc"#)
    );
    assert_eq!(params(&q), vec![preal(5.0), preal(5.0)]);
}

// =========================================================================
// multiConstraint — single-column IN / compound VALUES (§3.3)
// =========================================================================

#[test]
fn multi_constraint_single_column_emits_in_list() {
    let mc: MultiConstraint = vec![
        vec![(OWNER, n(1.0))],
        vec![(OWNER, n(2.0))],
        vec![(OWNER, n(3.0))],
    ];
    let q = build(None, &[mc], None, None, false, None);
    assert_eq!(q.sql, format!(r#"{ALL} WHERE "owner" IN (?, ?, ?)"#));
    assert_eq!(params(&q), vec![preal(1.0), preal(2.0), preal(3.0)]);
}

#[test]
fn multi_constraint_compound_emits_row_value_values() {
    let mc: MultiConstraint = vec![
        vec![(ID, n(1.0)), (OWNER, n(10.0))],
        vec![(ID, n(2.0)), (OWNER, n(20.0))],
    ];
    let q = build(None, &[mc], None, None, false, None);
    assert_eq!(
        q.sql,
        format!(r#"{ALL} WHERE ("id", "owner") IN (VALUES (?, ?), (?, ?))"#)
    );
    assert_eq!(
        params(&q),
        vec![preal(1.0), preal(10.0), preal(2.0), preal(20.0)]
    );
}

#[test]
fn empty_multi_constraint_is_skipped() {
    let q = build(None, &[vec![]], None, None, false, None);
    assert_eq!(q.sql, ALL);
    assert!(q.params.is_empty());
}

// =========================================================================
// Filters (query-builder.test.ts `optional filters to sql`)
// =========================================================================

#[test]
fn filter_simple_emits_op_verbatim() {
    let f = SqlCondition::Simple {
        left: Operand::Column(TITLE),
        op: SqlOp::Eq,
        right: Operand::Literal(OwnedValue::str("x")),
    };
    let q = build(None, &[], Some(&f), None, false, None);
    assert_eq!(q.sql, format!(r#"{ALL} WHERE "title" = ?"#));
    assert_eq!(params(&q), vec![pstr("x")]);
}

#[test]
fn filter_and_or_nest_with_parens() {
    let f = SqlCondition::And(vec![
        SqlCondition::Simple {
            left: Operand::Column(OWNER),
            op: SqlOp::Eq,
            right: Operand::Literal(n(5.0)),
        },
        SqlCondition::Or(vec![
            SqlCondition::Simple {
                left: Operand::Column(CLOSED),
                op: SqlOp::Eq,
                right: Operand::Literal(OwnedValue::Bool(true)),
            },
            SqlCondition::Simple {
                left: Operand::Column(TITLE),
                op: SqlOp::Ne,
                right: Operand::Literal(OwnedValue::str("x")),
            },
        ]),
    ]);
    let q = build(None, &[], Some(&f), None, false, None);
    assert_eq!(
        q.sql,
        format!(r#"{ALL} WHERE ("owner" = ? AND ("closed" = ? OR "title" != ?))"#)
    );
    assert_eq!(params(&q), vec![preal(5.0), pint(1), pstr("x")]);
}

#[test]
fn filter_empty_and_is_true_empty_or_is_false() {
    let t = build(
        None,
        &[],
        Some(&SqlCondition::And(vec![])),
        None,
        false,
        None,
    );
    assert_eq!(t.sql, format!("{ALL} WHERE TRUE"));
    let f = build(
        None,
        &[],
        Some(&SqlCondition::Or(vec![])),
        None,
        false,
        None,
    );
    assert_eq!(f.sql, format!("{ALL} WHERE FALSE"));
}

#[test]
fn filter_in_lowers_to_json_each() {
    let f = SqlCondition::Simple {
        left: Operand::Column(ID),
        op: SqlOp::In,
        right: Operand::Literal(OwnedValue::Json("[1,2,3]".into())),
    };
    let q = build(None, &[], Some(&f), None, false, None);
    assert_eq!(
        q.sql,
        format!(r#"{ALL} WHERE "id" IN (SELECT value FROM json_each(?))"#)
    );
    assert_eq!(params(&q), vec![pstr("[1,2,3]")]);
}

#[test]
fn filter_is_and_is_not_emit_verbatim() {
    // The NULL-aware equality operators reach the WHERE verbatim (a user
    // `.where(c, 'IS', null)` / the `NOT EXISTS` → `field IS NOT literal` rewrite),
    // with the literal null bound as a param (`col IS ?` ≡ `col IS NULL`).
    let is = SqlCondition::Simple {
        left: Operand::Column(OWNER),
        op: SqlOp::Is,
        right: Operand::Literal(OwnedValue::Null),
    };
    let q = build(None, &[], Some(&is), None, false, None);
    assert_eq!(q.sql, format!(r#"{ALL} WHERE "owner" IS ?"#));
    assert_eq!(params(&q), vec![format!("{:?}", SqliteParam::Null)]);

    let is_not = SqlCondition::Simple {
        left: Operand::Column(OWNER),
        op: SqlOp::IsNot,
        right: Operand::Literal(OwnedValue::Null),
    };
    let q2 = build(None, &[], Some(&is_not), None, false, None);
    assert_eq!(q2.sql, format!(r#"{ALL} WHERE "owner" IS NOT ?"#));
    assert_eq!(params(&q2), vec![format!("{:?}", SqliteParam::Null)]);
}

#[test]
fn filter_like_is_case_sensitive_with_escape() {
    let f = SqlCondition::Simple {
        left: Operand::Column(TITLE),
        op: SqlOp::Like,
        right: Operand::Literal(OwnedValue::str("a%")),
    };
    let q = build(None, &[], Some(&f), None, false, None);
    assert_eq!(q.sql, format!(r#"{ALL} WHERE "title" LIKE ? ESCAPE '\'"#));
    assert_eq!(params(&q), vec![pstr("a%")]);
}

#[test]
fn filter_ilike_lowers_both_sides() {
    let f = SqlCondition::Simple {
        left: Operand::Column(TITLE),
        op: SqlOp::Ilike,
        right: Operand::Literal(OwnedValue::str("a%")),
    };
    let q = build(None, &[], Some(&f), None, false, None);
    assert_eq!(
        q.sql,
        format!(r#"{ALL} WHERE lower("title") LIKE lower(?) ESCAPE '\'"#)
    );
    assert_eq!(params(&q), vec![pstr("a%")]);
}

// =========================================================================
// Clause emission order: constraint → multi → start → filter → ORDER BY
// =========================================================================

#[test]
fn full_clause_order_and_param_alignment() {
    let c: Constraint = vec![(OWNER, n(7.0))];
    let mc: MultiConstraint = vec![vec![(ID, n(1.0))], vec![(ID, n(2.0))]];
    let order: Sort = vec![(ID, true)];
    let start = Start {
        row: row4(3.0, 7.0, "", false),
        basis: Basis::After,
    };
    let f = SqlCondition::Simple {
        left: Operand::Column(TITLE),
        op: SqlOp::Eq,
        right: Operand::Literal(OwnedValue::str("z")),
    };
    let q = build(Some(&c), &[mc], Some(&f), Some(&order), false, Some(&start));
    // The sargable `"id" >= ?` prefix slots inside the start term, between the
    // multiConstraint and the OR-of-ANDs — param alignment must survive it.
    assert_eq!(
        q.sql,
        format!(
            r#"{ALL} WHERE "owner" = ? AND "id" IN (?, ?) AND "id" >= ? AND (("id" > ?)) AND "title" = ? ORDER BY "id" asc"#
        )
    );
    // constraint(7) → multi(1,2) → start seek-prefix(3) + disjunction(3) → filter("z")
    assert_eq!(
        params(&q),
        vec![
            preal(7.0),
            preal(1.0),
            preal(2.0),
            preal(3.0),
            preal(3.0),
            pstr("z")
        ]
    );
}

// --- helpers -------------------------------------------------------------

fn row4(id: f64, owner: f64, title: &str, closed: bool) -> rindle::OwnedRow {
    rindle::owned_row(vec![
        OwnedValue::Float(id),
        OwnedValue::Float(owner),
        OwnedValue::str(title),
        OwnedValue::Bool(closed),
    ])
}
