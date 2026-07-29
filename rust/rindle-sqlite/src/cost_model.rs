//! `SqliteCostModel` — a planner [`ConnectionCostModel`] backed by real SQLite
//! statistics (port of `zqlite/src/sqlite-cost-model.ts`).
//!
//! For each table scan the planner asks about, we build a single-table `SELECT`
//! (constraint columns as `= ?` placeholders, filters with their literals inlined so
//! SQLite's planner sees real values, plus the `ORDER BY`), prepare it, and read the
//! query planner's per-loop estimates via `scanstatus`:
//!
//! - **rows** = the main scan loop's estimated row count.
//! - **startup_cost** = `btree_cost(rows)` for each top-level `ORDER BY` sort loop.
//! - **fanout** = from `SQLiteStatFanout` (`stat4`/`stat1`).
//!
//! This is a faithful *logic* port, not a byte-for-byte JS differential: the row /
//! fanout numbers come from this build's SQLite + `ANALYZE` stats, which differ from
//! the JS engine's. It is validated by real-SQLite integration tests.

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_void};
use std::rc::Rc;

use rindle::{Condition, Dir, Lit, Op, OrderPart, SimpleCondition, ValuePosition};
use rindle_planner::{ConnectionCostModel, CostModelCost, PlannerConstraint};
use rusqlite::{ffi, Connection};

use crate::query_builder::ident;
use crate::stat_fanout::SQLiteStatFanout;

/// SQLite's default fanout when statistics are unavailable (`SQLiteStatFanout`'s
/// default).
pub const DEFAULT_FANOUT: f64 = 3.0;

/// A cost model that estimates table-scan cost from a live SQLite connection.
///
/// Holds a shared [`Connection`] (via `Rc`, since the planner takes an owned
/// `Rc<dyn ConnectionCostModel>`); statements are prepared on demand against it.
pub struct SqliteCostModel {
    conn: Rc<Connection>,
    fanout: Rc<SQLiteStatFanout>,
}

impl SqliteCostModel {
    pub fn new(conn: Rc<Connection>) -> Self {
        let fanout = Rc::new(SQLiteStatFanout::new(conn.clone(), DEFAULT_FANOUT));
        Self { conn, fanout }
    }
}

impl ConnectionCostModel for SqliteCostModel {
    fn estimate(
        &self,
        table: &str,
        sort: &[OrderPart],
        filters: Option<&Condition>,
        constraint: Option<&PlannerConstraint>,
    ) -> CostModelCost {
        // The cost model can't estimate correlated subqueries, so strip them
        // (conservative — the real cost may be higher).
        let no_sub_filters = filters.and_then(remove_correlated_subqueries);

        let sql = build_cost_sql(table, sort, no_sub_filters.as_ref(), constraint);
        let loops = unsafe { read_scanstatus_loops(&self.conn, &sql) };
        assert!(
            !loops.is_empty(),
            "scanstatus returned no loops for cost query: {sql}"
        );
        let (rows, startup_cost) = estimate_cost(&loops);

        let fanout = self.fanout.clone();
        let table: Box<str> = Box::from(table);
        CostModelCost {
            rows,
            startup_cost,
            fanout: Rc::new(move |columns| fanout.get_fanout(&table, columns)),
        }
    }
}

// ---------------------------------------------------------------------------
// scanstatus reading (raw FFI)
// ---------------------------------------------------------------------------

/// One `scanstatus` loop: the planner's per-loop estimate + EXPLAIN text.
pub(crate) struct ScanLoop {
    pub select_id: i32,
    pub parent_id: i32,
    pub est: f64,
    pub explain: String,
}

/// Raw-prepare `sql`, read all `scanstatus` loops (`SQLITE_SCANSTAT_COMPLEX` so sort
/// loops are included), then finalize. `est`/`explain` are planner estimates available
/// after prepare (no execution needed). Panics if `sql` fails to prepare (a cost-model
/// invariant); a caller that must not panic uses [`try_read_scanstatus_loops`].
pub(crate) unsafe fn read_scanstatus_loops(conn: &Connection, sql: &str) -> Vec<ScanLoop> {
    try_read_scanstatus_loops(conn, sql)
        .unwrap_or_else(|| panic!("cost-model prepare failed for: {sql}"))
}

/// [`read_scanstatus_loops`] that returns `None` when `sql` won't prepare (interior NUL or
/// a SQLite prepare error) instead of panicking — for the `analyze` diagnostic, which must
/// never bring down the daemon on a malformed leaf `SELECT`.
pub(crate) unsafe fn try_read_scanstatus_loops(
    conn: &Connection,
    sql: &str,
) -> Option<Vec<ScanLoop>> {
    let db = conn.handle();
    let csql = CString::new(sql).ok()?;
    let mut stmt: *mut ffi::sqlite3_stmt = std::ptr::null_mut();
    let rc = ffi::sqlite3_prepare_v2(db, csql.as_ptr(), -1, &mut stmt, std::ptr::null_mut());
    if rc != ffi::SQLITE_OK {
        return None;
    }

    let flags = ffi::SQLITE_SCANSTAT_COMPLEX as c_int;
    let mut out = Vec::new();
    let mut idx: c_int = 0;
    loop {
        let mut select_id: c_int = 0;
        let rc = ffi::sqlite3_stmt_scanstatus_v2(
            stmt,
            idx,
            ffi::SQLITE_SCANSTAT_SELECTID as c_int,
            flags,
            &mut select_id as *mut c_int as *mut c_void,
        );
        if rc != 0 {
            break; // idx past the last loop
        }
        let mut parent_id: c_int = 0;
        ffi::sqlite3_stmt_scanstatus_v2(
            stmt,
            idx,
            ffi::SQLITE_SCANSTAT_PARENTID as c_int,
            flags,
            &mut parent_id as *mut c_int as *mut c_void,
        );
        let mut est: f64 = 0.0;
        ffi::sqlite3_stmt_scanstatus_v2(
            stmt,
            idx,
            ffi::SQLITE_SCANSTAT_EST as c_int,
            flags,
            &mut est as *mut f64 as *mut c_void,
        );
        let mut explain_ptr: *const c_char = std::ptr::null();
        ffi::sqlite3_stmt_scanstatus_v2(
            stmt,
            idx,
            ffi::SQLITE_SCANSTAT_EXPLAIN as c_int,
            flags,
            &mut explain_ptr as *mut *const c_char as *mut c_void,
        );
        let explain = if explain_ptr.is_null() {
            String::new()
        } else {
            CStr::from_ptr(explain_ptr).to_string_lossy().into_owned()
        };

        out.push(ScanLoop {
            select_id,
            parent_id,
            est,
            explain,
        });
        idx += 1;
    }
    ffi::sqlite3_finalize(stmt);
    Some(out)
}

/// The SQLite access-path text for `sql` — the planner's per-loop `EXPLAIN` lines
/// (`"SCAN comments"`, `"SEARCH comments USING INDEX …"`, `"USE TEMP B-TREE FOR ORDER BY"`),
/// joined with `"; "` when a leaf has several. Read off a fresh prepare (no execution), so
/// it is exactly the plan SQLite chose for that `SELECT`. `None` if `sql` won't prepare or
/// the plan exposes no loop text. `analyze query` shows this beside each leaf's
/// scanned/emitted ratio: a `SCAN` on an amplified leaf is the smoking gun the ratio flags.
#[cfg(feature = "scan-stats")]
pub fn explain_plan(conn: &Connection, sql: &str) -> Option<String> {
    let loops = unsafe { try_read_scanstatus_loops(conn, sql)? };
    let text: Vec<String> = loops
        .into_iter()
        .map(|l| l.explain)
        .filter(|e| !e.trim().is_empty())
        .collect();
    (!text.is_empty()).then(|| text.join("; "))
}

// ---------------------------------------------------------------------------
// cost estimation (`estimateCost` / `btreeCost`)
// ---------------------------------------------------------------------------

/// `estimateCost`: rows = the first top-level (`parentId == 0`) loop's estimate;
/// startup cost accrues `btree_cost(rows)` for each later top-level `ORDER BY` loop.
fn estimate_cost(loops: &[ScanLoop]) -> (f64, f64) {
    let mut top: Vec<&ScanLoop> = loops.iter().filter(|l| l.parent_id == 0).collect();
    top.sort_by_key(|l| l.select_id);

    let mut total_rows = 0.0;
    let mut total_cost = 0.0;
    let mut first = true;
    for op in top {
        if first {
            total_rows = op.est;
            first = false;
        } else if op.explain.contains("ORDER BY") {
            total_cost += btree_cost(total_rows);
        }
    }
    (total_rows, total_cost)
}

/// `btreeCost` (`sqlite-cost-model.ts`): `(rows * log2(rows)) / 10` — O(n log n) sort,
/// `/10` because SQLite sorts ~10× faster than pulling rows into the host. Operation
/// order preserved (multiply then divide).
pub fn btree_cost(rows: f64) -> f64 {
    (rows * rows.log2()) / 10.0
}

// ---------------------------------------------------------------------------
// cost-query SQL (single-table SELECT with inlined filter literals)
// ---------------------------------------------------------------------------

/// `removeCorrelatedSubqueries`: drop EXISTS / NOT EXISTS from the filter tree (the
/// cost model can't price them); collapse empty / singleton AND/OR.
fn remove_correlated_subqueries(condition: &Condition) -> Option<Condition> {
    match condition {
        Condition::CorrelatedSubquery(_) => None,
        Condition::Simple(_) => Some(condition.clone()),
        Condition::And { conditions } => {
            let kept: Vec<Condition> = conditions
                .iter()
                .filter_map(remove_correlated_subqueries)
                .collect();
            collapse(kept, true)
        }
        Condition::Or { conditions } => {
            let kept: Vec<Condition> = conditions
                .iter()
                .filter_map(remove_correlated_subqueries)
                .collect();
            collapse(kept, false)
        }
    }
}

fn collapse(mut kept: Vec<Condition>, is_and: bool) -> Option<Condition> {
    match kept.len() {
        0 => None,
        1 => kept.pop(),
        _ => Some(if is_and {
            Condition::And { conditions: kept }
        } else {
            Condition::Or { conditions: kept }
        }),
    }
}

/// Build the single-table cost query: `SELECT * FROM table [WHERE …] [ORDER BY …]`.
/// `SELECT *` is faithful for the planner (a full-row fetch, like the JS column list);
/// constraint columns are `= ?` (value-less placeholders, so SQLite uses the index via
/// the average), and `filters` inline their literals.
fn build_cost_sql(
    table: &str,
    sort: &[OrderPart],
    filters: Option<&Condition>,
    constraint: Option<&PlannerConstraint>,
) -> String {
    let mut sql = format!("SELECT * FROM {}", ident(table));

    let mut wheres: Vec<String> = Vec::new();
    if let Some(c) = constraint {
        for col in c {
            wheres.push(format!("{} = ?", ident(col)));
        }
    }
    if let Some(f) = filters {
        wheres.push(condition_to_sql(f));
    }
    if !wheres.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&wheres.join(" AND "));
    }

    if !sort.is_empty() {
        sql.push_str(" ORDER BY ");
        let parts: Vec<String> = sort
            .iter()
            .map(|p| {
                let dir = match p.dir() {
                    Dir::Asc => "ASC",
                    Dir::Desc => "DESC",
                };
                format!("{} {}", ident(p.field()), dir)
            })
            .collect();
        sql.push_str(&parts.join(", "));
    }

    sql
}

fn condition_to_sql(condition: &Condition) -> String {
    match condition {
        Condition::Simple(s) => simple_to_sql(s),
        Condition::And { conditions } => group(conditions, "AND"),
        Condition::Or { conditions } => group(conditions, "OR"),
        // Removed by remove_correlated_subqueries before we get here.
        Condition::CorrelatedSubquery(_) => unreachable!("subqueries stripped for cost SQL"),
    }
}

fn group(conditions: &[Condition], op: &str) -> String {
    let inner: Vec<String> = conditions.iter().map(condition_to_sql).collect();
    format!("({})", inner.join(&format!(" {op} ")))
}

fn simple_to_sql(s: &SimpleCondition) -> String {
    let left = value_position_to_sql(&s.left);
    match s.op {
        Op::In | Op::NotIn => {
            let op = if s.op == Op::In { "IN" } else { "NOT IN" };
            let elems = match &s.right {
                ValuePosition::Literal {
                    value: Lit::Array(items),
                } => items.iter().map(inline_lit).collect::<Vec<_>>().join(", "),
                other => value_position_to_sql(other),
            };
            format!("{left} {op} ({elems})")
        }
        _ => format!(
            "{left} {} {}",
            op_to_sql(s.op),
            value_position_to_sql(&s.right)
        ),
    }
}

fn value_position_to_sql(v: &ValuePosition) -> String {
    match v {
        ValuePosition::Column { name } => ident(name),
        ValuePosition::Literal { value } => inline_lit(value),
    }
}

fn op_to_sql(op: Op) -> &'static str {
    match op {
        Op::Eq => "=",
        Op::Ne => "!=",
        Op::Lt => "<",
        Op::Le => "<=",
        Op::Gt => ">",
        Op::Ge => ">=",
        Op::Is => "IS",
        Op::IsNot => "IS NOT",
        // For cost estimation, the case-insensitive ILIKE collapses to LIKE; the exact
        // collation does not change the scan plan SQLite picks.
        Op::Like | Op::ILike => "LIKE",
        Op::NotLike | Op::NotILike => "NOT LIKE",
        Op::In => "IN",
        Op::NotIn => "NOT IN",
    }
}

/// Inline a literal into the cost SQL (`compileInline` / `inlineValue`).
fn inline_lit(lit: &Lit) -> String {
    match lit {
        Lit::Null => "NULL".to_string(),
        Lit::Bool(b) => if *b { "1" } else { "0" }.to_string(),
        Lit::Int(i) => i.to_string(),
        Lit::Number(n) => format!("{n}"),
        Lit::Str(s) => format!("'{}'", s.replace('\'', "''")),
        Lit::Array(items) => {
            // A bare array literal (arrays normally appear in IN, handled above). Emit a
            // JSON-text form so the SQL stays valid.
            let inner: Vec<String> = items.iter().map(json_lit).collect();
            format!("'[{}]'", inner.join(","))
        }
    }
}

fn json_lit(lit: &Lit) -> String {
    match lit {
        Lit::Null => "null".to_string(),
        Lit::Bool(b) => b.to_string(),
        Lit::Int(i) => i.to_string(),
        Lit::Number(n) => format!("{n}"),
        Lit::Str(s) => format!("{s:?}"),
        Lit::Array(items) => {
            let inner: Vec<String> = items.iter().map(json_lit).collect();
            format!("[{}]", inner.join(","))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cset(items: &[&str]) -> PlannerConstraint {
        items.iter().map(|s| Box::from(*s)).collect()
    }

    #[test]
    fn build_cost_sql_shapes() {
        // No filters/constraint/sort → bare scan.
        assert_eq!(
            build_cost_sql("users", &[], None, None),
            r#"SELECT * FROM "users""#
        );

        // Constraint column → `= ?` placeholder.
        assert_eq!(
            build_cost_sql("posts", &[], None, Some(&cset(&["userId"]))),
            r#"SELECT * FROM "posts" WHERE "userId" = ?"#
        );

        // Sort → ORDER BY.
        let sort = vec![OrderPart(Box::from("created"), Dir::Desc)];
        assert_eq!(
            build_cost_sql("posts", &sort, None, Some(&cset(&["userId"]))),
            r#"SELECT * FROM "posts" WHERE "userId" = ? ORDER BY "created" DESC"#
        );
    }

    #[test]
    fn filter_literals_inline() {
        let filter = Condition::Simple(SimpleCondition {
            op: Op::Eq,
            left: ValuePosition::Column {
                name: Box::from("active"),
            },
            right: ValuePosition::Literal {
                value: Lit::Bool(true),
            },
        });
        assert_eq!(
            build_cost_sql("users", &[], Some(&filter), None),
            r#"SELECT * FROM "users" WHERE "active" = 1"#
        );
    }

    #[test]
    fn remove_correlated_subqueries_collapses() {
        // A bare CSQ → None.
        let csq = Condition::CorrelatedSubquery(rindle::CorrelatedSubqueryCondition {
            related: rindle::CorrelatedSubquery {
                correlation: rindle::Correlation {
                    parent_field: vec![Box::from("id")],
                    child_field: vec![Box::from("userId")],
                },
                subquery: Box::new(rindle::Ast {
                    table: Box::from("posts"),
                    ..Default::default()
                }),
                system: None,
            },
            op: rindle::ExistsOp::Exists,
            flip: None,
            scalar: None,
            plan_id: None,
        });
        assert!(remove_correlated_subqueries(&csq).is_none());

        // AND[simple, CSQ] → just the simple.
        let simple = Condition::Simple(SimpleCondition {
            op: Op::Gt,
            left: ValuePosition::Column {
                name: Box::from("n"),
            },
            right: ValuePosition::Literal {
                value: Lit::Number(3.0),
            },
        });
        let and = Condition::And {
            conditions: vec![simple.clone(), csq],
        };
        assert_eq!(remove_correlated_subqueries(&and), Some(simple));
    }

    #[test]
    fn btree_cost_formula() {
        // (rows * log2(rows)) / 10
        assert_eq!(btree_cost(1024.0), (1024.0 * 10.0) / 10.0); // log2(1024)=10
        assert_eq!(btree_cost(1.0), 0.0); // log2(1)=0
    }
}
