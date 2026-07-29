//! Integration tests for the SQLite cost model (planner Stage 8) against a real,
//! ANALYZE'd database. Not a JS differential — the row/fanout numbers come from this
//! build's SQLite + stats — so we assert the estimates track reality and that the
//! planner runs end-to-end over real statistics.

use std::rc::Rc;

use rindle::{
    Ast, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation, Dir, ExistsOp,
    OrderPart,
};
use rindle_planner::{plan_ast, ConnectionCostModel, PlannerConstraint};
use rindle_sqlite::SqliteCostModel;
use rusqlite::{params, Connection};

fn setup_db(users: i64, posts_per_user: i64) -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);
         CREATE TABLE posts (id INTEGER PRIMARY KEY, userId INTEGER, body TEXT);
         CREATE INDEX posts_userId ON posts(userId);",
    )
    .unwrap();
    for u in 0..users {
        conn.execute(
            "INSERT INTO users (id, name) VALUES (?, ?)",
            params![u, format!("u{u}")],
        )
        .unwrap();
        for p in 0..posts_per_user {
            conn.execute(
                "INSERT INTO posts (id, userId, body) VALUES (?, ?, ?)",
                params![u * 1000 + p, u, "x"],
            )
            .unwrap();
        }
    }
    conn.execute_batch("ANALYZE;").unwrap();
    conn
}

fn cset(items: &[&str]) -> PlannerConstraint {
    items.iter().map(|s| Box::from(*s)).collect()
}

#[test]
fn estimates_track_real_row_counts() {
    let conn = Rc::new(setup_db(500, 2)); // 500 users, 1000 posts (2 per user)
    let model = SqliteCostModel::new(conn);

    // Full users scan ≈ 500 rows.
    let users = model.estimate("users", &[], None, None);
    assert!(
        users.rows > 100.0 && users.rows < 2000.0,
        "users full-scan estimate {} not near 500",
        users.rows
    );

    // posts constrained by userId ≈ 2 rows per distinct user.
    let posts = model.estimate("posts", &[], None, Some(&cset(&["userId"])));
    assert!(
        posts.rows >= 1.0 && posts.rows < 50.0,
        "posts-per-user estimate {} not near 2",
        posts.rows
    );

    // The constrained scan should be far cheaper than the full table.
    assert!(
        posts.rows < users.rows,
        "constrained child ({}) should estimate fewer rows than the parent scan ({})",
        posts.rows,
        users.rows
    );
}

#[test]
fn order_by_on_unindexed_column_adds_startup_cost() {
    let conn = Rc::new(setup_db(500, 1));
    let model = SqliteCostModel::new(conn);

    let unsorted = model.estimate("users", &[], None, None);
    assert_eq!(unsorted.startup_cost, 0.0);

    // Sorting on `name` (no index) forces a sort loop → btree_cost startup.
    let sort = vec![OrderPart(Box::from("name"), Dir::Asc)];
    let sorted = model.estimate("users", &sort, None, None);
    assert!(
        sorted.startup_cost > 0.0,
        "expected a sort startup cost, got {}",
        sorted.startup_cost
    );
}

fn users_where_exists_posts() -> Ast {
    Ast {
        table: Box::from("users"),
        r#where: Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec![Box::from("id")],
                    child_field: vec![Box::from("userId")],
                },
                subquery: Box::new(Ast {
                    table: Box::from("posts"),
                    alias: Some(Box::from("posts")),
                    ..Default::default()
                }),
                system: None,
            },
            op: ExistsOp::Exists,
            flip: None,
            scalar: None,
            plan_id: None,
        })),
        ..Default::default()
    }
}

fn flip_of(c: &Condition) -> Option<bool> {
    match c {
        Condition::CorrelatedSubquery(x) => x.flip,
        _ => None,
    }
}

#[test]
fn planner_runs_end_to_end_on_sqlite_stats() {
    let conn = Rc::new(setup_db(1000, 1));
    let model: Rc<dyn ConnectionCostModel> = Rc::new(SqliteCostModel::new(conn));

    let out = plan_ast(&users_where_exists_posts(), model);

    // The planner produced an explicit flip decision from real statistics without
    // panicking (the value depends on this build's SQLite + stats).
    let flip = flip_of(out.r#where.as_ref().unwrap());
    assert!(flip.is_some(), "planner should set an explicit flip");
}
