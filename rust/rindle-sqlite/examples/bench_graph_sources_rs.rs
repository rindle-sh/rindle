//! End-to-end graph benchmarks over identical query shapes, comparing the client
//! `MemorySource` leaf with the server `TableSource` leaf.
//!
//! This is intentionally a hand-rolled `name|ns` benchmark like the existing
//! `bench_*_rs.rs` examples, so it can be diffed directly against
//! `tools/bench_graph_sources_js.ts`.
//!
//!   cargo run -p rindle-sqlite --release --example bench_graph_sources_rs --features fast-alloc

use std::collections::BTreeMap;
use std::hint::black_box;
use std::rc::Rc;
use std::time::Instant;

use rindle::view::ViewData;
use rindle::{
    build_pipeline, owned_row, table, Graph, MemorySource, NodeId, OwnedRow, OwnedValue as V,
    RelDef, Schema, SourceChange, SourceSchema, ValueType,
};
use rindle_sqlite::{ColumnDef, GraphTableSourceExt, TableSource};
use rusqlite::Connection as SqliteConnection;

/// The source-registration shape of a view [`Schema`]: same columns/PK/sort, with
/// the (query-local) relationships stripped — what `build_pipeline`'s `resolve`,
/// `new_with_schema`, and the memory `add_source` boundary now speak.
fn source_schema(schema: &Schema) -> SourceSchema {
    SourceSchema::new(
        schema.columns.iter().map(|c| &**c).collect(),
        schema.primary_key.clone(),
        schema.sort.clone(),
    )
}

#[cfg(feature = "fast-alloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const NUM_USERS: usize = 50;
const NUM_ISSUES: usize = 500;
const NUM_COMMENTS: usize = 1000;

#[derive(Clone, Copy)]
enum Backend {
    Memory,
    Table,
}

impl Backend {
    fn label(self) -> &'static str {
        match self {
            Backend::Memory => "memory",
            Backend::Table => "table",
        }
    }
}

struct TableTemplate {
    name: &'static str,
    schema: SourceSchema,
    memory: MemorySource,
    table: TableSource,
}

struct Fixture {
    name: &'static str,
    ast: rindle::Ast,
    view_schema: Schema,
    sources: Vec<TableTemplate>,
}

struct BuiltGraph {
    graph: Graph,
    sources: BTreeMap<String, NodeId>,
    view: NodeId,
}

fn bench(name: &str, mut body: impl FnMut()) {
    let warm = Instant::now();
    while warm.elapsed().as_nanos() < 200_000_000 {
        body();
    }
    let mut best = f64::INFINITY;
    for _ in 0..6 {
        let mut iters: u64 = 1;
        loop {
            let start = Instant::now();
            for _ in 0..iters {
                body();
            }
            let dt = start.elapsed().as_nanos() as f64;
            if dt >= 100_000_000.0 {
                best = best.min(dt / iters as f64);
                break;
            }
            iters *= 2;
        }
    }
    println!("{name}|{best:.1}");
}

fn s(v: &str) -> V {
    V::str(v)
}

fn n(v: impl Into<f64>) -> V {
    V::Float(v.into())
}

fn b(v: bool) -> V {
    V::Bool(v)
}

fn col(name: &str, ty: ValueType, optional: bool) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        ty,
        optional,
    }
}

fn user_columns() -> Vec<ColumnDef> {
    vec![
        col("id", ValueType::String, false),
        col("login", ValueType::String, false),
        col("name", ValueType::String, true),
        col("avatar", ValueType::String, false),
        col("role", ValueType::String, false),
    ]
}

fn issue_columns() -> Vec<ColumnDef> {
    vec![
        col("id", ValueType::String, false),
        col("shortID", ValueType::Number, true),
        col("title", ValueType::String, false),
        col("open", ValueType::Boolean, false),
        col("modified", ValueType::Number, false),
        col("created", ValueType::Number, false),
        col("projectID", ValueType::String, false),
        col("creatorID", ValueType::String, false),
        col("assigneeID", ValueType::String, true),
        col("description", ValueType::String, false),
        col("visibility", ValueType::String, false),
    ]
}

fn comment_columns() -> Vec<ColumnDef> {
    vec![
        col("id", ValueType::String, false),
        col("issueID", ValueType::String, false),
        col("created", ValueType::Number, false),
        col("body", ValueType::String, false),
        col("creatorID", ValueType::String, false),
    ]
}

fn user_schema() -> Schema {
    Schema::new(
        vec!["id", "login", "name", "avatar", "role"],
        vec![0],
        vec![(0, true)],
    )
}

fn comment_schema(creator: bool) -> Schema {
    let schema = Schema::new(
        vec!["id", "issueID", "created", "body", "creatorID"],
        vec![0],
        vec![(0, true)],
    );
    if creator {
        schema.with_relationships(vec![RelDef::related("creator", user_schema())])
    } else {
        schema
    }
}

fn issue_schema(creator: bool, comments: Option<Schema>) -> Schema {
    let mut rels = Vec::new();
    if creator {
        rels.push(RelDef::related("creator", user_schema()));
    }
    if let Some(comment_schema) = comments {
        rels.push(RelDef::related("comments", comment_schema));
    }
    Schema::new(
        vec![
            "id",
            "shortID",
            "title",
            "open",
            "modified",
            "created",
            "projectID",
            "creatorID",
            "assigneeID",
            "description",
            "visibility",
        ],
        vec![0],
        vec![(0, true)],
    )
    .with_relationships(rels)
}

fn user_row(i: usize) -> OwnedRow {
    owned_row(vec![
        s(&format!("user-{i}")),
        s(&format!("user{i}")),
        s(&format!("User {i}")),
        s(&format!("avatar{i}")),
        s(if i.is_multiple_of(10) { "crew" } else { "user" }),
    ])
}

fn issue_row(i: usize) -> OwnedRow {
    owned_row(vec![
        s(&format!("issue-{i}")),
        n(i as f64),
        s(&format!("Issue {i}: Some bug or feature request")),
        b(!i.is_multiple_of(3)),
        n(1_700_000_000_000_f64 - i as f64 * 1000.0),
        n(1_700_000_000_000_f64 - i as f64 * 2000.0),
        s(&format!("proj-{}", i % 2)),
        s(&format!("user-{}", i % NUM_USERS)),
        if i.is_multiple_of(4) {
            V::Null
        } else {
            s(&format!("user-{}", (i + 1) % NUM_USERS))
        },
        s(&format!("Description for issue {i}")),
        s(if i.is_multiple_of(5) {
            "internal"
        } else {
            "public"
        }),
    ])
}

fn comment_row(i: usize) -> OwnedRow {
    owned_row(vec![
        s(&format!("comment-{i}")),
        s(&format!("issue-{}", i % NUM_ISSUES)),
        n(1_700_000_000_000_f64 - i as f64 * 500.0),
        s(&format!("Comment body {i}")),
        s(&format!("user-{}", i % NUM_USERS)),
    ])
}

fn users() -> Vec<OwnedRow> {
    (0..NUM_USERS).map(user_row).collect()
}

fn issues() -> Vec<OwnedRow> {
    (0..NUM_ISSUES).map(issue_row).collect()
}

fn comments() -> Vec<OwnedRow> {
    (0..NUM_COMMENTS).map(comment_row).collect()
}

fn table_template(
    name: &'static str,
    schema: Schema,
    columns: Vec<ColumnDef>,
    rows: Vec<OwnedRow>,
    indexes: &[&[&str]],
) -> TableTemplate {
    let memory = MemorySource::new(schema.clone(), rows.clone());
    let table = sqlite_source(name, &schema, columns, &rows, indexes);
    TableTemplate {
        name,
        schema: source_schema(&schema),
        memory,
        table,
    }
}

fn sqlite_source(
    name: &str,
    schema: &Schema,
    columns: Vec<ColumnDef>,
    rows: &[OwnedRow],
    indexes: &[&[&str]],
) -> TableSource {
    let conn = Rc::new(SqliteConnection::open_in_memory().expect("open sqlite bench db"));
    create_sqlite_table(&conn, name, &columns, &schema.primary_key, indexes);
    let source = TableSource::new_with_schema(
        conn,
        name,
        columns,
        schema.primary_key.clone(),
        source_schema(schema),
    );
    for row in rows {
        source.push(SourceChange::Add(row.clone()), &|_, _| {});
    }
    source
}

fn create_sqlite_table(
    conn: &SqliteConnection,
    name: &str,
    columns: &[ColumnDef],
    primary_key: &[usize],
    indexes: &[&[&str]],
) {
    let defs = columns
        .iter()
        .map(|c| {
            let ty = match c.ty {
                ValueType::Boolean | ValueType::Int => "INTEGER",
                ValueType::Number => "REAL",
                ValueType::String | ValueType::Json | ValueType::Null => "TEXT",
            };
            let not_null = if c.optional { "" } else { " NOT NULL" };
            format!("{} {ty}{not_null}", quote_ident(&c.name))
        })
        .collect::<Vec<_>>()
        .join(", ");
    conn.execute_batch(&format!("CREATE TABLE {} ({defs});", quote_ident(name)))
        .expect("create sqlite bench table");

    let pk = primary_key
        .iter()
        .map(|&c| quote_ident(&columns[c].name))
        .collect::<Vec<_>>()
        .join(", ");
    conn.execute_batch(&format!(
        "CREATE UNIQUE INDEX {} ON {} ({pk});",
        quote_ident(&format!("__bench_{name}_pk")),
        quote_ident(name)
    ))
    .expect("create sqlite bench pk");

    for (i, cols) in indexes.iter().enumerate() {
        let cols = cols
            .iter()
            .map(|c| quote_ident(c))
            .collect::<Vec<_>>()
            .join(", ");
        conn.execute_batch(&format!(
            "CREATE INDEX {} ON {} ({cols});",
            quote_ident(&format!("__bench_{name}_{i}")),
            quote_ident(name)
        ))
        .expect("create sqlite bench index");
    }
}

fn quote_ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

fn fixture(
    name: &'static str,
    ast: rindle::Ast,
    issue: Schema,
    comment: Option<Schema>,
) -> Fixture {
    let mut sources = Vec::new();
    sources.push(table_template(
        "issue",
        issue.clone(),
        issue_columns(),
        issues(),
        &[&["open", "id"], &["creatorID", "id"]],
    ));
    sources.push(table_template(
        "user",
        user_schema(),
        user_columns(),
        users(),
        &[],
    ));
    if let Some(comment) = comment {
        sources.push(table_template(
            "comment",
            comment,
            comment_columns(),
            comments(),
            &[&["issueID", "id"], &["creatorID", "id"]],
        ));
    }
    Fixture {
        name,
        ast,
        view_schema: issue,
        sources,
    }
}

fn hydration_fixtures() -> Vec<Fixture> {
    vec![
        fixture(
            "hydrate_issues_only",
            table("issue").build(),
            issue_schema(false, None),
            None,
        ),
        fixture(
            "hydrate_issues_filtered_open",
            table("issue").r#where("open", true).build(),
            issue_schema(false, None),
            None,
        ),
        fixture(
            "hydrate_issues_limit_50",
            table("issue").limit(50).build(),
            issue_schema(false, None),
            None,
        ),
        fixture(
            "hydrate_issues_with_creator",
            table("issue")
                .sub_as("creator", |r| {
                    table("user").r#where("id", r.col("creatorID"))
                })
                .build(),
            issue_schema(true, None),
            None,
        ),
        fixture(
            "hydrate_issues_with_comments",
            table("issue")
                .sub_as("comments", |r| {
                    table("comment").r#where("issueID", r.col("id"))
                })
                .build(),
            issue_schema(false, Some(comment_schema(false))),
            Some(comment_schema(false)),
        ),
        fixture(
            "hydrate_issues_with_creator_and_comments",
            table("issue")
                .sub_as("creator", |r| {
                    table("user").r#where("id", r.col("creatorID"))
                })
                .sub_as("comments", |r| {
                    table("comment").r#where("issueID", r.col("id"))
                })
                .build(),
            issue_schema(true, Some(comment_schema(false))),
            Some(comment_schema(false)),
        ),
        fixture(
            "hydrate_issues_with_comments_and_comment_creator",
            table("issue")
                .sub_as("comments", |r| {
                    table("comment")
                        .r#where("issueID", r.col("id"))
                        .sub_as("creator", |r| {
                            table("user").r#where("id", r.col("creatorID"))
                        })
                })
                .build(),
            issue_schema(false, Some(comment_schema(true))),
            Some(comment_schema(true)),
        ),
    ]
}

fn push_fixtures() -> Vec<Fixture> {
    vec![
        fixture(
            "push_add_issue_no_join",
            table("issue").build(),
            issue_schema(false, None),
            None,
        ),
        fixture(
            "push_edit_issue_no_join",
            table("issue").build(),
            issue_schema(false, None),
            None,
        ),
        fixture(
            "push_add_issue_with_creator",
            table("issue")
                .sub_as("creator", |r| {
                    table("user").r#where("id", r.col("creatorID"))
                })
                .build(),
            issue_schema(true, None),
            None,
        ),
        fixture(
            "push_add_comment_child_relation",
            table("issue")
                .sub_as("comments", |r| {
                    table("comment").r#where("issueID", r.col("id"))
                })
                .build(),
            issue_schema(false, Some(comment_schema(false))),
            Some(comment_schema(false)),
        ),
        fixture(
            "push_add_issue_inside_limit_50",
            table("issue").limit(50).build(),
            issue_schema(false, None),
            None,
        ),
        fixture(
            "push_add_issue_outside_limit_50",
            table("issue").limit(50).build(),
            issue_schema(false, None),
            None,
        ),
    ]
}

fn add_sources(
    graph: &mut Graph,
    fixture: &Fixture,
    backend: Backend,
) -> BTreeMap<String, (NodeId, SourceSchema)> {
    let mut out = BTreeMap::new();
    for source in &fixture.sources {
        let id = match backend {
            Backend::Memory => graph.add_memory_source(source.memory.fork()),
            Backend::Table => graph.add_table_source(source.table.fork()),
        };
        out.insert(source.name.to_string(), (id, source.schema.clone()));
    }
    out
}

fn build_view_graph(fixture: &Fixture, backend: Backend) -> BuiltGraph {
    let mut graph = Graph::new();
    let by_name = add_sources(&mut graph, fixture, backend);
    let top = {
        let resolve = |table: &str| by_name.get(table).cloned();
        build_pipeline(&mut graph, &fixture.ast, &resolve).expect("build bench pipeline")
    };
    let view = graph.add_view(top, fixture.view_schema.clone());
    graph.set_sink_edge(top, view);
    graph.hydrate(view);
    let sources = by_name
        .into_iter()
        .map(|(name, (id, _))| (name, id))
        .collect();
    BuiltGraph {
        graph,
        sources,
        view,
    }
}

fn hydrate_once(fixture: &Fixture, backend: Backend) -> usize {
    let built = build_view_graph(fixture, backend);
    count_view_data(&built.graph.view_data(built.view))
}

fn source_id(built: &BuiltGraph, name: &str) -> NodeId {
    *built
        .sources
        .get(name)
        .unwrap_or_else(|| panic!("missing source {name:?}"))
}

fn count_view_data(data: &ViewData) -> usize {
    data.items.len()
}

fn pushed_issue(id: &str, short_id: f64) -> OwnedRow {
    owned_row(vec![
        s(id),
        n(short_id),
        s("Pushed issue"),
        b(true),
        n(1_800_000_000_000_f64),
        n(1_800_000_000_000_f64),
        s("proj-0"),
        s("user-0"),
        V::Null,
        s("Pushed issue description"),
        s("public"),
    ])
}

fn pushed_comment(id: &str, issue_id: &str) -> OwnedRow {
    owned_row(vec![
        s(id),
        s(issue_id),
        n(1_800_000_000_000_f64),
        s("A new comment"),
        s("user-0"),
    ])
}

fn edited_issue_pair() -> (OwnedRow, OwnedRow) {
    let old = issue_row(1);
    let mut values = old.to_value_vec();
    values[2] = s("Edited issue title");
    (owned_row(values), old)
}

fn run_hydration_benches(fixtures: &[Fixture]) {
    for fixture in fixtures {
        for backend in [Backend::Memory, Backend::Table] {
            let name = format!("{}.{}", backend.label(), fixture.name);
            bench(&name, || {
                black_box(hydrate_once(fixture, backend));
            });
        }
    }
}

fn run_push_benches(fixtures: &[Fixture]) {
    for fixture in fixtures {
        for backend in [Backend::Memory, Backend::Table] {
            let built = build_view_graph(fixture, backend);
            let name = format!("{}.{}", backend.label(), fixture.name);
            match fixture.name {
                "push_add_issue_no_join" | "push_add_issue_with_creator" => {
                    let src = source_id(&built, "issue");
                    let row = pushed_issue("push-issue", NUM_ISSUES as f64 + 1.0);
                    bench(&name, || {
                        built.graph.source_push(src, SourceChange::Add(row.clone()));
                        built
                            .graph
                            .source_push(src, SourceChange::Remove(row.clone()));
                        black_box(count_view_data(&built.graph.view_data(built.view)));
                    });
                }
                "push_edit_issue_no_join" => {
                    let src = source_id(&built, "issue");
                    let (row, old) = edited_issue_pair();
                    bench(&name, || {
                        built.graph.source_push(
                            src,
                            SourceChange::Edit {
                                row: row.clone(),
                                old: old.clone(),
                            },
                        );
                        built.graph.source_push(
                            src,
                            SourceChange::Edit {
                                row: old.clone(),
                                old: row.clone(),
                            },
                        );
                        black_box(count_view_data(&built.graph.view_data(built.view)));
                    });
                }
                "push_add_comment_child_relation" => {
                    let src = source_id(&built, "comment");
                    let row = pushed_comment("push-comment", "issue-1");
                    bench(&name, || {
                        built.graph.source_push(src, SourceChange::Add(row.clone()));
                        built
                            .graph
                            .source_push(src, SourceChange::Remove(row.clone()));
                        black_box(count_view_data(&built.graph.view_data(built.view)));
                    });
                }
                "push_add_issue_inside_limit_50" => {
                    let src = source_id(&built, "issue");
                    let row = pushed_issue("aaa-push-issue", -1.0);
                    bench(&name, || {
                        built.graph.source_push(src, SourceChange::Add(row.clone()));
                        built
                            .graph
                            .source_push(src, SourceChange::Remove(row.clone()));
                        black_box(count_view_data(&built.graph.view_data(built.view)));
                    });
                }
                "push_add_issue_outside_limit_50" => {
                    let src = source_id(&built, "issue");
                    let row = pushed_issue("zzz-push-issue", NUM_ISSUES as f64 + 1.0);
                    bench(&name, || {
                        built.graph.source_push(src, SourceChange::Add(row.clone()));
                        built
                            .graph
                            .source_push(src, SourceChange::Remove(row.clone()));
                        black_box(count_view_data(&built.graph.view_data(built.view)));
                    });
                }
                other => panic!("unhandled push fixture {other}"),
            }
        }
    }
}

fn main() {
    let hydration = hydration_fixtures();
    run_hydration_benches(&hydration);

    let push = push_fixtures();
    run_push_benches(&push);
}
