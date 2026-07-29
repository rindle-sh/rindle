#![cfg(feature = "testkit")]

use rindle::testkit::{
    run_fetch_test_ast_view_with_backend, run_fetch_test_ast_with_backend,
    run_push_test_ast_with_backend, SourceBackend, TableSpec,
};
use rindle::value::{owned_row, OwnedValue as V, SourceSchema};
use rindle::{table, Ast, SourceChange};
use rindle_sqlite::testkit::sqlite_backend;

fn issue_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "val"], vec![0], vec![(0, true)])
}

fn comment_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issue", "val"], vec![0], vec![(0, true)])
}

fn sources() -> Vec<TableSpec> {
    vec![
        TableSpec::new(
            "issue",
            issue_schema(),
            vec![vec![V::Int(1), V::Int(100)], vec![V::Int(2), V::Int(200)]],
        ),
        TableSpec::new(
            "comment",
            comment_schema(),
            vec![
                vec![V::Int(10), V::Int(1), V::Int(500)],
                vec![V::Int(11), V::Int(1), V::Int(501)],
                vec![V::Int(20), V::Int(2), V::Int(900)],
            ],
        ),
    ]
}

fn filtered_related_ast() -> Ast {
    table("issue")
        .r#where("val", 100)
        .sub_as("comments", |r| {
            table("comment").r#where("issue", r.col("id"))
        })
        .build()
}

#[test]
fn sqlite_table_source_matches_memory_for_ast_fetch() {
    let ast = filtered_related_ast();
    let memory = run_fetch_test_ast_with_backend(sources(), SourceBackend::Memory, &ast).unwrap();
    let sqlite = run_fetch_test_ast_with_backend(sources(), sqlite_backend(), &ast).unwrap();
    assert_eq!(sqlite, memory);
}

#[test]
fn sqlite_table_source_matches_memory_for_view_fetch() {
    let ast = filtered_related_ast();
    let memory =
        run_fetch_test_ast_view_with_backend(sources(), SourceBackend::Memory, &ast).unwrap();
    let sqlite = run_fetch_test_ast_view_with_backend(sources(), sqlite_backend(), &ast).unwrap();
    assert_eq!(sqlite, memory);
}

#[test]
fn sqlite_table_source_matches_memory_for_ast_push_overlay() {
    let ast = filtered_related_ast();
    let pushes = vec![(
        "comment",
        SourceChange::Add(owned_row(vec![V::Int(12), V::Int(1), V::Int(502)])),
    )];

    let memory = run_push_test_ast_with_backend(
        sources(),
        SourceBackend::Memory,
        &ast,
        pushes.clone(),
        true,
    )
    .unwrap();
    let sqlite =
        run_push_test_ast_with_backend(sources(), sqlite_backend(), &ast, pushes, true).unwrap();

    assert_eq!(sqlite.initial, memory.initial);
    assert_eq!(sqlite.pushes, memory.pushes);
    assert_eq!(sqlite.pushes_with_fetch, memory.pushes_with_fetch);
}
