#![cfg(feature = "testkit")]

//! Focused coverage for `materialize_change_edit_old_row_only` (`src/change.rs`): a
//! **middle-level child edit** must preserve the child's own grandchild subtree in the
//! materialized view, even though [`Join::push_child_change`] now materializes the
//! Edit's `old` node **row-only** (its subtree is drained by no consumer — the View
//! reduces an Edit to `(row, old_row)`, `Take` keys off `old.row`, and `Exists` gates
//! only the *new* node).
//!
//! The topology mirrors the `zero-throughput-relational` bench's hot `account_edit`
//! path: an **ordered child** (`comments` ORDER BY `val`) that itself returns a nested
//! relationship (`reactions`). A comment edit routes as
//! `Child(issue → Edit{comment_new, comment_old})` through `push_child_change`, so the
//! `old` comment arrives carrying a reactions thunk that the trim now leaves undrained.
//! A non-key edit — including one that **repositions** the comment in its ordered window
//! — must keep the reactions nested and correct (the view-after-write == fresh-query
//! contract; the explicit `.data` assertions ARE the fresh-query oracle here).

use rindle::testkit::{cleaf, cnode, rel, run_push_test_ast_view, TableSpec};
use rindle::value::{owned_row as row, OwnedValue as V, SourceSchema};
use rindle::{table, Ast, SourceChange};

fn issues_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "val"], vec![0], vec![(0, true)])
}
fn comments_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "issue", "val"], vec![0], vec![(0, true)])
}
fn reactions_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "comment", "val"], vec![0], vec![(0, true)])
}

/// `issue { comments WHERE issue==id ORDER BY val ASC { reactions WHERE comment==id } }`
fn issue_comments_reactions_ast() -> Ast {
    table("issue")
        .sub_as("comments", |i| {
            table("comment")
                .r#where("issue", i.col("id"))
                .order_by("val", "asc")
                .sub_as("reactions", |c| {
                    table("reaction").r#where("comment", c.col("id"))
                })
        })
        .build()
}

fn sources(comments: Vec<Vec<V>>, reactions: Vec<Vec<V>>) -> Vec<TableSpec> {
    vec![
        TableSpec::new("issue", issues_schema(), vec![vec![V::Int(1), V::Int(100)]]),
        TableSpec::new("comment", comments_schema(), comments),
        TableSpec::new("reaction", reactions_schema(), reactions),
    ]
}

#[test]
fn middle_child_nonkey_edit_preserves_grandchild_subtree() {
    // issue 1 → comment 10 (val 500) → {reaction 100, reaction 101}. Edit comment 10's
    // val 500 -> 510 (a non-key, ORDER-column edit). With the old comment materialized
    // row-only in push_child_change, the view must STILL show both reactions nested and
    // the comment's new val.
    let r = run_push_test_ast_view(
        sources(
            vec![vec![V::Int(10), V::Int(1), V::Int(500)]],
            vec![
                vec![V::Int(100), V::Int(10), V::Int(7)],
                vec![V::Int(101), V::Int(10), V::Int(9)],
            ],
        ),
        &issue_comments_reactions_ast(),
        vec![(
            "comment",
            SourceChange::Edit {
                row: row(vec![V::Int(10), V::Int(1), V::Int(510)]),
                old: row(vec![V::Int(10), V::Int(1), V::Int(500)]),
            },
        )],
        false,
    )
    .unwrap();

    assert_eq!(
        r.data,
        vec![cnode(
            vec![V::Int(1), V::Int(100)],
            vec![(
                rel(0),
                vec![cnode(
                    vec![V::Int(10), V::Int(1), V::Int(510)], // new val reflected
                    vec![(
                        rel(0),
                        vec![
                            cleaf(vec![V::Int(100), V::Int(10), V::Int(7)]),
                            cleaf(vec![V::Int(101), V::Int(10), V::Int(9)]),
                        ],
                    )],
                )],
            )],
        )],
        "the comment's reactions subtree must survive a non-key edit of the comment"
    );
}

#[test]
fn ordered_child_reposition_edit_keeps_each_subtree() {
    // Two comments under issue 1, ordered by val asc: comment 10 (val 500) then comment
    // 20 (val 600); each has one reaction. Edit comment 10's val 500 -> 700 so the order
    // flips to [20, 10]. Both comments must keep their OWN reaction nested and correct —
    // the bench's account seq-bump reposition, in miniature.
    let r = run_push_test_ast_view(
        sources(
            vec![
                vec![V::Int(10), V::Int(1), V::Int(500)],
                vec![V::Int(20), V::Int(1), V::Int(600)],
            ],
            vec![
                vec![V::Int(100), V::Int(10), V::Int(7)],
                vec![V::Int(200), V::Int(20), V::Int(8)],
            ],
        ),
        &issue_comments_reactions_ast(),
        vec![(
            "comment",
            SourceChange::Edit {
                row: row(vec![V::Int(10), V::Int(1), V::Int(700)]),
                old: row(vec![V::Int(10), V::Int(1), V::Int(500)]),
            },
        )],
        false,
    )
    .unwrap();

    // Order is now [comment 20 (val 600), comment 10 (val 700)], each with its reaction.
    assert_eq!(
        r.data,
        vec![cnode(
            vec![V::Int(1), V::Int(100)],
            vec![(
                rel(0),
                vec![
                    cnode(
                        vec![V::Int(20), V::Int(1), V::Int(600)],
                        vec![(
                            rel(0),
                            vec![cleaf(vec![V::Int(200), V::Int(20), V::Int(8)])],
                        )],
                    ),
                    cnode(
                        vec![V::Int(10), V::Int(1), V::Int(700)],
                        vec![(
                            rel(0),
                            vec![cleaf(vec![V::Int(100), V::Int(10), V::Int(7)])],
                        )],
                    ),
                ],
            )],
        )],
        "a repositioning edit must move the comment AND carry its reaction with it"
    );
}
