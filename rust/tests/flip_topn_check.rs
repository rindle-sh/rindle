#![cfg(feature = "testkit")]

//! FLIPPED vs non-flipped `foo.whereExists(bar).orderBy(val).limit(2)` MUST produce the
//! same (correct) top-N — flip is a semantics-preserving plan choice, so a top-N EXISTS
//! is flip-invariant. (This is the property the fuzzer asserts in bulk now that
//! `exists+order+limit` is generated; pinned here as a fast, hand-verified guard.)
//! Covers hydrate + a push that moves a parent into the window.

use rindle::change::FetchRequest;
use rindle::graph::Graph;
use rindle::testkit::{run_fetch_test_ast, TableSpec};
use rindle::value::{owned_row, OwnedValue as V, SourceSchema};
use rindle::{
    Ast, Condition, CorrelatedSubquery, CorrelatedSubqueryCondition, Correlation, Dir, ExistsOp,
    OrderPart, SourceChange,
};

fn foo_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "val"], vec![0], vec![(0, true)])
}
fn bar_schema() -> SourceSchema {
    SourceSchema::new(vec!["id", "fooID"], vec![0], vec![(0, true)])
}

fn query(flip: Option<bool>) -> Ast {
    Ast {
        table: "foo".into(),
        order_by: vec![OrderPart("val".into(), Dir::Asc)],
        limit: Some(2),
        r#where: Some(Condition::CorrelatedSubquery(CorrelatedSubqueryCondition {
            related: CorrelatedSubquery {
                correlation: Correlation {
                    parent_field: vec!["id".into()],
                    child_field: vec!["fooID".into()],
                },
                subquery: Box::new(Ast {
                    table: "bar".into(),
                    alias: Some("bar".into()),
                    ..Default::default()
                }),
                system: None,
            },
            op: ExistsOp::Exists,
            flip,
            scalar: None,
            plan_id: None,
        })),
        ..Default::default()
    }
}

fn srcs(foos: Vec<Vec<V>>, bars: Vec<Vec<V>>) -> Vec<TableSpec> {
    vec![
        TableSpec::new("foo", foo_schema(), foos),
        TableSpec::new("bar", bar_schema(), bars),
    ]
}

fn ids(nodes: &[rindle::testkit::CaughtNode]) -> Vec<i64> {
    nodes
        .iter()
        .map(|n| match &n.row.col(0).to_owned() {
            V::Int(i) => *i,
            _ => -1,
        })
        .collect()
}

#[test]
fn flipped_vs_nonflipped_topn_exists_hydrate() {
    // foos 1,3,5 have a bar; 2,4 do not. By val asc among {1:50, 3:30, 5:40} -> [3,5].
    let foos = vec![
        vec![V::Int(1), V::Int(50)],
        vec![V::Int(2), V::Int(10)],
        vec![V::Int(3), V::Int(30)],
        vec![V::Int(4), V::Int(20)],
        vec![V::Int(5), V::Int(40)],
    ];
    let bars = vec![
        vec![V::Int(10), V::Int(1)],
        vec![V::Int(30), V::Int(3)],
        vec![V::Int(50), V::Int(5)],
    ];
    let non = run_fetch_test_ast(srcs(foos.clone(), bars.clone()), &query(None)).expect("nonflip");
    let flip =
        run_fetch_test_ast(srcs(foos, bars), &query(Some(true))).expect("flip build+hydrate");
    eprintln!(
        "hydrate: non-flipped ids = {:?} ; flipped ids = {:?} ; expected [3, 5]",
        ids(&non),
        ids(&flip)
    );
    assert_eq!(ids(&non), vec![3, 5], "non-flipped top-N");
    assert_eq!(
        ids(&flip),
        vec![3, 5],
        "flipped top-N must match (semantics-preserving)"
    );
}

#[test]
fn flipped_vs_nonflipped_topn_exists_push() {
    // Same data, but push a NEW bar for foo2 (val 10) -> foo2 should ENTER the window,
    // displacing foo5; window becomes [2, 3]. Flipped and non-flipped must agree.
    let foos = [
        vec![V::Int(1), V::Int(50)],
        vec![V::Int(2), V::Int(10)],
        vec![V::Int(3), V::Int(30)],
        vec![V::Int(4), V::Int(20)],
        vec![V::Int(5), V::Int(40)],
    ];
    let bars = [
        vec![V::Int(10), V::Int(1)],
        vec![V::Int(30), V::Int(3)],
        vec![V::Int(50), V::Int(5)],
    ];
    let run = |flip: Option<bool>| -> Vec<i64> {
        let mut g = Graph::new();
        let foo_src = g.add_source(foo_schema(), foos.iter().cloned().map(owned_row).collect());
        let bar_src = g.add_source(bar_schema(), bars.iter().cloned().map(owned_row).collect());
        let (fs, bs) = (foo_schema(), bar_schema());
        let resolve = |t: &str| match t {
            "foo" => Some((foo_src, fs.clone())),
            "bar" => Some((bar_src, bs.clone())),
            _ => None,
        };
        let top = rindle::build_pipeline(&mut g, &query(flip), &resolve).expect("build");
        let catch = g.add_catch(top, false);
        g.set_sink_edge(top, catch);
        let _ = g.catch_fetch(catch, &FetchRequest::all());
        g.source_push(
            bar_src,
            SourceChange::Add(owned_row(vec![V::Int(20), V::Int(2)])),
        );
        ids(&g.catch_fetch(catch, &FetchRequest::all()))
    };
    let non = run(None);
    let flip = run(Some(true));
    eprintln!("after push: non-flipped = {non:?} ; flipped = {flip:?} ; expected [2, 3]");
    assert_eq!(non, vec![2, 3], "non-flipped after push");
    assert_eq!(flip, vec![2, 3], "flipped after push must match");
}
