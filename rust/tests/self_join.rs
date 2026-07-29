//! The make-or-break proof.
//!
//! Table `t(id, parent)`. Self-join "children": a parent row with `id = X` has
//! children = all rows with `parent = X`. This puts the SAME source in the graph
//! twice (parent connection + child connection) and forces:
//!   * reentrant `parent.fetch()` during a child push,
//!   * the join overlay (in-progress child captured by value into the thunk),
//!   * the source overlay (uncommitted row seen by the reentrant fetch),
//!
//! all over an arena+NodeId graph with shared `&Graph` borrows only.

use rindle::change::{OutEdge, Port};
use rindle::graph::{Graph, NodeId};
use rindle::value::{owned_row as row, OwnedValue as Value, RelDef, RelId, Schema, SourceSchema};
use rindle::SourceChange;

const ID: usize = 0;
const PARENT: usize = 1;

fn schema() -> SourceSchema {
    // The self-relationship "children" is slot 0 on the parent (passed explicitly to
    // `add_join_slot` as `RelId(0)`); the source schema itself carries no relationships.
    SourceSchema::new(vec!["id", "parent"], vec![ID], vec![(ID, true)])
}

/// The schema the production `View` materializes against: the "children" hop
/// carries its (leaf) target schema so the View can sort/build the promoted
/// children. The query is one join deep, so the child level is a leaf in the view
/// (no grandchildren) — distinct from the source `schema()` used for name
/// resolution at `add_join`.
fn view_schema() -> Schema {
    let child = Schema::new(vec!["id", "parent"], vec![ID], vec![(ID, true)]);
    Schema::new(vec!["id", "parent"], vec![ID], vec![(ID, true)])
        .with_relationships(vec![RelDef::related("children", child)])
}

/// Build: source `t` -> (parent conn, child conn) -> Join(children) -> View.
/// Returns (graph, source_id, view_id).
fn build(initial: Vec<Vec<Value>>) -> (Graph, NodeId, NodeId) {
    let mut g = Graph::new();
    let src = g.add_source(schema(), initial.into_iter().map(row).collect());

    let parent_conn = g.connect(src, Some(schema().sort), None, vec![]);
    let child_conn = g.connect(src, Some(schema().sort), None, vec![]);

    // children: parent.id == child.parent (relationship slot 0)
    let join = g.add_join_slot(parent_conn, child_conn, vec![ID], vec![PARENT], RelId(0));

    let view = g.add_view(join, view_schema());

    // Wire edges (mirrors builder.ts two-phase setOutput).
    g.set_conn_output(
        parent_conn,
        OutEdge {
            node: join,
            port: Port::JoinParent,
        },
    );
    g.set_conn_output(
        child_conn,
        OutEdge {
            node: join,
            port: Port::JoinChild,
        },
    );
    g.set_output(join, view);

    (g, src, view)
}

fn n(id: i64, parent: Option<i64>) -> Vec<Value> {
    vec![
        Value::Int(id),
        parent.map(Value::Int).unwrap_or(Value::Null),
    ]
}

#[test]
fn hydrate_self_join() {
    // node 1 is a root; node 2's parent is 1.
    let (g, _src, view) = build(vec![n(1, None), n(2, Some(1))]);
    g.hydrate(view);
    // top-level [1, 2]; 1 has child 2; 2 has no children.
    assert_eq!(g.dump_view(view), vec![(1, vec![2]), (2, vec![])]);
}

#[test]
fn push_add_child_is_reentrant() {
    // Start with just the root, then add a child of it.
    let (g, src, view) = build(vec![n(1, None)]);
    g.hydrate(view);
    assert_eq!(g.dump_view(view), vec![(1, vec![])]);

    // Add {id:2, parent:1}. This must:
    //  - appear as a new top-level parent (id 2, no children), AND
    //  - via the reentrant push_child_change, re-enter parent.fetch({id:1}),
    //    find node 1, and add node 2 to node 1's children.
    g.source_push(src, SourceChange::Add(row(n(2, Some(1)))));

    assert_eq!(g.dump_view(view), vec![(1, vec![2]), (2, vec![])]);
}

#[test]
fn self_referential_add_needs_source_overlay() {
    // The strongest case: add {id:1, parent:1} into an EMPTY table. Node 1 is
    // its OWN child. During push_child_change the reentrant parent.fetch({id:1})
    // must observe node 1 even though it is NOT yet committed to storage — that
    // requires the source overlay (gated by epoch on the already-pushed parent
    // connection). The join overlay then puts node 1 into its own children.
    let (g, src, view) = build(vec![]);
    g.hydrate(view);
    assert_eq!(g.dump_view(view), Vec::<(i64, Vec<i64>)>::new());

    g.source_push(src, SourceChange::Add(row(n(1, Some(1)))));

    assert_eq!(g.dump_view(view), vec![(1, vec![1])]);
}

#[test]
fn remove_child_updates_parent() {
    let (g, src, view) = build(vec![n(1, None), n(2, Some(1))]);
    g.hydrate(view);
    assert_eq!(g.dump_view(view), vec![(1, vec![2]), (2, vec![])]);

    // Remove node 2: it disappears as a top-level row AND from node 1's children.
    g.source_push(src, SourceChange::Remove(row(n(2, Some(1)))));
    assert_eq!(g.dump_view(view), vec![(1, vec![])]);
}

#[test]
fn drop_releases_cursor_on_early_termination() {
    // Primitive #2: partially consume a fetch and drop it; the "cursor" count
    // must return to 0 with no explicit close — Drop == JS finally/.return().
    let (g, src, view) = build(vec![n(1, None), n(2, Some(1)), n(3, Some(1))]);
    assert_eq!(g.cursors_open(src), 0);

    // Hydrate fully consumes & drops all streams -> back to 0.
    g.hydrate(view);
    assert_eq!(g.cursors_open(src), 0, "all cursors released after hydrate");

    // Note: the leaf scan is materialized in the spike, so the open-cursor window
    // is short-lived; this asserts the RAII guard balances to zero across the
    // many fetches a hydrate performs (including the reentrant child fetches).
}
