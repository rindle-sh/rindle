//! Node-level k-way merge — the `Node`-stream analogue of
//! [`crate::source_common::merge_sorted_streams`] (which is row-level).
//!
//! Two operators need to merge pre-sorted `NodeStream`s:
//! - **Chunked [`FlippedJoin`](crate::op::FlippedJoin)** merges the per-IN-chunk
//!   parent streams — **no dedup** (the chunks are disjoint key-sets, so no row
//!   appears twice).
//! - **`UnionFanIn`** (`union-fan-in.ts:224` `mergeFetches`) merges the branch
//!   fetch streams **with consecutive-PK dedup** (a row matched by two OR branches
//!   is yielded once).
//!
//! One function serves both via an optional dedup primary key. It is a near-exact
//! clone of the row-level `MergeSorted` heap (binary min-heap, prime-once,
//! refill-root-in-place) over `Node` instead of `Row`, reusing the **reverse-aware**
//! [`compare_rows_rev`] (which negates the *result*, not the arguments — so this
//! port is correct on a reverse scan where the JS `mergeFetches` has a documented
//! argument-swap bug, `union-fan-in.ts:107`).
//!
//! **`Drop`-clean:** the per-branch sub-streams live in `self.streams`; dropping the
//! merged iterator early (a downstream `Take` break, or `UnionFanIn`'s existence
//! `.next()` probe) drops every sub-stream, releasing its leaf cursors — the RAII
//! port of the JS `finally → .return()` cursor cleanup.

use std::cmp::Ordering;

use crate::change::{Node, NodeStream};
use crate::source_common::compare_rows_rev;
use crate::value::{same_pk, ColId, OwnedRow as Row, Sort};

struct NodeHeapEntry<'g> {
    node: Node<'g>,
    idx: usize,
}

struct MergeNodes<'g> {
    streams: Vec<NodeStream<'g>>,
    heap: Vec<NodeHeapEntry<'g>>,
    sort: Sort,
    reverse: bool,
    /// `Some(pk)` ⇒ drop a popped node whose row is PK-equal to the last yielded
    /// (the `mergeFetches` dedup); `None` ⇒ raw k-way merge (chunked FlippedJoin).
    dedup_pk: Option<Vec<ColId>>,
    last_pk: Option<Row>,
    primed: bool,
}

impl<'g> MergeNodes<'g> {
    #[inline]
    fn less(&self, a: usize, b: usize) -> bool {
        match compare_rows_rev(
            &self.sort,
            self.reverse,
            &self.heap[a].node.row,
            &self.heap[b].node.row,
        ) {
            Ordering::Less => true,
            Ordering::Greater => false,
            // Equal sort keys (a PK-equal row carried by two branches): pop in **stream
            // order** so the dedup yields the *earliest branch*'s node — matching the JS
            // `mergeFetches` linear-head reduce ("only the first of equal rows is
            // yielded", `union-fan-in.ts:224`), which keeps the first branch in branch
            // order. Without this the surviving branch (hence its relationship
            // attachment) would be heap-arrangement-arbitrary.
            Ordering::Equal => self.heap[a].idx < self.heap[b].idx,
        }
    }

    fn sift_up(&mut self, mut i: usize) {
        while i > 0 {
            let p = (i - 1) >> 1;
            if !self.less(i, p) {
                return;
            }
            self.heap.swap(i, p);
            i = p;
        }
    }

    fn sift_down(&mut self, mut i: usize) {
        let n = self.heap.len();
        loop {
            let l = (i << 1) + 1;
            let r = l + 1;
            let mut smallest = i;
            if l < n && self.less(l, smallest) {
                smallest = l;
            }
            if r < n && self.less(r, smallest) {
                smallest = r;
            }
            if smallest == i {
                return;
            }
            self.heap.swap(i, smallest);
            i = smallest;
        }
    }

    fn prime(&mut self) {
        for i in 0..self.streams.len() {
            if let Some(node) = self.streams[i].next() {
                self.heap.push(NodeHeapEntry { node, idx: i });
                self.sift_up(self.heap.len() - 1);
            }
        }
    }

    /// The raw heap pop (no dedup): return the min node, refilling the root from its
    /// source stream in place. Mirrors `MergeSorted::next` exactly.
    fn pop_min(&mut self) -> Option<Node<'g>> {
        if self.heap.is_empty() {
            return None;
        }
        let idx = self.heap[0].idx;
        match self.streams[idx].next() {
            Some(new_node) => {
                let old = std::mem::replace(&mut self.heap[0].node, new_node);
                self.sift_down(0);
                Some(old)
            }
            None => {
                let last = self.heap.pop().unwrap();
                if self.heap.is_empty() {
                    Some(last.node)
                } else {
                    let old_root = std::mem::replace(&mut self.heap[0], last);
                    self.sift_down(0);
                    Some(old_root.node)
                }
            }
        }
    }
}

impl<'g> Iterator for MergeNodes<'g> {
    type Item = Node<'g>;
    fn next(&mut self) -> Option<Node<'g>> {
        if !self.primed {
            self.primed = true;
            self.prime();
        }
        loop {
            let node = self.pop_min()?;
            // Adjacent-dedup: skip a node PK-equal to the last yielded one. Because
            // the merge is sorted and `sort` is PK-complete, equal-PK rows from
            // different branches are adjacent in the merged order.
            let dup = match &self.dedup_pk {
                Some(pk) => self
                    .last_pk
                    .as_ref()
                    .is_some_and(|last| same_pk(&node.row, last, pk)),
                None => false,
            };
            if dup {
                continue;
            }
            if self.dedup_pk.is_some() {
                self.last_pk = Some(node.row.clone());
            }
            return Some(node);
        }
    }
}

/// K-way min-heap merge of pre-sorted node streams under the reverse-aware
/// comparator. `dedup_pk = Some(pk)` drops consecutive PK-equal rows (UnionFanIn's
/// `mergeFetches`); `None` keeps every row (chunked FlippedJoin). `Drop`-clean.
pub fn merge_node_streams<'g>(
    streams: Vec<NodeStream<'g>>,
    sort: Sort,
    reverse: bool,
    dedup_pk: Option<Vec<ColId>>,
) -> NodeStream<'g> {
    Box::new(MergeNodes {
        streams,
        heap: Vec::new(),
        sort,
        reverse,
        dedup_pk,
        last_pk: None,
        primed: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::{owned_row, OwnedValue as V};

    fn leaf_stream(ids: Vec<i64>) -> NodeStream<'static> {
        Box::new(
            ids.into_iter()
                .map(|i| Node::leaf(owned_row(vec![V::Int(i)]))),
        )
    }
    fn ids(s: NodeStream<'_>) -> Vec<i64> {
        s.map(|n| match n.row.col(0) {
            crate::value::Value::Int(i) => i,
            _ => panic!("expected Int"),
        })
        .collect()
    }

    #[test]
    fn merges_sorted_without_dedup() {
        // Two ascending streams; 3 appears in both and is kept twice (no dedup).
        let merged = merge_node_streams(
            vec![leaf_stream(vec![1, 3, 5]), leaf_stream(vec![2, 3, 4])],
            vec![(0, true)],
            false,
            None,
        );
        assert_eq!(ids(merged), vec![1, 2, 3, 3, 4, 5]);
    }

    #[test]
    fn merges_with_pk_dedup() {
        // Same input; PK dedup collapses the adjacent equal 3s to one.
        let merged = merge_node_streams(
            vec![leaf_stream(vec![1, 3, 5]), leaf_stream(vec![2, 3, 4])],
            vec![(0, true)],
            false,
            Some(vec![0]),
        );
        assert_eq!(ids(merged), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn merges_reverse_descending_with_dedup() {
        // Reverse scan: branches are pre-sorted DESCENDING; the reverse-aware
        // comparator (negate the result, NOT swap args) yields a correct descending
        // merge — the port is correct where the JS mergeFetches reverse path is buggy.
        let merged = merge_node_streams(
            vec![leaf_stream(vec![5, 3, 1]), leaf_stream(vec![4, 3, 2])],
            vec![(0, true)],
            true,
            Some(vec![0]),
        );
        assert_eq!(ids(merged), vec![5, 4, 3, 2, 1]);
    }

    #[test]
    fn empty_and_single_stream() {
        assert_eq!(
            ids(merge_node_streams(vec![], vec![(0, true)], false, None)),
            Vec::<i64>::new()
        );
        assert_eq!(
            ids(merge_node_streams(
                vec![leaf_stream(vec![1, 2, 3])],
                vec![(0, true)],
                false,
                None,
            )),
            vec![1, 2, 3]
        );
    }
}
