//! `Skip` (`ivm/skip.ts`, spec `07` §5) — drop rows before a start bound. Also the
//! **worked template** for the operator fan-out seam (see [`crate::op`]).
//!
//! A faithful port of `skip.ts`: a stateless, ordered operator that sets the
//! pipeline's start position. On `fetch` it pushes the bound down into the input's
//! `FetchRequest::start` (merging it with any incoming start via [`Skip::get_start`],
//! exactly as `#getStart`) and, on a reverse scan, trims the far end with a
//! `take_while`. On `push` it gates Add/Remove/Child on the bound and splits an
//! `Edit` that crosses it (`maybe-split-and-push-edit-change.ts`).

use std::cell::Cell;
use std::cmp::Ordering;

use crate::change::{Basis, Change, FetchRequest, NodeStream, OutEdge, Start};
use crate::graph::{Graph, NodeId};
use crate::value::{compare_rows, OwnedRow as Row, Sort};

/// `Skip`: a stateless, ordered operator that drops rows up to `bound` (the AST
/// `start`). Stateless ⇒ no [`StorageId`](crate::graph::StorageId).
pub struct Skip {
    /// Upstream input operator (always the source connection in the built subset —
    /// `start` is lowered right after the source, mirroring `buildPipelineInternal`).
    pub input: NodeId,
    /// The start bound: rows at/after it survive, per its [`Start::basis`]
    /// (`Basis::At` includes the bound row, `Basis::After` excludes it). The
    /// builder lowers the AST `start` (a name-keyed partial row + `exclusive`) to
    /// this positional [`Start`]; the comparator only reads the sort columns.
    pub bound: Start,
    /// The single downstream edge as a **port-carrying** [`OutEdge`] (like a
    /// [`Join`](crate::graph)'s): `Port::Single` to a terminal sink, or
    /// `Port::JoinParent` when this `Skip` feeds the parent port of a relationship
    /// join stacked above it. Wired two-phase via
    /// [`Graph::set_output`](crate::graph::Graph::set_output) (Single) or
    /// [`Graph::set_out_edge`](crate::graph::Graph::set_out_edge) (explicit port).
    pub output: Cell<Option<OutEdge>>,
}

/// The resolved start for the input fetch — `#getStart`'s return (`skip.ts:107`).
/// `Empty` means the request can produce no rows; `Use(None)` means "no start
/// gate" (JS `undefined`); `Use(Some(s))` pushes `s` down to the input.
enum Resolved {
    Empty,
    Use(Option<Start>),
}

impl Skip {
    pub fn new(input: NodeId, bound: Start) -> Skip {
        Skip {
            input,
            bound,
            output: Cell::new(None),
        }
    }

    /// `#shouldBePresent` (`skip.ts:83`): `row` survives iff the bound sorts before
    /// it, or sorts equal and the bound is inclusive (`Basis::At`).
    fn should_be_present(&self, sort: &Sort, row: &Row) -> bool {
        match compare_rows(sort, &self.bound.row, row) {
            Ordering::Less => true,
            Ordering::Equal => matches!(self.bound.basis, Basis::At),
            Ordering::Greater => false,
        }
    }

    /// `#getStart` (`skip.ts:107`): merge the Skip bound with the request's own
    /// start into the single start to push down to the input. Handles forward and
    /// reverse scans and the equal/exclusive boundary straddles.
    fn get_start(&self, sort: &Sort, req: &FetchRequest) -> Resolved {
        let exclusive = matches!(self.bound.basis, Basis::After);
        let bound_start = || self.bound.clone();
        let after_bound = || Start {
            row: self.bound.row.clone(),
            basis: Basis::After,
        };

        let Some(rs) = &req.start else {
            // No incoming start: forward uses the bound; reverse needs no gate here
            // (the far-end trim happens in `fetch`'s `take_while`).
            return if req.reverse {
                Resolved::Use(None)
            } else {
                Resolved::Use(Some(bound_start()))
            };
        };

        let cmp = compare_rows(sort, &self.bound.row, &rs.row);
        if !req.reverse {
            match cmp {
                // Bound is after the requested start → the requested start is moot.
                Ordering::Greater => Resolved::Use(Some(bound_start())),
                // Equal: if either side is exclusive, exclude the shared row.
                Ordering::Equal => {
                    if exclusive || matches!(rs.basis, Basis::After) {
                        Resolved::Use(Some(after_bound()))
                    } else {
                        Resolved::Use(Some(bound_start()))
                    }
                }
                // Bound is before the requested start → honor the request's start.
                Ordering::Less => Resolved::Use(Some(rs.clone())),
            }
        } else {
            match cmp {
                // Reverse scan whose start is *below* the bound → no rows survive.
                Ordering::Greater => Resolved::Empty,
                Ordering::Equal => {
                    // Only the single shared row survives, and only if both inclusive.
                    if !exclusive && matches!(rs.basis, Basis::At) {
                        Resolved::Use(Some(bound_start()))
                    } else {
                        Resolved::Empty
                    }
                }
                Ordering::Less => Resolved::Use(Some(rs.clone())),
            }
        }
    }

    /// Lazy pull (`skip.ts:53`): merge the bound into the input start and fetch.
    /// Forward scans pass the input through (the source's start gate did the work);
    /// reverse scans additionally `take_while` rows that are still at/after the
    /// bound, stopping at the first that falls below it.
    pub fn fetch<'g>(&'g self, g: &'g Graph, req: &FetchRequest) -> NodeStream<'g> {
        let sort = g.input_sort(self.input);
        let start = match self.get_start(&sort, req) {
            Resolved::Empty => return Box::new(std::iter::empty()),
            Resolved::Use(s) => s,
        };
        let adjusted = FetchRequest {
            start,
            ..req.clone()
        };
        let nodes = g.fetch(self.input, &adjusted);
        if req.reverse {
            Box::new(nodes.take_while(move |node| self.should_be_present(&sort, &node.row)))
        } else {
            nodes
        }
    }

    /// Eager push (`skip.ts:88`): forward Add/Remove/Child iff their row is at/after
    /// the bound; split an `Edit` that crosses the bound into Remove(old)/Add(new)
    /// (`maybe-split-and-push-edit-change.ts`). Forwards on the edge's own port so a
    /// `Skip` can feed a relationship join's parent port.
    pub fn push<'g>(&'g self, g: &'g Graph, change: Change<'g>) {
        let out = self.output.get().expect("Skip output not wired");
        let sort = g.input_sort(self.input);
        match change {
            Change::Edit { node, old } => {
                match (
                    self.should_be_present(&sort, &old.row),
                    self.should_be_present(&sort, &node.row),
                ) {
                    (true, true) => g.push(out.node, Change::Edit { node, old }, out.port),
                    (true, false) => g.push(out.node, Change::Remove(old), out.port),
                    (false, true) => g.push(out.node, Change::Add(node), out.port),
                    (false, false) => {}
                }
            }
            Change::Add(n) => {
                if self.should_be_present(&sort, &n.row) {
                    g.push(out.node, Change::Add(n), out.port);
                }
            }
            Change::Remove(n) => {
                if self.should_be_present(&sort, &n.row) {
                    g.push(out.node, Change::Remove(n), out.port);
                }
            }
            Change::Child { node, rel, child } => {
                if self.should_be_present(&sort, &node.row) {
                    g.push(out.node, Change::Child { node, rel, child }, out.port);
                }
            }
        }
    }
}
