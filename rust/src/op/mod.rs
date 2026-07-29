//! Spec `06`/`07` operators implemented **outside `graph.rs`** — the operator
//! **fan-out seam**.
//!
//! ## Why this module exists
//!
//! Operators are variants of the closed `graph::Operator` enum, and `Graph`
//! dispatches `fetch`/`push`/`input_schema`/… over it. That enum + its match sites
//! are a single file: if every operator's *logic* also lived in `graph.rs`, N
//! agents each implementing one operator would all edit the same file and collide.
//!
//! The seam (the decision in the prep wave) keeps each operator's struct **and all
//! its logic** in its own file here (`op/<name>.rs`), and leaves only **thin,
//! stable wiring** in `graph.rs`:
//!
//! 1. one `Operator::<Name>(<Name>)` enum variant,
//! 2. a **delegating** arm in each dispatch fn — `Op::<Name>(o) => o.fetch(self,
//!    req)` / `o.push(self, change)`,
//! 3. a typed `input_schema` arm (pass-through for the non-reshaping operators),
//! 4. an `add_<name>(<Name>) -> NodeId` builder that takes a **fully-constructed
//!    value** — so changing the struct's fields never changes `graph.rs`.
//!
//! The operator drives the graph through the `pub(crate)` Graph API:
//! [`Graph::fetch`](crate::graph::Graph::fetch),
//! `Graph::push`,
//! `Graph::input_schema`, and [`Graph::storage`](crate::graph::Graph) (stateful
//! operators get a [`StorageId`](crate::graph::StorageId) from
//! `Graph::alloc_storage`).
//!
//! ## Adding an operator (the template)
//!
//! [`Skip`] is the worked example. To add `Take`/`Cap`/`Exists`/`FlippedJoin`/
//! `UnionFanOut`/`UnionFanIn`:
//!
//! 1. Copy `skip.rs` → `op/<name>.rs`; define the struct (fields private to the
//!    module — grow them freely) and `impl` its `fetch`/`push` (+ the filter-chain
//!    methods if it's a chain link like `Exists`). Stateful operators store a
//!    `StorageId` and read state via `g.storage(id)`.
//! 2. Add the four wiring lines to `graph.rs` (variant + the two delegating arms +
//!    the `input_schema` arm; plus the `set_output` arm and `add_<name>` builder).
//! 3. `mod <name>; pub use <name>::<Name>;` here.
//!
//! Two agents adding two operators touch **disjoint files** plus a handful of
//! distinct, auto-mergeable lines in `graph.rs` — not the same match arm. That is
//! what makes the operator port fan out.

pub mod join_util;
pub use join_util::{is_join_match, row_equals_for_compound_key};

mod merge;
pub use merge::merge_node_streams;

mod partition;

mod skip;
pub use skip::Skip;

mod take;
pub use take::Take;

mod cap;
pub use cap::Cap;

mod exists;
pub use exists::Exists;

mod flipped_join;
pub use flipped_join::FlippedJoin;

mod union;
pub use union::{UnionFanIn, UnionFanOut};

mod reduce;
pub use reduce::{AggSpec, Reduce};
