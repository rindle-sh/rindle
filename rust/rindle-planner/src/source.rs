//! `PlannerSource` — a per-table factory for [`PlannerConnection`]s (port of
//! `planner-source.ts`). One source per table in the query; `connect` mints a fresh,
//! independent connection (the planner reuses a source for self-joins but each scan
//! is its own connection).

use std::rc::Rc;

use rindle::{Condition, OrderPart};

use crate::connection::PlannerConnection;
use crate::constraint::PlannerConstraint;
use crate::cost::ConnectionCostModel;

#[derive(Clone)]
pub struct PlannerSource {
    pub name: Box<str>,
    model: Rc<dyn ConnectionCostModel>,
}

impl PlannerSource {
    pub fn new(name: impl Into<Box<str>>, model: Rc<dyn ConnectionCostModel>) -> Self {
        Self {
            name: name.into(),
            model,
        }
    }

    /// Create a connection for this table (`connect`). `name` defaults to the table.
    pub fn connect(
        &self,
        sort: Vec<OrderPart>,
        filters: Option<Condition>,
        is_root: bool,
        base_constraints: Option<PlannerConstraint>,
        limit: Option<f64>,
    ) -> PlannerConnection {
        PlannerConnection::new(
            self.name.clone(),
            self.model.clone(),
            sort,
            filters,
            is_root,
            base_constraints,
            limit,
            None,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cost::SimpleCostModel;
    use rindle::Dir;

    fn sort_by(col: &str) -> Vec<OrderPart> {
        vec![OrderPart(Box::from(col), Dir::Asc)]
    }

    #[test]
    fn connect_returns_connection() {
        let s = PlannerSource::new("users", Rc::new(SimpleCostModel));
        let c = s.connect(sort_by("id"), None, false, None, None);
        assert_eq!(c.kind(), "connection");
        assert_eq!(&*c.table, "users");
    }

    #[test]
    fn connections_are_independent() {
        let s = PlannerSource::new("users", Rc::new(SimpleCostModel));
        let mut c1 = s.connect(sort_by("id"), None, false, None, None);
        let c2 = s.connect(sort_by("name"), None, false, None, None);

        // Constraining c1 must not perturb c2.
        c1.propagate_constraints(&[0], Some([Box::from("userId")].into_iter().collect()));
        assert_eq!(c1.estimate_cost(1.0, &[0]).scan_est, 90.0);
        assert_eq!(c2.estimate_cost(1.0, &[0]).scan_est, 100.0);
    }
}
