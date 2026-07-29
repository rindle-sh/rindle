//! Runtime error surface for the engine.
//!
//! Build-time query-shape failures stay in [`crate::BuildError`]. This type is for
//! failures that can occur while reading/writing external data: SQLite I/O/step
//! errors, value conversion failures, and backend storage failures.

use std::fmt;

#[derive(Debug)]
pub enum RindleError {
    /// A SQLite backend operation failed. The underlying error is stringified at
    /// the construction boundary (see [`RindleError::sqlite`]) so the core crate
    /// carries no `rusqlite` dependency — the SQLite backend lives in `rindle-sqlite`.
    Sqlite {
        op: &'static str,
        source: String,
    },
    UnsupportedValue(String),
    InvalidUtf8 {
        value_type: &'static str,
        source: std::str::Utf8Error,
    },
    Storage(String),
    /// A malformed change stream relative to current state: an ADD of an existing
    /// row, a REMOVE/EDIT of an absent row, or an EDIT that mutates a join key.
    /// Surfaced only when strict change validation is enabled (WS02.2); otherwise
    /// these are `debug_assert!`-checked internal invariants.
    ConsistencyViolation {
        kind: &'static str,
    },
    /// An ingested row does not match its schema (wrong width, etc.) — rejected at
    /// the source boundary before it can reach the engine's unchecked column index
    /// (WS02.4).
    SchemaViolation(String),
    /// A push blew the host-armed wall-clock deadline (FOLLOWER-LAG-SHED §6.6 — the
    /// runaway-push bail): a fan-out checkpoint (`site` names the loop) parked this and
    /// stopped iterating. Operator state is torn and must be discarded — the host's
    /// derive-fault path (teardown + rehydrate) is the defined recovery, exactly as for
    /// a panic. Never raised unless the host armed `Graph::set_push_deadline`.
    PushDeadlineExceeded {
        site: &'static str,
    },
}

impl RindleError {
    /// Construct a [`RindleError::Sqlite`] from any displayable backend error. The
    /// source is stringified here so callers in `rindle-sqlite` can pass a
    /// `rusqlite::Error` without the core crate depending on `rusqlite`.
    pub fn sqlite(op: &'static str, source: impl fmt::Display) -> RindleError {
        RindleError::Sqlite {
            op,
            source: source.to_string(),
        }
    }

    pub fn unsupported_value(message: impl Into<String>) -> RindleError {
        RindleError::UnsupportedValue(message.into())
    }

    pub(crate) fn schema_violation(message: impl Into<String>) -> RindleError {
        RindleError::SchemaViolation(message.into())
    }
}

impl fmt::Display for RindleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RindleError::Sqlite { op, source } => write!(f, "SQLite {op} failed: {source}"),
            RindleError::UnsupportedValue(message) => write!(f, "unsupported value: {message}"),
            RindleError::InvalidUtf8 { value_type, source } => {
                write!(f, "{value_type} value is not valid UTF-8: {source}")
            }
            RindleError::Storage(message) => write!(f, "storage error: {message}"),
            RindleError::ConsistencyViolation { kind } => {
                write!(f, "change consistency violation: {kind}")
            }
            RindleError::SchemaViolation(message) => write!(f, "schema violation: {message}"),
            RindleError::PushDeadlineExceeded { site } => {
                write!(f, "push deadline exceeded in {site} (runaway push bailed)")
            }
        }
    }
}

impl std::error::Error for RindleError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            RindleError::InvalidUtf8 { source, .. } => Some(source),
            // `Sqlite.source` is now a stringified message (the core crate carries no
            // `rusqlite` type), so there is no nested `Error` to chain to.
            RindleError::Sqlite { .. }
            | RindleError::UnsupportedValue(_)
            | RindleError::Storage(_)
            | RindleError::ConsistencyViolation { .. }
            | RindleError::SchemaViolation(_)
            | RindleError::PushDeadlineExceeded { .. } => None,
        }
    }
}
