//! `SQLiteStatFanout` — join fanout (average child rows per distinct parent key) from
//! SQLite's `sqlite_stat4`/`sqlite_stat1` statistics (port of
//! `zqlite/src/sqlite-stat-fanout.ts`).
//!
//! Strategy, per (table, columns): try `sqlite_stat4` (histogram; separates NULL from
//! non-NULL samples → accurate for sparse FKs), then `sqlite_stat1` (average, includes
//! NULLs), then a constant default. Results are cached per (table, sorted-columns).
//! Requires `SQLITE_ENABLE_STAT4` (on) + `ANALYZE` having been run + an index on the
//! join column(s).

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use rindle_planner::{FanoutConfidence, FanoutEst};
use rusqlite::{params, Connection};

struct IndexInfo {
    name: String,
    /// Number of leading columns matched (== `columns.len()`); the stat depth.
    depth: usize,
}

pub struct SQLiteStatFanout {
    conn: Rc<Connection>,
    default_fanout: f64,
    cache: RefCell<HashMap<String, FanoutEst>>,
}

impl SQLiteStatFanout {
    pub fn new(conn: Rc<Connection>, default_fanout: f64) -> Self {
        Self {
            conn,
            default_fanout,
            cache: RefCell::new(HashMap::new()),
        }
    }

    /// Fanout for a join on `columns` of `table` (`getFanout`): stat4 → stat1 → default,
    /// cached per (table, sorted columns).
    pub fn get_fanout(&self, table: &str, columns: &[&str]) -> FanoutEst {
        let mut sorted: Vec<&str> = columns.to_vec();
        sorted.sort_unstable();
        let key = format!("{table}:{}", sorted.join(","));

        if let Some(cached) = self.cache.borrow().get(&key) {
            return cached.clone();
        }

        let result = self
            .get_fanout_from_stat4(table, columns)
            .or_else(|| self.get_fanout_from_stat1(table, columns))
            .unwrap_or(FanoutEst {
                fanout: self.default_fanout,
                confidence: FanoutConfidence::None,
            });

        self.cache.borrow_mut().insert(key, result.clone());
        result
    }

    /// Fanout from `sqlite_stat4`: median of the non-NULL samples' `neq` at the matched
    /// depth (`#getFanoutFromStat4`). All-NULL → fanout 0 (NULLs don't join).
    fn get_fanout_from_stat4(&self, table: &str, columns: &[&str]) -> Option<FanoutEst> {
        let index = self.find_index_for_columns(table, columns)?;

        let mut stmt = self
            .conn
            .prepare(
                "SELECT neq, sample FROM sqlite_stat4 WHERE tbl = ?1 AND idx = ?2 ORDER BY nlt",
            )
            .ok()?;
        let samples: Vec<(String, Vec<u8>)> = stmt
            .query_map(params![table, index.name], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?))
            })
            .ok()?
            .filter_map(Result::ok)
            .collect();
        if samples.is_empty() {
            return None;
        }

        let neq_index = index.depth - 1; // depth is 1-based, the neq array 0-based
        let mut non_null: Vec<i64> = Vec::new();
        for (neq, sample) in &samples {
            if decode_sample_is_null(sample) {
                continue;
            }
            let parts: Vec<&str> = neq.split(' ').collect();
            let fanout: i64 = parts
                .get(neq_index)
                .or_else(|| parts.first())
                .and_then(|p| p.parse().ok())
                .unwrap_or(0);
            non_null.push(fanout);
        }

        if non_null.is_empty() {
            // All samples NULL → 0 (NULLs match nothing in a join).
            return Some(FanoutEst {
                fanout: 0.0,
                confidence: FanoutConfidence::High,
            });
        }

        // Median (more robust than mean); even length floors the mean of the two middle.
        non_null.sort_unstable();
        let n = non_null.len();
        let median = if n.is_multiple_of(2) {
            (non_null[n / 2 - 1] + non_null[n / 2]) / 2
        } else {
            non_null[n / 2]
        };
        Some(FanoutEst {
            fanout: median as f64,
            confidence: FanoutConfidence::High,
        })
    }

    /// Fanout from `sqlite_stat1`: the average at the matched depth (`#getFanoutFromStat1`).
    /// Includes NULL rows, so may overestimate for sparse columns.
    fn get_fanout_from_stat1(&self, table: &str, columns: &[&str]) -> Option<FanoutEst> {
        let index = self.find_index_for_columns(table, columns)?;

        let stat: String = self
            .conn
            .query_row(
                "SELECT stat FROM sqlite_stat1 WHERE tbl = ?1 AND idx = ?2",
                params![table, index.name],
                |r| r.get(0),
            )
            .ok()?;

        let parts: Vec<&str> = stat.split(' ').collect();
        if parts.len() < index.depth + 1 {
            return None;
        }
        let fanout: i64 = parts[index.depth].parse().ok()?;
        Some(FanoutEst {
            fanout: fanout as f64,
            confidence: FanoutConfidence::Med,
        })
    }

    /// Find an index whose first `columns.len()` columns are exactly `columns` (any
    /// order, case-insensitive; gaps disallowed) — `#findIndexForColumns`. Statistics at
    /// depth N are the fanout for the combination of the first N index columns.
    fn find_index_for_columns(&self, table: &str, columns: &[&str]) -> Option<IndexInfo> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT il.name AS index_name, ii.seqno, ii.name AS column_name
                 FROM pragma_index_list(?1) il
                 JOIN pragma_index_info(il.name) ii
                 ORDER BY il.seq, ii.seqno",
            )
            .ok()?;
        let rows: Vec<(String, String)> = stmt
            .query_map([table], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(2)?))
            })
            .ok()?
            .filter_map(Result::ok)
            .collect();

        // Group columns by index, preserving first-seen index order.
        let mut order: Vec<String> = Vec::new();
        let mut by_index: HashMap<String, Vec<String>> = HashMap::new();
        for (index_name, column_name) in rows {
            if !by_index.contains_key(&index_name) {
                order.push(index_name.clone());
            }
            by_index.entry(index_name).or_default().push(column_name);
        }

        for index_name in order {
            if is_prefix_match(columns, &by_index[&index_name]) {
                return Some(IndexInfo {
                    name: index_name,
                    depth: columns.len(),
                });
            }
        }
        None
    }
}

/// True if every `query_columns` appears in the first `query_columns.len()` positions of
/// `index_columns`, regardless of order (case-insensitive). Gaps disallowed
/// (`#isPrefixMatch`).
fn is_prefix_match(query_columns: &[&str], index_columns: &[String]) -> bool {
    if query_columns.len() > index_columns.len() {
        return false;
    }
    let prefix: std::collections::HashSet<String> = index_columns[..query_columns.len()]
        .iter()
        .map(|c| c.to_lowercase())
        .collect();
    query_columns
        .iter()
        .all(|q| prefix.contains(&q.to_lowercase()))
}

/// Decode whether a `sqlite_stat4` sample's first column is NULL (`#decodeSampleIsNull`).
/// SQLite record: a varint header size, then one serial type per column. Serial type 0
/// = NULL. We only check the first column (single-byte-header simplification, matching
/// the JS).
fn decode_sample_is_null(sample: &[u8]) -> bool {
    if sample.is_empty() {
        return true;
    }
    let header_size = sample[0] as usize;
    if header_size == 0 || header_size >= sample.len() {
        return true;
    }
    let serial_type = sample[1];
    serial_type == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    fn db(posts_per_user: i64) -> Rc<Connection> {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE posts (id INTEGER PRIMARY KEY, userId INTEGER);
             CREATE INDEX posts_userId ON posts(userId);",
        )
        .unwrap();
        for u in 0..100 {
            for p in 0..posts_per_user {
                conn.execute(
                    "INSERT INTO posts (id, userId) VALUES (?, ?)",
                    params![u * 1000 + p, u],
                )
                .unwrap();
            }
        }
        conn.execute_batch("ANALYZE;").unwrap();
        Rc::new(conn)
    }

    #[test]
    fn fanout_tracks_rows_per_key() {
        // ~5 posts per user → fanout ≈ 5 from stat4/stat1.
        let f = SQLiteStatFanout::new(db(5), 3.0);
        let r = f.get_fanout("posts", &["userId"]);
        assert!(
            r.fanout >= 3.0 && r.fanout <= 8.0,
            "expected ~5, got {} ({:?})",
            r.fanout,
            r.confidence
        );
        assert_ne!(r.confidence, FanoutConfidence::None); // came from real stats
    }

    #[test]
    fn unindexed_column_falls_back_to_default() {
        let f = SQLiteStatFanout::new(db(2), 3.0);
        // No index on `id`-as-join... use a column with no index match.
        let r = f.get_fanout("posts", &["nonexistent"]);
        assert_eq!(r.fanout, 3.0);
        assert_eq!(r.confidence, FanoutConfidence::None);
    }

    #[test]
    fn result_is_cached() {
        let f = SQLiteStatFanout::new(db(3), 3.0);
        let a = f.get_fanout("posts", &["userId"]);
        let b = f.get_fanout("posts", &["userId"]);
        assert_eq!(a.fanout, b.fanout);
        assert!(f.cache.borrow().contains_key("posts:userId"));
    }
}
