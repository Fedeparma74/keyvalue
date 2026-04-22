//! Range / pagination primitives shared by all KV traits.
//!
//! These types describe a slice of the ordered key-space to iterate,
//! independently of the backend.  Every KV trait in this crate exposes an
//! `iter_range` method that accepts a [`KeyRange`]; all other pagination
//! helpers ([`iter_paginated`](crate::KeyValueDB::iter_paginated),
//! [`iter_from_prefix_paginated`](crate::KeyValueDB::iter_from_prefix_paginated),
//! ...) are thin convenience wrappers built on top of it.
//!
//! Key comparison is performed **byte-wise on the UTF-8 representation**,
//! which matches the natural ordering used by every supported backend
//! (fjall, redb, rocksdb, sqlite, indexed-db, ...).

#[cfg(not(feature = "std"))]
use alloc::{
    string::{String, ToString},
    vec::Vec,
};

use core::cmp::Ordering;

/// Direction in which the ordered key space is traversed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Direction {
    /// Ascending byte-wise key order.
    #[default]
    Forward,
    /// Descending byte-wise key order.
    Reverse,
}

/// A bound on a key range (inclusive, exclusive, or open).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub enum Bound {
    /// No bound.
    #[default]
    Unbounded,
    /// Keys `>=` (resp. `<=`) this value are included.
    Included(String),
    /// Keys strictly `>` (resp. `<`) this value are included.
    Excluded(String),
}

impl Bound {
    /// Returns the bound key string, if any.
    pub fn key(&self) -> Option<&str> {
        match self {
            Bound::Included(k) | Bound::Excluded(k) => Some(k.as_str()),
            Bound::Unbounded => None,
        }
    }

    /// Returns `true` if `self` is [`Bound::Unbounded`].
    pub fn is_unbounded(&self) -> bool {
        matches!(self, Bound::Unbounded)
    }
}

/// A description of a slice of the ordered key space.
///
/// Construct with [`KeyRange::all`] / [`KeyRange::prefix`] and mutate with
/// the builder-style helpers.
///
/// ## Semantics
///
/// * [`lower`](Self::lower) / [`upper`](Self::upper) define the absolute
///   byte-wise range, regardless of direction.
/// * [`prefix`](Self::prefix) further constrains the range: only keys that
///   start with the given prefix are yielded.  It composes with
///   `lower`/`upper`: the effective forward bounds are
///   `max(lower, Included(prefix))` and `min(upper, prefix_successor(prefix))`.
/// * [`direction`](Self::direction) selects the iteration order.  With
///   [`Direction::Reverse`] entries are produced in descending order.
/// * [`limit`](Self::limit) caps the number of returned items.  `None`
///   means "no limit".
///
/// Builder helpers like [`start_after`](Self::start_after) /
/// [`start_from`](Self::start_from) adjust the bound corresponding to the
/// *current* iteration direction (`lower` for forward, `upper` for reverse),
/// which makes cursor-based pagination direction-agnostic:
///
/// ```ignore
/// KeyRange::prefix("tx:")
///     .reverse()
///     .start_after(cursor)      // skip past last key seen
///     .with_limit(limit + 1);   // fetch one extra to detect has_more
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KeyRange {
    pub lower: Bound,
    pub upper: Bound,
    pub prefix: Option<String>,
    pub direction: Direction,
    pub limit: Option<usize>,
}

impl KeyRange {
    /// Full-table range: forward, unbounded, unlimited.
    pub fn all() -> Self {
        Self::default()
    }

    /// Range restricted to keys starting with `prefix`.
    pub fn prefix(prefix: impl Into<String>) -> Self {
        Self {
            prefix: Some(prefix.into()),
            ..Self::default()
        }
    }

    /// Builder: set [`limit`](Self::limit).
    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Builder: set an explicit direction.
    pub fn with_direction(mut self, direction: Direction) -> Self {
        self.direction = direction;
        self
    }

    /// Builder shortcut for [`Direction::Forward`].
    pub fn forward(mut self) -> Self {
        self.direction = Direction::Forward;
        self
    }

    /// Builder shortcut for [`Direction::Reverse`].
    pub fn reverse(mut self) -> Self {
        self.direction = Direction::Reverse;
        self
    }

    /// Builder: set the prefix restriction.
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }

    /// Builder: set the absolute lower bound.
    pub fn with_lower(mut self, bound: Bound) -> Self {
        self.lower = bound;
        self
    }

    /// Builder: set the absolute upper bound.
    pub fn with_upper(mut self, bound: Bound) -> Self {
        self.upper = bound;
        self
    }

    /// Cursor-based pagination helper: skip past `key` in the current
    /// direction.  For [`Direction::Forward`] this sets the lower bound to
    /// [`Bound::Excluded`]; for [`Direction::Reverse`] it sets the upper
    /// bound to [`Bound::Excluded`].
    pub fn start_after(mut self, key: impl Into<String>) -> Self {
        let k = key.into();
        match self.direction {
            Direction::Forward => self.lower = Bound::Excluded(k),
            Direction::Reverse => self.upper = Bound::Excluded(k),
        }
        self
    }

    /// Like [`start_after`](Self::start_after) but inclusive of `key`.
    pub fn start_from(mut self, key: impl Into<String>) -> Self {
        let k = key.into();
        match self.direction {
            Direction::Forward => self.lower = Bound::Included(k),
            Direction::Reverse => self.upper = Bound::Included(k),
        }
        self
    }

    /// Returns `true` if `key` satisfies the lower bound (byte-wise).
    pub fn matches_lower(&self, key: &str) -> bool {
        match &self.lower {
            Bound::Unbounded => true,
            Bound::Included(b) => key >= b.as_str(),
            Bound::Excluded(b) => key > b.as_str(),
        }
    }

    /// Returns `true` if `key` satisfies the upper bound (byte-wise).
    pub fn matches_upper(&self, key: &str) -> bool {
        match &self.upper {
            Bound::Unbounded => true,
            Bound::Included(b) => key <= b.as_str(),
            Bound::Excluded(b) => key < b.as_str(),
        }
    }

    /// Returns `true` if `key` satisfies the prefix restriction.
    pub fn matches_prefix(&self, key: &str) -> bool {
        match &self.prefix {
            Some(p) => key.starts_with(p.as_str()),
            None => true,
        }
    }

    /// Returns `true` if `key` satisfies every constraint of this range
    /// (lower, upper, prefix).
    pub fn contains(&self, key: &str) -> bool {
        self.matches_lower(key) && self.matches_upper(key) && self.matches_prefix(key)
    }

    /// Returns `true` if the iteration for this range should stop once a
    /// key sorted beyond this bound is reached, allowing early exit on
    /// natively ordered backends.
    ///
    /// Specifically: once a key outside of the combined upper bound
    /// (forward) or lower bound (reverse) is observed, no further key in
    /// the native order can satisfy the range.
    pub fn is_beyond_far_end(&self, key: &str) -> bool {
        match self.direction {
            Direction::Forward => !self.matches_upper(key) || self.is_beyond_prefix_forward(key),
            Direction::Reverse => !self.matches_lower(key) || self.is_before_prefix_reverse(key),
        }
    }

    fn is_beyond_prefix_forward(&self, key: &str) -> bool {
        matches!(&self.prefix, Some(p) if key > p.as_str() && !key.starts_with(p.as_str()))
    }

    fn is_before_prefix_reverse(&self, key: &str) -> bool {
        matches!(&self.prefix, Some(p) if key < p.as_str())
    }
}

/// In-memory fallback implementation of [`KeyValueDB::iter_range`].
///
/// Used as the default for every trait; backends with native range
/// support MUST override their trait method for efficiency, but may still
/// reuse this helper during tests.
///
/// The algorithm is:
///   1. Filter the input stream by [`KeyRange::contains`].
///   2. Sort the surviving entries (stable).  Only necessary for
///      backends that do not return items in key order.
///   3. Apply direction: reverse if needed.
///   4. Truncate to `limit`.
///
/// The function takes ownership of `items` to avoid unnecessary clones.
#[allow(clippy::type_complexity)]
pub fn apply_range_in_memory<V>(mut items: Vec<(String, V)>, range: &KeyRange) -> Vec<(String, V)> {
    items.retain(|(k, _)| range.contains(k));
    items.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
    if range.direction == Direction::Reverse {
        items.reverse();
    }
    if let Some(limit) = range.limit {
        items.truncate(limit);
    }
    items
}

/// Strictly order two borrowed keys byte-wise, matching [`Ord`] on
/// `String` in this crate.
#[inline]
pub fn cmp_keys(a: &str, b: &str) -> Ordering {
    a.as_bytes().cmp(b.as_bytes())
}

/// Return the opaque cursor string that should be sent to the client to
/// continue iteration after `last_key` in the given direction.
///
/// Clients then pass this cursor back via
/// [`KeyRange::start_after`](KeyRange::start_after); the exact encoding is
/// backend-independent and is simply the last yielded key.
#[inline]
pub fn cursor_from_last_key(last_key: &str) -> String {
    last_key.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn pairs(keys: &[&str]) -> Vec<(String, Vec<u8>)> {
        keys.iter()
            .map(|k| (k.to_string(), k.as_bytes().to_vec()))
            .collect()
    }

    fn keys_from(items: Vec<(String, Vec<u8>)>) -> Vec<String> {
        items.into_iter().map(|(k, _)| k).collect()
    }

    // -----------------------------------------------------------------------
    // Bound
    // -----------------------------------------------------------------------

    #[test]
    fn bound_unbounded_is_unbounded() {
        let b = Bound::Unbounded;
        assert!(b.is_unbounded());
        assert!(b.key().is_none());
    }

    #[test]
    fn bound_included_key() {
        let b = Bound::Included("abc".to_string());
        assert!(!b.is_unbounded());
        assert_eq!(b.key(), Some("abc"));
    }

    #[test]
    fn bound_excluded_key() {
        let b = Bound::Excluded("xyz".to_string());
        assert!(!b.is_unbounded());
        assert_eq!(b.key(), Some("xyz"));
    }

    // -----------------------------------------------------------------------
    // KeyRange construction helpers
    // -----------------------------------------------------------------------

    #[test]
    fn key_range_all_defaults() {
        let r = KeyRange::all();
        assert_eq!(r.lower, Bound::Unbounded);
        assert_eq!(r.upper, Bound::Unbounded);
        assert!(r.prefix.is_none());
        assert_eq!(r.direction, Direction::Forward);
        assert!(r.limit.is_none());
    }

    #[test]
    fn key_range_prefix_ctor() {
        let r = KeyRange::prefix("tx:");
        assert_eq!(r.prefix, Some("tx:".to_string()));
        assert_eq!(r.lower, Bound::Unbounded);
        assert_eq!(r.upper, Bound::Unbounded);
    }

    #[test]
    fn key_range_builders() {
        let r = KeyRange::all()
            .with_limit(10)
            .with_direction(Direction::Reverse)
            .with_prefix("p:")
            .with_lower(Bound::Included("p:a".to_string()))
            .with_upper(Bound::Excluded("p:z".to_string()));

        assert_eq!(r.limit, Some(10));
        assert_eq!(r.direction, Direction::Reverse);
        assert_eq!(r.prefix, Some("p:".to_string()));
        assert_eq!(r.lower, Bound::Included("p:a".to_string()));
        assert_eq!(r.upper, Bound::Excluded("p:z".to_string()));
    }

    #[test]
    fn start_after_forward() {
        let r = KeyRange::all().start_after("k5");
        assert_eq!(r.lower, Bound::Excluded("k5".to_string()));
        assert_eq!(r.upper, Bound::Unbounded);
    }

    #[test]
    fn start_after_reverse() {
        let r = KeyRange::all().reverse().start_after("k5");
        assert_eq!(r.upper, Bound::Excluded("k5".to_string()));
        assert_eq!(r.lower, Bound::Unbounded);
    }

    #[test]
    fn start_from_forward() {
        let r = KeyRange::all().start_from("k3");
        assert_eq!(r.lower, Bound::Included("k3".to_string()));
    }

    #[test]
    fn start_from_reverse() {
        let r = KeyRange::all().reverse().start_from("k3");
        assert_eq!(r.upper, Bound::Included("k3".to_string()));
    }

    // -----------------------------------------------------------------------
    // KeyRange::contains / matches_lower / matches_upper / matches_prefix
    // -----------------------------------------------------------------------

    #[test]
    fn contains_unbounded_range() {
        let r = KeyRange::all();
        assert!(r.contains(""));
        assert!(r.contains("a"));
        assert!(r.contains("zzz"));
    }

    #[test]
    fn contains_lower_included() {
        let r = KeyRange::all().with_lower(Bound::Included("c".to_string()));
        assert!(!r.contains("b"));
        assert!(r.contains("c"));
        assert!(r.contains("d"));
    }

    #[test]
    fn contains_lower_excluded() {
        let r = KeyRange::all().with_lower(Bound::Excluded("c".to_string()));
        assert!(!r.contains("b"));
        assert!(!r.contains("c"));
        assert!(r.contains("c\0")); // next byte
        assert!(r.contains("d"));
    }

    #[test]
    fn contains_upper_included() {
        let r = KeyRange::all().with_upper(Bound::Included("c".to_string()));
        assert!(r.contains("b"));
        assert!(r.contains("c"));
        assert!(!r.contains("d"));
    }

    #[test]
    fn contains_upper_excluded() {
        let r = KeyRange::all().with_upper(Bound::Excluded("c".to_string()));
        assert!(r.contains("b"));
        assert!(!r.contains("c"));
        assert!(!r.contains("d"));
    }

    #[test]
    fn contains_prefix_restriction() {
        let r = KeyRange::prefix("tx:");
        assert!(r.contains("tx:abc"));
        assert!(r.contains("tx:"));
        assert!(!r.contains("tx"));
        assert!(!r.contains("other"));
    }

    #[test]
    fn contains_combined_lower_upper_prefix() {
        let r = KeyRange::prefix("tx:")
            .with_lower(Bound::Included("tx:b".to_string()))
            .with_upper(Bound::Excluded("tx:d".to_string()));
        assert!(!r.contains("tx:a"));
        assert!(r.contains("tx:b"));
        assert!(r.contains("tx:c"));
        assert!(!r.contains("tx:d"));
    }

    // -----------------------------------------------------------------------
    // KeyRange::is_beyond_far_end
    // -----------------------------------------------------------------------

    #[test]
    fn is_beyond_far_end_forward_upper() {
        let r = KeyRange::all().with_upper(Bound::Excluded("k5".to_string()));
        assert!(!r.is_beyond_far_end("k4"));
        assert!(r.is_beyond_far_end("k5"));
        assert!(r.is_beyond_far_end("k6"));
    }

    #[test]
    fn is_beyond_far_end_reverse_lower() {
        let r = KeyRange::all()
            .reverse()
            .with_lower(Bound::Included("k3".to_string()));
        assert!(!r.is_beyond_far_end("k4"));
        assert!(!r.is_beyond_far_end("k3"));
        assert!(r.is_beyond_far_end("k2"));
    }

    #[test]
    fn is_beyond_far_end_prefix_forward() {
        let r = KeyRange::prefix("ab:");
        assert!(!r.is_beyond_far_end("ab:"));
        assert!(!r.is_beyond_far_end("ab:zzz"));
        // "ab;" > "ab:" but doesn't start with prefix → beyond end
        assert!(r.is_beyond_far_end("ab;"));
    }

    // -----------------------------------------------------------------------
    // apply_range_in_memory
    // -----------------------------------------------------------------------

    #[test]
    fn apply_range_empty_input() {
        let items: Vec<(String, Vec<u8>)> = vec![];
        let r = KeyRange::all();
        assert!(apply_range_in_memory(items, &r).is_empty());
    }

    #[test]
    fn apply_range_all_forward() {
        let items = pairs(&["b", "a", "c"]);
        let r = KeyRange::all();
        let out = keys_from(apply_range_in_memory(items, &r));
        assert_eq!(out, vec!["a", "b", "c"]);
    }

    #[test]
    fn apply_range_all_reverse() {
        let items = pairs(&["b", "a", "c"]);
        let r = KeyRange::all().reverse();
        let out = keys_from(apply_range_in_memory(items, &r));
        assert_eq!(out, vec!["c", "b", "a"]);
    }

    #[test]
    fn apply_range_with_limit_forward() {
        let items = pairs(&["a", "b", "c", "d", "e"]);
        let r = KeyRange::all().with_limit(3);
        let out = keys_from(apply_range_in_memory(items, &r));
        assert_eq!(out, vec!["a", "b", "c"]);
    }

    #[test]
    fn apply_range_with_limit_reverse() {
        let items = pairs(&["a", "b", "c", "d", "e"]);
        let r = KeyRange::all().reverse().with_limit(2);
        let out = keys_from(apply_range_in_memory(items, &r));
        assert_eq!(out, vec!["e", "d"]);
    }

    #[test]
    fn apply_range_lower_included() {
        let items = pairs(&["a", "b", "c", "d"]);
        let r = KeyRange::all().with_lower(Bound::Included("b".to_string()));
        let out = keys_from(apply_range_in_memory(items, &r));
        assert_eq!(out, vec!["b", "c", "d"]);
    }

    #[test]
    fn apply_range_lower_excluded() {
        let items = pairs(&["a", "b", "c", "d"]);
        let r = KeyRange::all().with_lower(Bound::Excluded("b".to_string()));
        let out = keys_from(apply_range_in_memory(items, &r));
        assert_eq!(out, vec!["c", "d"]);
    }

    #[test]
    fn apply_range_upper_included() {
        let items = pairs(&["a", "b", "c", "d"]);
        let r = KeyRange::all().with_upper(Bound::Included("c".to_string()));
        let out = keys_from(apply_range_in_memory(items, &r));
        assert_eq!(out, vec!["a", "b", "c"]);
    }

    #[test]
    fn apply_range_upper_excluded() {
        let items = pairs(&["a", "b", "c", "d"]);
        let r = KeyRange::all().with_upper(Bound::Excluded("c".to_string()));
        let out = keys_from(apply_range_in_memory(items, &r));
        assert_eq!(out, vec!["a", "b"]);
    }

    #[test]
    fn apply_range_prefix() {
        let items = pairs(&["tx:1", "tx:2", "other", "tx:3"]);
        let r = KeyRange::prefix("tx:");
        let out = keys_from(apply_range_in_memory(items, &r));
        assert_eq!(out, vec!["tx:1", "tx:2", "tx:3"]);
    }

    #[test]
    fn apply_range_prefix_reverse_limit() {
        let items = pairs(&["tx:1", "tx:2", "tx:3", "tx:4", "other"]);
        let r = KeyRange::prefix("tx:").reverse().with_limit(2);
        let out = keys_from(apply_range_in_memory(items, &r));
        assert_eq!(out, vec!["tx:4", "tx:3"]);
    }

    #[test]
    fn apply_range_cursor_pagination() {
        // Simulate cursor-based pagination: page 1
        let all = pairs(&["k1", "k2", "k3", "k4", "k5"]);
        let page1 = keys_from(apply_range_in_memory(
            all.clone(),
            &KeyRange::all().with_limit(2),
        ));
        assert_eq!(page1, vec!["k1", "k2"]);

        // page 2: start_after last seen
        let page2 = keys_from(apply_range_in_memory(
            all.clone(),
            &KeyRange::all().start_after("k2").with_limit(2),
        ));
        assert_eq!(page2, vec!["k3", "k4"]);

        // page 3
        let page3 = keys_from(apply_range_in_memory(
            all,
            &KeyRange::all().start_after("k4").with_limit(2),
        ));
        assert_eq!(page3, vec!["k5"]);
    }

    #[test]
    fn apply_range_reverse_cursor_pagination() {
        let all = pairs(&["k1", "k2", "k3", "k4", "k5"]);
        // Reverse page 1
        let page1 = keys_from(apply_range_in_memory(
            all.clone(),
            &KeyRange::all().reverse().with_limit(2),
        ));
        assert_eq!(page1, vec!["k5", "k4"]);

        // Reverse page 2: start_after last seen (k4 in reverse)
        let page2 = keys_from(apply_range_in_memory(
            all,
            &KeyRange::all().reverse().start_after("k4").with_limit(2),
        ));
        assert_eq!(page2, vec!["k3", "k2"]);
    }

    #[test]
    fn apply_range_limit_zero_returns_empty() {
        let items = pairs(&["a", "b", "c"]);
        let r = KeyRange::all().with_limit(0);
        assert!(apply_range_in_memory(items, &r).is_empty());
    }

    #[test]
    fn apply_range_limit_larger_than_input() {
        let items = pairs(&["a", "b"]);
        let r = KeyRange::all().with_limit(100);
        let out = keys_from(apply_range_in_memory(items, &r));
        assert_eq!(out, vec!["a", "b"]);
    }

    // -----------------------------------------------------------------------
    // cmp_keys
    // -----------------------------------------------------------------------

    #[test]
    fn cmp_keys_byte_order() {
        use core::cmp::Ordering;
        assert_eq!(cmp_keys("a", "b"), Ordering::Less);
        assert_eq!(cmp_keys("b", "a"), Ordering::Greater);
        assert_eq!(cmp_keys("abc", "abc"), Ordering::Equal);
        // byte-wise: "Z" (0x5A) < "a" (0x61)
        assert_eq!(cmp_keys("Z", "a"), Ordering::Less);
    }

    // -----------------------------------------------------------------------
    // cursor_from_last_key
    // -----------------------------------------------------------------------

    #[test]
    fn cursor_from_last_key_roundtrip() {
        let cursor = cursor_from_last_key("some:key:123");
        assert_eq!(cursor, "some:key:123");
    }
}
