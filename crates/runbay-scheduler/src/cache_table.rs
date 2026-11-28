//! `CacheStateTable` — online sliding-TTL prefix-cluster cache ledger (P3a).
//!
//! Renewal rule (design v1.1 §3, adjudicated against review A1):
//! - TTL anchor = **request start** (Anthropic-style: TTL counts from request
//!   start, a long streaming response eats its own window).
//! - A successfully completed call carrying the cluster prefix **renews** the
//!   cluster (`last_touch = request_start`) on BOTH hit and miss — a miss is a
//!   cache *write* (the 1.25x write premium exists precisely to re-establish
//!   the cache), so "only hits renew" would systematically record warm
//!   clusters as cold and lose legitimate rescues.
//! - Aborted / below-cacheable-threshold calls only bump `last_activity`
//!   (feeds cluster-value estimates) and never touch `warm_until`.
//!
//! Persistence: append-only journal + periodic snapshot (atomic rename).
//! Entries store absolute wall-clock seconds; on restore, expired entries are
//! naturally cold (`warm_until < now`) — the conservative behavior required
//! by review B5 (a restart must not resurrect stale warmth).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Default provider-side prompt-cache TTL (seconds). Mirrors the 5-min tier.
pub const DEFAULT_TTL_S: f64 = 300.0;
/// Conservative floor for "this call is cacheable at all" (provider minimums
/// are in the 512..2048 token range; 1024 is the safe middle).
pub const DEFAULT_MIN_CACHEABLE_TOKENS: u64 = 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterEntry {
    /// Last renewal anchor (unix seconds, request-start based).
    pub last_touch: f64,
    /// Last time any call carried this cluster (unix seconds) — feeds reuse estimates.
    pub last_activity: f64,
    pub ttl_s: f64,
    pub prefix_tokens: u64,
    pub tenant: String,
    pub rev: u64,
    pub hits: u64,
    pub misses: u64,
    /// Set when the entry was restored from disk already-cold (observability).
    pub recovered_cold: bool,
}

impl ClusterEntry {
    pub fn warm_until(&self) -> f64 {
        self.last_touch + self.ttl_s
    }
}

/// One gateway call attributed to a cluster (input to [`CacheStateTable::on_call`]).
#[derive(Debug, Clone, Copy)]
pub struct CallEvent {
    /// When the request *started* (unix seconds) — the TTL anchor.
    pub request_start: f64,
    pub prompt_tokens: u64,
    pub cached_tokens: u64,
    /// True for aborted/revoked-in-flight calls (499) — never renew.
    pub aborted: bool,
    /// Cluster prefix size hint (updates the entry; `prompt_tokens` fallback).
    pub cluster_prefix_tokens: u64,
}

#[derive(Debug, Clone)]
pub struct CacheTableConfig {
    pub max_entries: usize,
    /// Entry drops to cold-summary after `warm_until` + `cold_after_mult × ttl`
    /// of inactivity.
    pub cold_after_mult: f64,
    /// Cold summaries are physically removed after this many seconds idle.
    pub gc_after_s: f64,
    pub min_cacheable_tokens: u64,
    /// Snapshot the journal every N appends.
    pub snapshot_every: u64,
}

impl Default for CacheTableConfig {
    fn default() -> Self {
        Self {
            max_entries: 4096,
            cold_after_mult: 2.0,
            gc_after_s: 86_400.0,
            min_cacheable_tokens: DEFAULT_MIN_CACHEABLE_TOKENS,
            snapshot_every: 256,
        }
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct Snapshot {
    entries: HashMap<String, ClusterEntry>,
}

/// Sliding-TTL cache ledger keyed by prefix-cluster id.
pub struct CacheStateTable {
    entries: HashMap<String, ClusterEntry>,
    cfg: CacheTableConfig,
    journal_path: Option<PathBuf>,
    snapshot_path: Option<PathBuf>,
    journal_lines: u64,
}

impl CacheStateTable {
    /// In-memory table (tests / sim-side).
    pub fn new(cfg: CacheTableConfig) -> Self {
        Self {
            entries: HashMap::new(),
            cfg,
            journal_path: None,
            snapshot_path: None,
            journal_lines: 0,
        }
    }

    /// Durable table under `state_dir/scheduler/`: restores snapshot + journal.
    /// Entries already past `warm_until` stay cold (wall-clock timestamps make
    /// this automatic; we additionally flag them `recovered_cold`).
    pub fn open(state_dir: &Path, cfg: CacheTableConfig) -> std::io::Result<Self> {
        let dir = state_dir.join("scheduler");
        fs::create_dir_all(&dir)?;
        let snapshot_path = dir.join("cache_table.snapshot.json");
        let journal_path = dir.join("cache_table.journal.jsonl");

        let mut entries: HashMap<String, ClusterEntry> = if snapshot_path.exists() {
            let raw = fs::read_to_string(&snapshot_path)?;
            serde_json::from_str::<Snapshot>(&raw)
                .map(|s| s.entries)
                .unwrap_or_default()
        } else {
            HashMap::new()
        };

        if journal_path.exists() {
            for line in fs::read_to_string(&journal_path)?.lines() {
                if line.trim().is_empty() {
                    continue;
                }
                if let Ok(ev) = serde_json::from_str::<JournalEvent>(line) {
                    ev.apply(&mut entries);
                }
            }
        }

        // Recovery clamp (B5): anything already past warm_until is cold — flag it.
        let now = unix_now();
        for e in entries.values_mut() {
            if e.warm_until() < now {
                e.recovered_cold = true;
            }
        }

        Ok(Self {
            entries,
            cfg,
            journal_path: Some(journal_path),
            snapshot_path: Some(snapshot_path),
            journal_lines: 0,
        })
    }

    pub fn warm_until(&self, cluster: &str) -> Option<f64> {
        self.entries.get(cluster).map(|e| e.warm_until())
    }

    /// Remaining TTL at `now`; `None` = unknown cluster, `Some(r ≤ 0)` = cold.
    pub fn rem_ttl(&self, cluster: &str, now: f64) -> Option<f64> {
        self.entries.get(cluster).map(|e| e.warm_until() - now)
    }

    pub fn entry(&self, cluster: &str) -> Option<&ClusterEntry> {
        self.entries.get(cluster)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Number of entries still warm at `now` (`warm_until > now`) — the
    /// high-water "cache resident" count exported by /metrics (P3c).
    pub fn warm_count(&self, now: f64) -> usize {
        self.entries
            .values()
            .filter(|e| e.warm_until() > now)
            .count()
    }

    /// Feed one gateway call for `cluster` (see module docs for the renewal rule).
    pub fn on_call(&mut self, cluster: &str, tenant: &str, rev: u64, ev: &CallEvent, now: f64) {
        let cacheable = !ev.aborted && ev.prompt_tokens >= self.cfg.min_cacheable_tokens;
        let entry = self
            .entries
            .entry(cluster.to_string())
            .or_insert_with(|| ClusterEntry {
                last_touch: 0.0,
                last_activity: 0.0,
                ttl_s: DEFAULT_TTL_S,
                prefix_tokens: ev.cluster_prefix_tokens,
                tenant: tenant.to_string(),
                rev,
                hits: 0,
                misses: 0,
                recovered_cold: false,
            });

        entry.last_activity = ev.request_start.max(entry.last_activity);
        entry.tenant = tenant.to_string();
        entry.rev = rev;
        if ev.cluster_prefix_tokens > 0 {
            entry.prefix_tokens = ev.cluster_prefix_tokens.max(entry.prefix_tokens);
        }
        if cacheable {
            // Hit AND miss renew — anchored at request START (v1.1 rule).
            entry.last_touch = ev.request_start.max(entry.last_touch);
            if ev.cached_tokens > 0 {
                entry.hits += 1;
            } else {
                entry.misses += 1;
            }
            self.journal_append(&JournalEvent {
                op: "renew".into(),
                cluster: cluster.into(),
                at: ev.request_start,
                hit: ev.cached_tokens > 0,
            });
        } else {
            self.journal_append(&JournalEvent {
                op: "activity".into(),
                cluster: cluster.into(),
                at: ev.request_start,
                hit: false,
            });
        }
        self.maybe_snapshot();
        self.gc(now);
    }

    /// Record cluster activity without renewal (e.g. a job submitted against it).
    pub fn note_activity(&mut self, cluster: &str, now: f64) {
        if let Some(e) = self.entries.get_mut(cluster) {
            e.last_activity = e.last_activity.max(now);
        }
    }

    /// Layered eviction (B5): idle entries past `warm_until + cold_after_mult × ttl`
    /// lose detail, then disappear entirely after `gc_after_s` idle. Also LRU
    /// caps the map at `max_entries`.
    pub fn gc(&mut self, now: f64) {
        let gc_after = self.cfg.gc_after_s;
        let cold_mult = self.cfg.cold_after_mult;
        self.entries.retain(|_, e| {
            let idle = now - e.last_activity;
            if idle <= 0.0 {
                return true;
            }
            if idle > gc_after {
                return false;
            }
            // Cold-summary threshold (M3): `warm_until + cold_after_mult × ttl`.
            // The earlier `1.0 + cold_mult` was off by one ttl vs the config
            // comment; use the documented multiplier.
            let cold_limit = e.ttl_s * cold_mult;
            if now > e.warm_until() + cold_limit {
                // Drop the warm-era hit/miss history, but KEEP `prefix_tokens`
                // — it is a stable cluster property (rescue qualification +
                // prefix-size hint), and zeroing it would permanently break the
                // `cluster_prefix_tokens` bookkeeping in `on_gateway_call`.
                e.hits = 0;
                e.misses = 0;
            }
            true
        });

        if self.entries.len() > self.cfg.max_entries {
            let mut victims: Vec<(String, f64)> = self
                .entries
                .iter()
                .map(|(k, e)| (k.clone(), e.last_activity))
                .collect();
            victims.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
            let excess = self.entries.len() - self.cfg.max_entries;
            for (k, _) in victims.into_iter().take(excess) {
                self.entries.remove(&k);
            }
        }
    }

    fn journal_append(&mut self, ev: &JournalEvent) {
        let Some(path) = &self.journal_path else {
            return;
        };
        match fs::OpenOptions::new().create(true).append(true).open(path) {
            Ok(mut f) => {
                if let Ok(line) = serde_json::to_string(ev) {
                    if writeln!(f, "{line}").is_err() {
                        // L2: surface (don't swallow) journal write failures.
                        eprintln!(
                            "runbayd: cache-table journal write failed for {}",
                            path.display()
                        );
                        return;
                    }
                    self.journal_lines += 1;
                }
            }
            Err(e) => {
                eprintln!(
                    "runbayd: cache-table journal open failed for {}: {e}",
                    path.display()
                );
            }
        }
    }

    fn maybe_snapshot(&mut self) {
        if self.journal_lines == 0 || !self.journal_lines.is_multiple_of(self.cfg.snapshot_every) {
            return;
        }
        let (Some(snap), Some(journal)) = (&self.snapshot_path, &self.journal_path) else {
            return;
        };
        let body = serde_json::to_string(&Snapshot {
            entries: self.entries.clone(),
        })
        .unwrap_or_default();
        let tmp = snap.with_extension("tmp");
        if fs::write(&tmp, &body).is_ok() {
            let _ = fs::rename(&tmp, snap);
            let _ = fs::remove_file(journal); // compacted into snapshot
            self.journal_lines = 0;
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct JournalEvent {
    op: String,
    cluster: String,
    at: f64,
    #[serde(default)]
    hit: bool,
}

impl JournalEvent {
    fn apply(&self, entries: &mut HashMap<String, ClusterEntry>) {
        let e = entries
            .entry(self.cluster.clone())
            .or_insert_with(|| ClusterEntry {
                last_touch: 0.0,
                last_activity: 0.0,
                ttl_s: DEFAULT_TTL_S,
                prefix_tokens: 0,
                tenant: String::new(),
                rev: 0,
                hits: 0,
                misses: 0,
                recovered_cold: false,
            });
        e.last_activity = e.last_activity.max(self.at);
        if self.op == "renew" {
            e.last_touch = e.last_touch.max(self.at);
            if self.hit {
                e.hits += 1;
            } else {
                e.misses += 1;
            }
        }
    }
}

/// FNV-1a 64-bit prompt-head fingerprint — fallback cluster key when the
/// caller supplies no explicit `cluster_id` (design §3.1).
pub fn fingerprint_prompt(prompt: &str) -> String {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut h = OFFSET;
    for b in prompt
        .chars()
        .take(4096)
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .bytes()
    {
        h ^= b as u64;
        h = h.wrapping_mul(PRIME);
    }
    format!("fp_{h:016x}")
}

fn unix_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(start: f64, prompt: u64, cached: u64, aborted: bool) -> CallEvent {
        CallEvent {
            request_start: start,
            prompt_tokens: prompt,
            cached_tokens: cached,
            aborted,
            cluster_prefix_tokens: 40_000,
        }
    }

    #[test]
    fn miss_renews_hit_renews_abort_does_not() {
        let mut t = CacheStateTable::new(CacheTableConfig::default());
        // miss at t=100 (2000 prompt tokens) → renews (miss = cache write)
        t.on_call("c1", "t1", 1, &ev(100.0, 2000, 0, false), 110.0);
        assert_eq!(t.rem_ttl("c1", 110.0), Some(290.0));

        // abort at t=200 → activity only, no renewal
        t.on_call("c1", "t1", 1, &ev(200.0, 2000, 0, true), 205.0);
        assert_eq!(t.rem_ttl("c1", 205.0), Some(195.0), "abort must not renew");

        // hit at t=250 → renews, anchor = request start (250), not completion
        t.on_call("c1", "t1", 1, &ev(250.0, 2000, 1900, false), 260.0);
        assert_eq!(t.rem_ttl("c1", 260.0), Some(290.0));

        // below cacheable floor → activity only
        t.on_call("c1", "t1", 1, &ev(300.0, 10, 0, false), 301.0);
        assert_eq!(
            t.rem_ttl("c1", 301.0),
            Some(249.0),
            "below-floor must not renew"
        );
    }

    #[test]
    fn anchor_is_request_start_not_completion() {
        let mut t = CacheStateTable::new(CacheTableConfig::default());
        // request starts t=100, completes t=160 (long stream) — anchor is 100
        t.on_call("c1", "t1", 1, &ev(100.0, 2000, 0, false), 160.0);
        assert_eq!(t.rem_ttl("c1", 160.0), Some(240.0));
    }

    #[test]
    fn fingerprint_is_stable_and_whitespace_insensitive() {
        let a = fingerprint_prompt("hello   world\n\tfoo");
        let b = fingerprint_prompt("hello world foo");
        assert_eq!(a, b);
        assert_ne!(a, fingerprint_prompt("hello world bar"));
    }

    #[test]
    fn gc_evicts_long_idle_entries() {
        let cfg = CacheTableConfig {
            gc_after_s: 100.0,
            ..Default::default()
        };
        let mut t = CacheStateTable::new(cfg);
        t.on_call("c1", "t1", 1, &ev(0.0, 2000, 0, false), 1.0);
        t.gc(50.0);
        assert!(t.entry("c1").is_some());
        t.gc(200.0); // idle 200 > 100
        assert!(t.entry("c1").is_none());
    }

    #[test]
    fn gc_cold_summary_preserves_prefix_and_resets_hit_history() {
        // M3 regression: the cold summary must NOT zero `prefix_tokens` (a
        // stable cluster property), and its threshold is `cold_after_mult × ttl`
        // after `warm_until` (not `1.0 + cold_after_mult`).
        let cfg = CacheTableConfig {
            gc_after_s: 1000.0,
            ..Default::default()
        };
        let mut t = CacheStateTable::new(cfg);
        // miss at t=100 (ttl=300): warm_until=400, last_activity=100.
        t.on_call("c1", "t1", 1, &ev(100.0, 2000, 0, false), 110.0);
        assert_eq!(t.entry("c1").unwrap().prefix_tokens, 40_000);
        assert_eq!(t.entry("c1").unwrap().misses, 1);
        // Past warm_until + 2×ttl = 400 + 600 = 1000.
        t.gc(1001.0);
        let e = t.entry("c1").unwrap();
        assert_eq!(
            e.prefix_tokens, 40_000,
            "prefix_tokens is stable, not warmth"
        );
        assert_eq!(e.hits, 0, "warm-era hit history reset");
        assert_eq!(e.misses, 0, "warm-era miss history reset");
    }

    #[test]
    fn persistence_restore_clamps_cold() {
        let dir = std::env::temp_dir().join(format!("rb_cache_test_{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("scheduler")).unwrap();

        {
            let mut t = CacheStateTable::open(&dir, CacheTableConfig::default()).unwrap();
            t.on_call(
                "c1",
                "t1",
                1,
                &ev(unix_now() - 1000.0, 2000, 0, false),
                unix_now() - 990.0,
            );
        }
        // reopen: entry restored but warm_until long past → cold
        let t = CacheStateTable::open(&dir, CacheTableConfig::default()).unwrap();
        let e = t.entry("c1").expect("entry restored");
        assert!(e.recovered_cold, "stale warmth must not survive restart");
        assert!(t.rem_ttl("c1", unix_now()).unwrap() < 0.0);
        let _ = fs::remove_dir_all(&dir);
    }
}
