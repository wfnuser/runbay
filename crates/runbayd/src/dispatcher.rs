//! `DispatchQueue` — P3b unified arbitration queue (design §3/§4.5).
//!
//! One pickable pool: admitted jobs wait here for a worker slot; deferred jobs
//! carry a `defer_until` deadline and escalate to the forced tier (same
//! arbitration as the ceiling, B3 — no second queue-jumping mechanism).
//! Deferred jobs may still be picked early: the rescue bonus in `score_job_v2`
//! surfaces them before their deadline whenever slots free up (work-conserving).
//!
//! Slot bookkeeping mirrors the simulator's worker warm-env state: each
//! capacity slot remembers the last (tenant, rev, session) it ran, which feeds
//! online affinity scoring.

use crate::runtime_pool::{DispatchCandidate, SubmitMeta};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::{oneshot, OwnedSemaphorePermit};

/// Handed to a waiting run task when the dispatcher picks it.
pub struct DispatchTicket {
    pub permit: OwnedSemaphorePermit,
    pub slot_id: usize,
}

pub struct DispatchEntry {
    pub meta: SubmitMeta,
    pub enqueued_at: f64,
    /// `Some` = admitted via the DeferPool path (design §4.1).
    pub defer_until: Option<f64>,
    /// Set once `defer_until` has passed (or the rescue window forces it).
    pub escalated: bool,
    pub from_defer: bool,
    pub ticket: oneshot::Sender<DispatchTicket>,
}

/// Slot occupancy + warm-env affinity memory (design §4.2 worker context).
#[derive(Debug, Clone, Default)]
pub struct SlotState {
    pub busy: bool,
    pub running_bg: bool,
    /// Reserved for the background slice (design §4.2): a reserved slot must
    /// serve bg when bg is pending, borrowing shared work otherwise.
    pub reserved_bg: bool,
    pub tenant: Option<String>,
    pub rev: Option<u64>,
    /// Session = cluster id of the last job run on this slot.
    pub session: Option<String>,
}

#[derive(Default)]
pub struct DispatchQueue {
    entries: Vec<DispatchEntry>,
    slots: Vec<SlotState>,
    free_slots: VecDeque<usize>,
}

impl DispatchQueue {
    pub fn new(max_concurrent: usize, n_reserved_bg: usize) -> Self {
        let n = max_concurrent.max(1);
        // Mirror the simulator: at least one slot is never reserved (so
        // foreground work always has a home), and reservation only applies
        // when `n_reserved_bg > 0`.
        let reserved = n_reserved_bg.min(n.saturating_sub(1));
        let mut slots = vec![SlotState::default(); n];
        for s in slots.iter_mut().take(reserved) {
            s.reserved_bg = true;
        }
        Self {
            entries: Vec::new(),
            slots,
            free_slots: (0..n).collect(),
        }
    }

    pub fn push(&mut self, entry: DispatchEntry) {
        self.entries.push(entry);
    }

    /// Cancel path: remove a not-yet-dispatched job. Returns true if removed.
    /// Zero-leak guarantee (design §8.1) — the waiting task calls this when its
    /// cancel/shutdown select fires before a ticket arrives.
    pub fn remove(&mut self, run_id: &str) -> bool {
        let before = self.entries.len();
        self.entries.retain(|e| e.meta.run_id != run_id);
        before != self.entries.len()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Queued (not deferred) depth.
    pub fn queued_count(&self) -> usize {
        self.entries.iter().filter(|e| e.defer_until.is_none()).count()
    }

    /// Deferred depth (design B3: combined quota with queued).
    pub fn deferred_count(&self) -> usize {
        self.entries.iter().filter(|e| e.defer_until.is_some()).count()
    }

    /// Mark deferred entries whose deadline has passed as escalated. Returns
    /// their run ids (caller flips `RunState::Deferred → Queued`).
    pub fn escalate_due(&mut self, now: f64) -> Vec<String> {
        let mut escalated = Vec::new();
        for e in &mut self.entries {
            if !e.escalated {
                if let Some(d) = e.defer_until {
                    if now >= d {
                        e.escalated = true;
                        escalated.push(e.meta.run_id.clone());
                    }
                }
            }
        }
        escalated
    }

    /// Snapshot for scoring. Indices align with `take(idx)`.
    pub fn candidates(&self) -> Vec<DispatchCandidate> {
        self.entries
            .iter()
            .map(|e| DispatchCandidate {
                meta: e.meta.clone(),
                enqueued_at: e.enqueued_at,
                defer_until: e.defer_until,
                escalated: e.escalated,
                from_defer: e.from_defer,
            })
            .collect()
    }

    pub fn take(&mut self, idx: usize) -> DispatchEntry {
        self.entries.remove(idx)
    }

    pub fn pop_free_slot(&mut self) -> Option<usize> {
        self.free_slots.pop_front()
    }

    pub fn slot_ctx(&self, slot_id: usize) -> crate::runtime_pool::SlotCtx {
        self.slots
            .get(slot_id)
            .map(|s| crate::runtime_pool::SlotCtx {
                session: s.session.clone(),
                tenant: s.tenant.clone(),
                rev: s.rev,
            })
            .unwrap_or_default()
    }

    /// Whether the given slot is reserved for the background slice.
    pub fn slot_reserved_bg(&self, slot_id: usize) -> bool {
        self.slots.get(slot_id).map(|s| s.reserved_bg).unwrap_or(false)
    }

    /// Count of busy slots currently running a background job. Used by tests
    /// and future P3c `/metrics`; production dispatch uses per-slot
    /// `slot_reserved_bg` instead.
    #[allow(dead_code)]
    pub fn bg_running(&self) -> usize {
        self.slots.iter().filter(|s| s.busy && s.running_bg).count()
    }

    /// Occupy a slot with the dispatched run.
    pub fn assign(&mut self, slot_id: usize, entry: &DispatchEntry) {
        if let Some(s) = self.slots.get_mut(slot_id) {
            s.busy = true;
            s.running_bg = entry.meta.priority == "background";
            s.tenant = Some(entry.meta.tenant.clone());
            s.rev = Some(entry.meta.rev);
            s.session = Some(entry.meta.cluster_id.clone());
        }
    }

    /// Free a slot but KEEP its affinity memory (warm-env reuse, design §4.2).
    pub fn release(&mut self, slot_id: usize) {
        if let Some(s) = self.slots.get_mut(slot_id) {
            s.busy = false;
            s.running_bg = false;
        }
        if !self.free_slots.contains(&slot_id) {
            self.free_slots.push_back(slot_id);
        }
    }

    /// Slots that can still accept work (free + not yet assigned).
    #[allow(dead_code)] // used by tests and P3c metrics
    pub fn free_slot_count(&self) -> usize {
        self.free_slots.len()
    }
}

/// RAII slot guard: any early return of a dispatched run task frees the slot
/// while preserving affinity memory.
pub struct SlotGuard {
    queue: Arc<parking_lot::Mutex<DispatchQueue>>,
    slot_id: usize,
    released: bool,
}

impl SlotGuard {
    /// Occupancy is assigned by the dispatcher; this guard only owns release.
    pub fn new(queue: Arc<parking_lot::Mutex<DispatchQueue>>, slot_id: usize) -> Self {
        Self {
            queue,
            slot_id,
            released: false,
        }
    }
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        if !self.released {
            self.released = true;
            self.queue.lock().release(self.slot_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_pool::admit_floor_pass;

    fn meta(run: &str, priority: &str) -> SubmitMeta {
        SubmitMeta {
            run_id: run.into(),
            cluster_id: format!("c_{run}"),
            tenant: "t1".into(),
            rev: 1,
            priority: priority.into(),
            prompt_tokens_est: 40_000,
            submitted_at: 1000.0,
            deadline_budget_s: None,
            rescuable: false,
        }
    }

    fn entry(run: &str, priority: &str, enq: f64, defer_until: Option<f64>) -> DispatchEntry {
        let (tx, _rx) = oneshot::channel();
        DispatchEntry {
            meta: meta(run, priority),
            enqueued_at: enq,
            defer_until,
            escalated: false,
            from_defer: defer_until.is_some(),
            ticket: tx,
        }
    }

    #[test]
    fn cancel_removal_is_zero_leak() {
        let mut q = DispatchQueue::new(2, 0);
        q.push(entry("r1", "interactive", 0.0, None));
        q.push(entry("r2", "interactive", 0.0, Some(30.0)));
        assert_eq!(q.len(), 2);
        assert!(q.remove("r1"));
        assert!(!q.remove("r1"), "double remove must report absent");
        assert_eq!(q.len(), 1);
        assert_eq!(q.deferred_count(), 1);
    }

    #[test]
    fn defer_expiry_escalates_to_forced_tier() {
        let mut q = DispatchQueue::new(2, 0);
        q.push(entry("d1", "interactive", 0.0, Some(27.0)));
        q.push(entry("d2", "interactive", 0.0, Some(99.0)));
        let esc = q.escalate_due(28.0);
        assert_eq!(esc, vec!["d1".to_string()]);
        let cands = q.candidates();
        assert!(cands[0].escalated, "d1 must be escalated after deadline");
        assert!(!cands[1].escalated, "d2 deadline not reached");
        // Idempotent: second pass escalates nothing new.
        assert!(q.escalate_due(29.0).is_empty());
    }

    #[test]
    fn slot_affinity_survives_release() {
        let mut q = DispatchQueue::new(2, 0);
        let e = entry("r1", "background", 0.0, None);
        let slot = q.pop_free_slot().unwrap();
        q.assign(slot, &e);
        assert_eq!(q.bg_running(), 1);
        q.release(slot);
        assert_eq!(q.bg_running(), 0, "released slot must not count as running");
        let ctx = q.slot_ctx(slot);
        assert_eq!(ctx.tenant.as_deref(), Some("t1"), "affinity kept after release");
        assert_eq!(ctx.rev, Some(1));
        assert_eq!(ctx.session.as_deref(), Some("c_r1"));
        assert_eq!(q.free_slot_count(), 2);
    }

    #[test]
    fn admit_floor_is_deterministic() {
        // Same run id always yields the same coin.
        let a = admit_floor_pass("run_deadbeef", 0.05);
        let b = admit_floor_pass("run_deadbeef", 0.05);
        assert_eq!(a, b);
        // floor=0 → never; floor=1 → always.
        assert!(!admit_floor_pass("run_deadbeef", 0.0));
        assert!(admit_floor_pass("run_deadbeef", 1.0));
        // Statistical sanity: ~5% over 2000 distinct ids (wide band).
        let hits = (0..2000)
            .filter(|i| admit_floor_pass(&format!("run_{i}"), 0.05))
            .count();
        assert!(
            (30..170).contains(&hits),
            "floor pass-rate wildly off: {hits}/2000"
        );
    }

    #[test]
    fn reserved_bg_slots_are_marked_and_capped() {
        // M1 regression: reservation is per-worker, not a global "force one bg".
        let q = DispatchQueue::new(4, 2);
        assert!(q.slot_reserved_bg(0));
        assert!(q.slot_reserved_bg(1));
        assert!(!q.slot_reserved_bg(2));
        assert!(!q.slot_reserved_bg(3));
        // n_reserved_bg >= max_concurrent → clamp to max-1 so fg keeps a slot.
        let q2 = DispatchQueue::new(2, 99);
        assert!(q2.slot_reserved_bg(0));
        assert!(!q2.slot_reserved_bg(1));
        // n_reserved_bg = 0 → no reservation at all.
        let q3 = DispatchQueue::new(2, 0);
        assert!(!q3.slot_reserved_bg(0));
        assert!(!q3.slot_reserved_bg(1));
    }
}
