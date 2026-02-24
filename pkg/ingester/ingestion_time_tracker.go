package ingester

import (
	"sync"
	"time"
)

// IngestionTimeTracker stores the first ingestion time per series (keyed by head series ref).
// Used to compute query-to-ingestion delay for head series only. Entries are evicted after retention.
type IngestionTimeTracker struct {
	mu        sync.RWMutex
	byRef     map[uint64]int64 // head series ref -> firstSeenUnixNanos
	retention time.Duration
}

// NewIngestionTimeTracker creates a tracker that retains first-seen times for the given duration.
func NewIngestionTimeTracker(retention time.Duration) *IngestionTimeTracker {
	if retention <= 0 {
		retention = 2 * time.Hour
	}
	return &IngestionTimeTracker{
		byRef:     make(map[uint64]int64),
		retention: retention,
	}
}

// RecordIfNew records the given time as the first ingestion time for the series identified by ref (head series ref).
// If the series was already present, it does nothing (does not overwrite).
func (t *IngestionTimeTracker) RecordIfNew(ref uint64, now time.Time) {
	nowNanos := now.UnixNano()
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.byRef[ref]; !exists {
		t.byRef[ref] = nowNanos
	}
}

// GetIngestionTime returns the first ingestion time for the series if known.
func (t *IngestionTimeTracker) GetIngestionTime(ref uint64) (time.Time, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	nanos, ok := t.byRef[ref]
	if !ok {
		return time.Time{}, false
	}
	return time.Unix(0, nanos), true
}

// Evict removes entries whose first-seen time is older than (now - retention).
// Prefer OnSeriesDeleted for cleanup when series are removed from the head.
func (t *IngestionTimeTracker) Evict(now time.Time) {
	cutoff := now.Add(-t.retention).UnixNano()
	t.mu.Lock()
	defer t.mu.Unlock()
	for r, nanos := range t.byRef {
		if nanos < cutoff {
			delete(t.byRef, r)
		}
	}
}

// OnSeriesDeleted removes tracking for the given series refs (called from PostDeletion when series are removed from the head).
// This is more effective than time-based Evict as it removes entries in O(len(refs)) without scanning the whole map.
func (t *IngestionTimeTracker) OnSeriesDeleted(refs []uint64) {
	if len(refs) == 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, ref := range refs {
		delete(t.byRef, ref)
	}
}

// FirstQueriedTracker tracks which series have already been observed for the "first query" delay metric.
// Keyed by head series ref. Evict by time so the same series can be observed again after retention.
type FirstQueriedTracker struct {
	mu        sync.RWMutex
	byRef     map[uint64]int64 // head series ref -> firstQueriedUnixNanos (for eviction)
	retention time.Duration
}

// NewFirstQueriedTracker creates a tracker with the given retention for eviction.
func NewFirstQueriedTracker(retention time.Duration) *FirstQueriedTracker {
	if retention <= 0 {
		retention = 2 * time.Hour
	}
	return &FirstQueriedTracker{
		byRef:     make(map[uint64]int64),
		retention: retention,
	}
}

// RecordIfNew records that the series was queried at now. It returns true if this was the first time
// (caller should observe the delay). If the series was already recorded, it returns false.
func (f *FirstQueriedTracker) RecordIfNew(ref uint64, now time.Time) (firstTime bool) {
	nowNanos := now.UnixNano()
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.byRef[ref]; exists {
		return false
	}
	f.byRef[ref] = nowNanos
	return true
}

// Evict removes entries whose first-queried time is older than (now - retention).
// Prefer OnSeriesDeleted for cleanup when series are removed from the head.
func (f *FirstQueriedTracker) Evict(now time.Time) {
	cutoff := now.Add(-f.retention).UnixNano()
	f.mu.Lock()
	defer f.mu.Unlock()
	for r, nanos := range f.byRef {
		if nanos < cutoff {
			delete(f.byRef, r)
		}
	}
}

// OnSeriesDeleted removes tracking for the given series refs (called from PostDeletion when series are removed from the head).
// This is more effective than time-based Evict as it removes entries in O(len(refs)) without scanning the whole map.
func (f *FirstQueriedTracker) OnSeriesDeleted(refs []uint64) {
	if len(refs) == 0 {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, ref := range refs {
		delete(f.byRef, ref)
	}
}
