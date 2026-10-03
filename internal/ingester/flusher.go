package ingester

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/itsHabib/ingest/internal/index"
	"github.com/itsHabib/ingest/internal/observability"
	"github.com/itsHabib/ingest/internal/storage"
	"go.uber.org/zap"
)

// bufPool is a pool of bytes.Buffer used to avoid allocations during JSON
// marshaling in the flush hot path.
var bufPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

// Flusher serializes batches to JSON and writes them to an ObjectStore.
type Flusher struct {
	store      storage.ObjectStore
	indexStore index.IndexStore // optional; nil means no index writes
	logger     *zap.Logger
	seq        atomic.Int64 // monotonic counter to ensure unique storage keys
}

// NewFlusher creates a Flusher backed by the given ObjectStore.
func NewFlusher(store storage.ObjectStore, logger *zap.Logger) *Flusher {
	return &Flusher{
		store:  store,
		logger: logger,
	}
}

// SetIndexStore configures the flusher to write block metadata to the given
// IndexStore after each successful batch flush. Pass nil to disable.
func (f *Flusher) SetIndexStore(is index.IndexStore) {
	f.indexStore = is
}

// Flush serializes the batch to JSON, generates the storage key, and writes
// it to the object store.
func (f *Flusher) Flush(ctx context.Context, job *flushJob) error {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)

	if err := json.NewEncoder(buf).Encode(job.batch); err != nil {
		return fmt.Errorf("flusher: marshal batch: %w", err)
	}

	seq := f.seq.Add(1)
	key, err := storage.BatchKey(job.batch.TenantID, time.Now().UTC(), job.part, seq)
	if err != nil {
		return fmt.Errorf("flusher: build key: %w", err)
	}

	if err := f.store.Put(ctx, key, buf.Bytes()); err != nil {
		return fmt.Errorf("flusher: put: %w", err)
	}

	f.logger.Info("flushed batch",
		zap.String("key", key),
		zap.String("tenant_id", job.batch.TenantID),
		zap.Int("events", job.batch.Len()),
		zap.Int64("offset", job.offset),
	)

	// Write block metadata to the index (best-effort, never fails the flush).
	f.writeBlockIndex(ctx, job, key, buf.Len())

	return nil
}

// writeBlockIndex computes BlockMeta from the flushed batch and updates the index.
// Errors are logged but never propagated — the batch is already safely stored.
func (f *Flusher) writeBlockIndex(ctx context.Context, job *flushJob, key string, sizeBytes int) {
	if f.indexStore == nil {
		return
	}

	batch := job.batch
	now := time.Now().UTC()
	date := now.Format("2006-01-02")

	// Compute min/max time and label union from events.
	var minTime, maxTime time.Time
	labels := make(map[string]string)
	for i, ev := range batch.Events {
		if i == 0 || ev.Timestamp.Before(minTime) {
			minTime = ev.Timestamp
		}
		if i == 0 || ev.Timestamp.After(maxTime) {
			maxTime = ev.Timestamp
		}
		for k, v := range ev.Labels {
			labels[k] = v
		}
	}

	meta := index.BlockMeta{
		FilePath:   key,
		TenantID:   batch.TenantID,
		Partition:  job.part,
		MinTime:    minTime,
		MaxTime:    maxTime,
		EventCount: batch.Len(),
		Labels:     labels,
		SizeBytes:  sizeBytes,
	}

	// Read existing index (or create new one).
	idx, err := f.indexStore.ReadIndex(ctx, batch.TenantID, date)
	if err != nil {
		// Index doesn't exist yet or is corrupt — create a new one.
		idx = index.NewIndex(batch.TenantID, date)
	}

	idx.AddBlock(meta)

	if err := f.indexStore.WriteIndex(ctx, idx); err != nil {
		f.logger.Warn("failed to write block index",
			zap.String("tenant_id", batch.TenantID),
			zap.String("date", date),
			zap.Error(err),
		)
		return
	}

	f.logger.Info("wrote block index",
		zap.String("tenant_id", batch.TenantID),
		zap.String("date", date),
		zap.Int("total_blocks", len(idx.Blocks)),
	)
}

// FlushWithRetry retries Flush with exponential backoff (100ms base, 2x multiplier).
// Returns the last error if all retries are exhausted.
func (f *Flusher) FlushWithRetry(ctx context.Context, job *flushJob, maxRetries int) error {
	start := time.Now()
	var lastErr error
	backoff := 100 * time.Millisecond

	for attempt := 0; attempt <= maxRetries; attempt++ {
		lastErr = f.Flush(ctx, job)
		if lastErr == nil {
			elapsed := time.Since(start)
			observability.BatchFlushTotal.WithLabelValues(job.batch.TenantID, "success").Inc()
			observability.BatchFlushDuration.WithLabelValues(job.batch.TenantID).Observe(elapsed.Seconds())
			observability.BatchSizeEvents.Observe(float64(job.batch.Len()))
			return nil
		}

		f.logger.Warn("flush failed, retrying",
			zap.Error(lastErr),
			zap.Int("attempt", attempt+1),
			zap.Int("max_retries", maxRetries),
			zap.Duration("backoff", backoff),
		)

		if attempt < maxRetries {
			select {
			case <-ctx.Done():
				return fmt.Errorf("flusher: context cancelled during retry: %w", ctx.Err())
			case <-time.After(backoff):
			}
			backoff *= 2
		}
	}

	observability.BatchFlushTotal.WithLabelValues(job.batch.TenantID, "error").Inc()
	observability.BatchFlushDuration.WithLabelValues(job.batch.TenantID).Observe(time.Since(start).Seconds())
	return fmt.Errorf("flusher: exhausted %d retries: %w", maxRetries, lastErr)
}
