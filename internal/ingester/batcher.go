package ingester

import (
	"context"
	"strconv"
	"sync"
	"time"

	"github.com/itsHabib/ingest/internal/event"
)

// flushJob carries a batch plus Kafka metadata needed for offset commit after flush.
type flushJob struct {
	batch  *event.Batch
	offset int64  // last Kafka offset in this batch
	topic  string // Kafka topic for offset commit
	part   int32  // Kafka partition for offset commit
}

// Batcher accumulates events keyed by tenantID:partition and flushes when
// a batch reaches maxSize events or maxAge duration.
type Batcher struct {
	maxSize int
	maxAge  time.Duration

	mu      sync.Mutex
	pending map[string]*flushJob // key: "tenantID:partition"
	flushCh chan *flushJob
}

// NewBatcher creates a Batcher with the given size and age thresholds.
// The flush channel is buffered at 100 to provide backpressure.
func NewBatcher(maxSize int, maxAge time.Duration) *Batcher {
	return &Batcher{
		maxSize: maxSize,
		maxAge:  maxAge,
		pending: make(map[string]*flushJob),
		flushCh: make(chan *flushJob, 100),
	}
}

// FlushChan returns the read-only channel that flush workers consume from.
func (b *Batcher) FlushChan() <-chan *flushJob {
	return b.flushCh
}

// Add routes an event into the appropriate batch. If the batch reaches maxSize,
// it is sent to flushCh. The lock is released before the channel send to avoid
// holding it during I/O.
func (b *Batcher) Add(ev event.Event, partition int32, offset int64, topic string) {
	key := ev.TenantID + ":" + strconv.FormatInt(int64(partition), 10)

	b.mu.Lock()
	job, ok := b.pending[key]
	if !ok {
		batch := event.NewBatch(ev.TenantID, partition)
		job = &flushJob{
			batch: &batch,
			topic: topic,
			part:  partition,
		}
		b.pending[key] = job
	}
	job.batch.Add(ev)
	job.offset = offset // track the latest offset

	var toFlush *flushJob
	if job.batch.Len() >= b.maxSize {
		toFlush = job
		delete(b.pending, key)
	}
	b.mu.Unlock()

	// Send outside the lock — may block if flushCh is full (backpressure).
	if toFlush != nil {
		b.flushCh <- toFlush
	}
}

// Start runs a background goroutine that checks for age-expired batches every
// second. It returns when ctx is cancelled.
func (b *Batcher) Start(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.flushExpired()
		}
	}
}

// flushExpired moves all batches older than maxAge to the flush channel.
func (b *Batcher) flushExpired() {
	now := time.Now()
	var expired []*flushJob

	b.mu.Lock()
	for key, job := range b.pending {
		if now.Sub(job.batch.CreatedAt) >= b.maxAge {
			expired = append(expired, job)
			delete(b.pending, key)
		}
	}
	b.mu.Unlock()

	for _, job := range expired {
		b.flushCh <- job
	}
}

// FlushAll moves all pending batches to the flush channel, regardless of size
// or age. After FlushAll returns, the pending map is empty.
func (b *Batcher) FlushAll() {
	var all []*flushJob

	b.mu.Lock()
	for key, job := range b.pending {
		all = append(all, job)
		delete(b.pending, key)
	}
	b.mu.Unlock()

	for _, job := range all {
		b.flushCh <- job
	}
}
