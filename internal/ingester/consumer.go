package ingester

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/itsHabib/ingest/internal/config"
	"github.com/itsHabib/ingest/internal/event"
	"github.com/itsHabib/ingest/internal/observability"
	"github.com/itsHabib/ingest/internal/storage"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
)

// Consumer reads events from Kafka, routes them through the batcher, and
// coordinates flush workers that write to storage and commit offsets.
type Consumer struct {
	client         *kgo.Client
	batcher        *Batcher
	flusher        *Flusher
	store          storage.ObjectStore // used for backpressure health probe
	logger         *zap.Logger
	topic          string
	flushWorkers   int
	paused         atomic.Bool  // guards idempotent pause/resume
	consecFailures atomic.Int64 // consecutive flush failures across all workers
	pauseThreshold int64        // failures before triggering backpressure
}

// NewConsumer creates a franz-go consumer group client with auto-commit disabled.
// The store parameter is used for backpressure health probing; pass nil to disable.
func NewConsumer(cfg config.IngesterConfig, batcher *Batcher, flusher *Flusher, store storage.ObjectStore, logger *zap.Logger) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaBrokers...),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.ConsumeTopics(cfg.KafkaTopic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("consumer: create client: %w", err)
	}

	workers := 4
	if cfg.FlushWorkers > 0 {
		workers = cfg.FlushWorkers
	}

	threshold := int64(5)
	if cfg.PauseThreshold > 0 {
		threshold = int64(cfg.PauseThreshold)
	}

	return &Consumer{
		client:         client,
		batcher:        batcher,
		flusher:        flusher,
		store:          store,
		logger:         logger,
		topic:          cfg.KafkaTopic,
		flushWorkers:   workers,
		pauseThreshold: threshold,
	}, nil
}

// Run starts the batcher, flush workers, and the Kafka poll loop.
// It blocks until ctx is cancelled, then performs graceful shutdown:
// stop consuming -> FlushAll -> drain workers -> return.
func (c *Consumer) Run(ctx context.Context) error {
	// Start batcher background goroutine (age-triggered flushes).
	batcherCtx, batcherCancel := context.WithCancel(ctx)
	defer batcherCancel()

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		c.batcher.Start(batcherCtx)
	}()

	// Start flush workers.
	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()

	var workerWg sync.WaitGroup
	for i := 0; i < c.flushWorkers; i++ {
		workerWg.Add(1)
		go func(id int) {
			defer workerWg.Done()
			c.flushWorker(workerCtx, id)
		}(i)
	}

	// Poll loop.
	c.logger.Info("consumer started", zap.String("topic", c.topic), zap.Int("flush_workers", c.flushWorkers))

	for {
		fetches := c.client.PollFetches(ctx)

		if ctx.Err() != nil {
			break
		}

		errs := fetches.Errors()
		for _, e := range errs {
			c.logger.Error("fetch error",
				zap.String("topic", e.Topic),
				zap.Int32("partition", e.Partition),
				zap.Error(e.Err),
			)
		}

		fetches.EachRecord(func(rec *kgo.Record) {
			var events []event.Event
			if err := json.Unmarshal(rec.Value, &events); err != nil {
				c.logger.Warn("consumer: skipping malformed record",
					zap.Error(err),
					zap.Int64("offset", rec.Offset),
					zap.Int32("partition", rec.Partition),
				)
				return
			}

			partStr := strconv.FormatInt(int64(rec.Partition), 10)
			observability.EventsConsumedTotal.WithLabelValues(partStr).Add(float64(len(events)))

			for _, ev := range events {
				c.batcher.Add(ev, rec.Partition, rec.Offset, rec.Topic)
			}
		})
	}

	// Graceful shutdown: stop batcher ticker, flush all remaining batches.
	c.logger.Info("shutting down consumer")
	batcherCancel()
	wg.Wait()

	c.batcher.FlushAll()
	close(c.batcher.flushCh)

	// Wait for flush workers to drain.
	workerWg.Wait()
	workerCancel()

	c.client.Close()
	c.logger.Info("consumer stopped")
	return nil
}

// flushWorker reads from the flush channel, flushes to storage, and commits
// the Kafka offset on success. On flush failure after retries, it triggers
// backpressure by pausing Kafka consumption and starting a storage probe.
func (c *Consumer) flushWorker(ctx context.Context, id int) {
	for job := range c.batcher.flushCh {
		if err := c.flusher.FlushWithRetry(ctx, job, 3); err != nil {
			n := c.consecFailures.Add(1)
			c.logger.Error("flush failed after retries",
				zap.Error(err),
				zap.Int("worker", id),
				zap.String("tenant_id", job.batch.TenantID),
				zap.Int("events", job.batch.Len()),
				zap.Int64("consecutive_failures", n),
			)

			// Trigger backpressure only after reaching the threshold.
			if n >= c.pauseThreshold {
				if c.paused.CompareAndSwap(false, true) {
					c.logger.Warn("backpressure: pausing Kafka consumption due to storage failure",
						zap.String("topic", c.topic),
						zap.Int64("consecutive_failures", n),
					)
					c.client.PauseFetchTopics(c.topic)

					// Spawn a background probe to detect storage recovery.
					go c.storageProbe(ctx)
				}
			}

			// Re-enqueue the failed job for retry (at-least-once: never drop events).
			// Wait briefly to avoid a tight retry loop while storage is broken.
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}

			c.batcher.flushCh <- job

			continue
		}

		// Successful flush resets the consecutive failure counter.
		c.consecFailures.Store(0)

		// Commit offset ONLY after successful flush (at-least-once).
		c.client.CommitOffsets(ctx, map[string]map[int32]kgo.EpochOffset{
			job.topic: {
				job.part: kgo.EpochOffset{
					Offset: job.offset + 1, // commit the NEXT offset to consume
				},
			},
		}, nil)

		c.logger.Debug("committed offset",
			zap.String("topic", job.topic),
			zap.Int32("partition", job.part),
			zap.Int64("offset", job.offset+1),
		)
	}
}

// storageProbe tests storage health every 5 seconds by writing and deleting
// a sentinel key. When storage recovers, it resumes Kafka consumption.
func (c *Consumer) storageProbe(ctx context.Context) {
	const probeKey = "__probe/health"
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if c.store == nil {
				c.resumeConsumption()
				return
			}

			probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			err := c.store.Put(probeCtx, probeKey, []byte("ok"))
			if err == nil {
				_ = c.store.Delete(probeCtx, probeKey)
			}
			cancel()

			if err == nil {
				c.resumeConsumption()
				return
			}

			c.logger.Warn("backpressure: storage probe failed, still paused",
				zap.Error(err),
			)
		}
	}
}

// resumeConsumption resumes Kafka fetch for the topic and clears the paused flag.
func (c *Consumer) resumeConsumption() {
	c.client.ResumeFetchTopics(c.topic)
	c.consecFailures.Store(0)
	c.paused.Store(false)
	c.logger.Info("backpressure: resumed Kafka consumption, storage recovered",
		zap.String("topic", c.topic),
	)
}
