package querier

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/itsHabib/ingest/internal/event"
	"github.com/itsHabib/ingest/internal/index"
	"github.com/itsHabib/ingest/internal/storage"
)

const defaultWorkers = 8

// BlockReader reads blocks from object storage in parallel, deserializes them,
// applies label matchers to filter events, and sends matching events to an output channel.
type BlockReader struct {
	store   storage.ObjectStore
	workers int
}

// NewBlockReader creates a BlockReader with the given concurrency.
// If workers <= 0, defaults to 8.
func NewBlockReader(store storage.ObjectStore, workers int) *BlockReader {
	if workers <= 0 {
		workers = defaultWorkers
	}
	return &BlockReader{
		store:   store,
		workers: workers,
	}
}

// ReadBlocks reads blocks in parallel, filters by matchers, and returns event and error channels.
// The event channel is closed when all blocks are processed.
// The error channel receives non-fatal per-block errors and is closed when done.
// If paths is empty, both channels close immediately.
// If matchers is nil or empty, all events pass through.
func (r *BlockReader) ReadBlocks(ctx context.Context, paths []string, matchers []index.LabelMatcher) (<-chan event.Event, <-chan error) {
	eventCh := make(chan event.Event, 256)
	errCh := make(chan error, len(paths))

	if len(paths) == 0 {
		close(eventCh)
		close(errCh)
		return eventCh, errCh
	}

	pathCh := make(chan string, len(paths))
	for _, p := range paths {
		pathCh <- p
	}
	close(pathCh)

	var wg sync.WaitGroup
	wg.Add(r.workers)

	for i := 0; i < r.workers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case path, ok := <-pathCh:
					if !ok {
						return
					}
					r.processBlock(ctx, path, matchers, eventCh, errCh)
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(eventCh)
		close(errCh)
	}()

	return eventCh, errCh
}

func (r *BlockReader) processBlock(ctx context.Context, path string, matchers []index.LabelMatcher, eventCh chan<- event.Event, errCh chan<- error) {
	data, err := r.store.Get(ctx, path)
	if err != nil {
		select {
		case errCh <- fmt.Errorf("read block %q: %w", path, err):
		case <-ctx.Done():
		}
		return
	}

	var batch event.Batch
	if err := json.Unmarshal(data, &batch); err != nil {
		select {
		case errCh <- fmt.Errorf("unmarshal block %q: %w", path, err):
		case <-ctx.Done():
		}
		return
	}

	for _, ev := range batch.Events {
		if len(matchers) > 0 && !index.MatchAll(matchers, ev.Labels) {
			continue
		}
		select {
		case eventCh <- ev:
		case <-ctx.Done():
			return
		}
	}
}
