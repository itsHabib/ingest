package querier

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/itsHabib/ingest/internal/event"
	"github.com/itsHabib/ingest/internal/index"
	"github.com/itsHabib/ingest/internal/storage"
)

// IndexStoreAdapter bridges the handler's IndexStore interface to the real
// index.ObjectStoreIndexStore. It reads the index for a tenant+date, converts
// querier.Matcher to index.LabelMatcher, calls Index.Lookup, and returns
// matching block file paths. If no index exists, it falls back to listing all
// batch files under the tenant/date prefix so data is always queryable.
type IndexStoreAdapter struct {
	Store       *index.ObjectStoreIndexStore
	ObjectStore storage.ObjectStore
}

func (a *IndexStoreAdapter) Lookup(ctx context.Context, tenantID, date string, matchers []Matcher) ([]string, error) {
	idx, err := a.Store.ReadIndex(ctx, tenantID, date)
	if err != nil {
		// No index for this date — fall back to listing all block files.
		return a.listFallback(ctx, tenantID, date)
	}

	labelMatchers := ConvertMatchers(matchers)

	// Use full-day time range since the handler already enumerates by date.
	dayStart, err := time.Parse("2006-01-02", date)
	if err != nil {
		return nil, fmt.Errorf("parse date %q: %w", date, err)
	}
	dayEnd := dayStart.AddDate(0, 0, 1)

	blocks := idx.Lookup(dayStart, dayEnd, labelMatchers)
	paths := make([]string, len(blocks))
	for i, b := range blocks {
		paths[i] = b.FilePath
	}
	return paths, nil
}

// listFallback lists all .json batch files under tenant/date/ when no index exists.
// The block reader will still apply label and time filtering on the events.
func (a *IndexStoreAdapter) listFallback(ctx context.Context, tenantID, date string) ([]string, error) {
	prefix := fmt.Sprintf("%s/%s/", tenantID, date)
	keys, err := a.ObjectStore.List(ctx, prefix)
	if err != nil {
		return nil, nil
	}
	var paths []string
	for _, k := range keys {
		// Skip the index file itself.
		if strings.HasSuffix(k, "/index.json") {
			continue
		}
		paths = append(paths, k)
	}
	return paths, nil
}

// EventReaderAdapter bridges the handler's EventReader interface to the real
// BlockReader. It converts querier.Matcher to index.LabelMatcher for block-level
// filtering and applies time-range filtering on the output.
type EventReaderAdapter struct {
	Reader *BlockReader
}

func (a *EventReaderAdapter) Read(ctx context.Context, keys []string, matchers []Matcher, start, end time.Time) (<-chan event.Event, <-chan error) {
	labelMatchers := ConvertMatchers(matchers)
	eventCh, errCh := a.Reader.ReadBlocks(ctx, keys, labelMatchers)

	// Apply time range filtering (block reader only does label filtering).
	filteredCh := make(chan event.Event, 256)

	go func() {
		defer close(filteredCh)
		for ev := range eventCh {
			if ev.Timestamp.Before(start) || !ev.Timestamp.Before(end) {
				continue
			}
			select {
			case filteredCh <- ev:
			case <-ctx.Done():
				return
			}
		}
	}()

	return filteredCh, errCh
}

// ConvertMatchers converts querier.Matcher (HTTP layer) to index.LabelMatcher
// (index/reader layer) by reconstructing the matcher string and re-parsing
// through the index package for proper regex anchoring and compilation.
func ConvertMatchers(matchers []Matcher) []index.LabelMatcher {
	if len(matchers) == 0 {
		return nil
	}
	result := make([]index.LabelMatcher, 0, len(matchers))
	for _, m := range matchers {
		var s string
		switch m.Type {
		case MatchRegex:
			s = m.Name + "=~" + m.Value
		default:
			s = m.Name + "=" + m.Value
		}
		parsed, err := index.ParseMatchers(s)
		if err == nil {
			result = append(result, parsed...)
		}
	}
	return result
}
