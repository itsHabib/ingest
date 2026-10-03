package index

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/itsHabib/ingest/internal/storage"
)

// IndexStore reads and writes Index objects.
type IndexStore interface {
	WriteIndex(ctx context.Context, idx *Index) error
	ReadIndex(ctx context.Context, tenantID, date string) (*Index, error)
}

// ObjectStoreIndexStore implements IndexStore using the existing ObjectStore.
// Index files are stored at <tenantID>/<date>/index.json.
type ObjectStoreIndexStore struct {
	store storage.ObjectStore
}

// NewObjectStoreIndexStore creates an IndexStore backed by the given ObjectStore.
func NewObjectStoreIndexStore(store storage.ObjectStore) *ObjectStoreIndexStore {
	return &ObjectStoreIndexStore{store: store}
}

// indexKey returns the storage key for an index file.
func indexKey(tenantID, date string) string {
	return fmt.Sprintf("%s/%s/index.json", tenantID, date)
}

// WriteIndex serializes the index to JSON and writes it to the object store.
func (s *ObjectStoreIndexStore) WriteIndex(ctx context.Context, idx *Index) error {
	data, err := json.Marshal(idx)
	if err != nil {
		return fmt.Errorf("index store: marshal: %w", err)
	}
	key := indexKey(idx.TenantID, idx.Date)
	if err := s.store.Put(ctx, key, data); err != nil {
		return fmt.Errorf("index store: put %s: %w", key, err)
	}
	return nil
}

// ReadIndex reads and deserializes an index from the object store.
// Returns an error if the key does not exist or the JSON is corrupt.
func (s *ObjectStoreIndexStore) ReadIndex(ctx context.Context, tenantID, date string) (*Index, error) {
	key := indexKey(tenantID, date)
	data, err := s.store.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("index store: get %s: %w", key, err)
	}
	var idx Index
	if err := json.Unmarshal(data, &idx); err != nil {
		return nil, fmt.Errorf("index store: unmarshal %s: %w", key, err)
	}
	return &idx, nil
}
