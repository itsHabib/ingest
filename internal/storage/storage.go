package storage

import (
	"context"
	"fmt"
	"time"
)

// ObjectStore defines a CRUD interface for blob storage.
// Implementations must be safe for concurrent use.
type ObjectStore interface {
	// Put writes data to the given key. Overwrites if the key already exists.
	Put(ctx context.Context, key string, data []byte) error

	// Get reads data from the given key. Returns an error if the key does not exist.
	Get(ctx context.Context, key string) ([]byte, error)

	// List returns all keys matching the given prefix.
	// Returns (nil, nil) if no keys match.
	List(ctx context.Context, prefix string) ([]string, error)

	// Delete removes the key. Returns nil if the key does not exist (idempotent).
	Delete(ctx context.Context, key string) error

	// Exists returns true if the key exists.
	Exists(ctx context.Context, key string) (bool, error)
}

// BatchKey produces the canonical storage key for a flushed batch.
// Format: <tenantID>/<YYYY-MM-DD>/<partition>-<offset padded to 10 digits>.json
func BatchKey(tenantID string, date time.Time, partition int32, offset int64) (string, error) {
	if tenantID == "" {
		return "", fmt.Errorf("storage: BatchKey: tenant ID must not be empty")
	}
	return fmt.Sprintf("%s/%s/%d-%010d.json",
		tenantID,
		date.UTC().Format("2006-01-02"),
		partition,
		offset,
	), nil
}
