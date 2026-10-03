//go:build integration

package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// newTestS3Store creates an S3Store pointing at local MinIO for testing.
// It creates a unique bucket per test to avoid collisions.
func newTestS3Store(t *testing.T) *S3Store {
	t.Helper()

	bucket := fmt.Sprintf("test-%d", time.Now().UnixNano())
	store, err := NewS3Store(
		"http://localhost:9000",
		bucket,
		"minioadmin",
		"minioadmin",
		"us-east-1",
		true,
	)
	if err != nil {
		t.Fatalf("NewS3Store: %v", err)
	}
	return store
}

func TestS3PutGetRoundtrip(t *testing.T) {
	store := newTestS3Store(t)
	ctx := context.Background()

	key := "tenant-a/2026-03-01/0-0000000001.json"
	data := []byte(`{"events":[{"value":42}]}`)

	if err := store.Put(ctx, key, data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := store.Get(ctx, key)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if string(got) != string(data) {
		t.Errorf("Get returned %q, want %q", got, data)
	}
}

func TestS3ListPrefix(t *testing.T) {
	store := newTestS3Store(t)
	ctx := context.Background()

	keys := []string{
		"tenant-a/2026-03-01/0-0000000001.json",
		"tenant-a/2026-03-01/0-0000000002.json",
		"tenant-b/2026-03-01/0-0000000001.json",
	}
	for _, k := range keys {
		if err := store.Put(ctx, k, []byte("data")); err != nil {
			t.Fatalf("Put %q: %v", k, err)
		}
	}

	got, err := store.List(ctx, "tenant-a/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}

	if len(got) != 2 {
		t.Errorf("List(tenant-a/) returned %d keys, want 2: %v", len(got), got)
	}
}

func TestS3Delete(t *testing.T) {
	store := newTestS3Store(t)
	ctx := context.Background()

	key := "tenant-a/2026-03-01/0-0000000001.json"
	if err := store.Put(ctx, key, []byte("data")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := store.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	exists, err := store.Exists(ctx, key)
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Error("Exists returned true after Delete, want false")
	}
}

func TestS3ExistsNonExistent(t *testing.T) {
	store := newTestS3Store(t)
	ctx := context.Background()

	exists, err := store.Exists(ctx, "does-not-exist/key.json")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Error("Exists returned true for non-existent key, want false")
	}
}

func TestS3DeleteIdempotent(t *testing.T) {
	store := newTestS3Store(t)
	ctx := context.Background()

	// Deleting a non-existent key should not error.
	if err := store.Delete(ctx, "does-not-exist/key.json"); err != nil {
		t.Fatalf("Delete non-existent key: %v", err)
	}
}

func TestS3PutContentType(t *testing.T) {
	store := newTestS3Store(t)
	ctx := context.Background()

	key := "tenant-a/2026-03-01/0-0000000001.json"
	if err := store.Put(ctx, key, []byte("data")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Verify content type via HeadObject.
	out, err := store.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(store.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if out.ContentType == nil || *out.ContentType != "application/json" {
		ct := "<nil>"
		if out.ContentType != nil {
			ct = *out.ContentType
		}
		t.Errorf("ContentType = %q, want %q", ct, "application/json")
	}
}
