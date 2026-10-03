package tenant

import (
	"context"
	"errors"
	"net/http"
	"testing"
)

func TestExtractTenantID_Present(t *testing.T) {
	req, _ := http.NewRequest("POST", "/api/v1/push", nil)
	req.Header.Set("X-Scope-OrgID", "tenant-a")

	id, err := ExtractTenantID(req)
	if err != nil {
		t.Fatalf("ExtractTenantID() error = %v", err)
	}
	if id != "tenant-a" {
		t.Errorf("ExtractTenantID() = %q, want %q", id, "tenant-a")
	}
}

func TestExtractTenantID_Missing(t *testing.T) {
	req, _ := http.NewRequest("POST", "/api/v1/push", nil)

	_, err := ExtractTenantID(req)
	if err == nil {
		t.Fatal("ExtractTenantID() expected error, got nil")
	}
	if !errors.Is(err, ErrMissingTenantID) {
		t.Errorf("ExtractTenantID() error = %v, want %v", err, ErrMissingTenantID)
	}
}

func TestExtractTenantID_Empty(t *testing.T) {
	req, _ := http.NewRequest("POST", "/api/v1/push", nil)
	req.Header.Set("X-Scope-OrgID", "")

	_, err := ExtractTenantID(req)
	if err == nil {
		t.Fatal("ExtractTenantID() expected error, got nil")
	}
	if !errors.Is(err, ErrMissingTenantID) {
		t.Errorf("ExtractTenantID() error = %v, want %v", err, ErrMissingTenantID)
	}
}

func TestContextRoundTrip(t *testing.T) {
	ctx := context.Background()
	ctx = ContextWithTenantID(ctx, "tenant-x")

	id, ok := TenantIDFromContext(ctx)
	if !ok {
		t.Fatal("TenantIDFromContext() ok = false, want true")
	}
	if id != "tenant-x" {
		t.Errorf("TenantIDFromContext() = %q, want %q", id, "tenant-x")
	}
}

func TestTenantIDFromContext_NotSet(t *testing.T) {
	ctx := context.Background()

	id, ok := TenantIDFromContext(ctx)
	if ok {
		t.Error("TenantIDFromContext() ok = true, want false")
	}
	if id != "" {
		t.Errorf("TenantIDFromContext() = %q, want %q", id, "")
	}
}
