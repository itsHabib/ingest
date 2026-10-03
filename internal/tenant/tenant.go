package tenant

import (
	"context"
	"errors"
	"net/http"
)

// ErrMissingTenantID is returned when the X-Scope-OrgID header is missing or empty.
var ErrMissingTenantID = errors.New("missing required X-Scope-OrgID header")

// ctxKey is a private type used as the context key for tenant ID to avoid collisions.
type ctxKey struct{}

// ExtractTenantID reads the tenant ID from the X-Scope-OrgID HTTP header.
// Returns ErrMissingTenantID if the header is missing or empty.
func ExtractTenantID(r *http.Request) (string, error) {
	id := r.Header.Get("X-Scope-OrgID")
	if id == "" {
		return "", ErrMissingTenantID
	}
	return id, nil
}

// ContextWithTenantID returns a new context with the tenant ID attached.
func ContextWithTenantID(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, ctxKey{}, tenantID)
}

// TenantIDFromContext extracts the tenant ID from the context.
// Returns ("", false) if no tenant ID was set.
func TenantIDFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(ctxKey{}).(string)
	return id, ok
}
