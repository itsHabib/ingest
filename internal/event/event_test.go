package event

import (
	"testing"
	"time"
)

func TestNewEvent(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	labels := map[string]string{"service": "api", "env": "prod"}
	e := NewEvent("tenant-a", ts, labels, 42.0)

	if e.TenantID != "tenant-a" {
		t.Errorf("TenantID = %q, want %q", e.TenantID, "tenant-a")
	}
	if !e.Timestamp.Equal(ts) {
		t.Errorf("Timestamp = %v, want %v", e.Timestamp, ts)
	}
	if e.Value != 42.0 {
		t.Errorf("Value = %v, want %v", e.Value, 42.0)
	}
	if len(e.Labels) != 2 {
		t.Errorf("Labels len = %d, want 2", len(e.Labels))
	}
	if e.Payload != nil {
		t.Errorf("Payload = %v, want nil", e.Payload)
	}
}

func TestEventValidate(t *testing.T) {
	validTS := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)

	tests := []struct {
		name    string
		event   Event
		wantErr bool
	}{
		{
			name:    "valid",
			event:   NewEvent("tenant-a", validTS, map[string]string{"k": "v"}, 1.0),
			wantErr: false,
		},
		{
			name:    "empty_tenant_id",
			event:   NewEvent("", validTS, map[string]string{"k": "v"}, 1.0),
			wantErr: true,
		},
		{
			name:    "zero_timestamp",
			event:   NewEvent("tenant-a", time.Time{}, map[string]string{"k": "v"}, 1.0),
			wantErr: true,
		},
		{
			name:    "no_labels",
			event:   NewEvent("tenant-a", validTS, nil, 1.0),
			wantErr: false,
		},
		{
			name:    "empty_labels",
			event:   NewEvent("tenant-a", validTS, map[string]string{}, 1.0),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.event.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNewBatch(t *testing.T) {
	before := time.Now()
	b := NewBatch("tenant-a", 3)
	after := time.Now()

	if b.TenantID != "tenant-a" {
		t.Errorf("TenantID = %q, want %q", b.TenantID, "tenant-a")
	}
	if b.Partition != 3 {
		t.Errorf("Partition = %d, want 3", b.Partition)
	}
	if b.CreatedAt.Before(before) || b.CreatedAt.After(after) {
		t.Errorf("CreatedAt = %v, want between %v and %v", b.CreatedAt, before, after)
	}
	if b.Len() != 0 {
		t.Errorf("Len() = %d, want 0", b.Len())
	}
}

func TestBatchAdd(t *testing.T) {
	b := NewBatch("tenant-a", 0)
	e := NewEvent("tenant-a", time.Now(), map[string]string{"k": "v"}, 1.0)
	b.Add(e)

	if b.Len() != 1 {
		t.Errorf("Len() = %d, want 1", b.Len())
	}
}

func TestBatchAdd_Multiple(t *testing.T) {
	b := NewBatch("tenant-a", 0)
	ts := time.Now()

	for i := 0; i < 5; i++ {
		e := NewEvent("tenant-a", ts, map[string]string{"i": "v"}, float64(i))
		b.Add(e)
	}

	if b.Len() != 5 {
		t.Errorf("Len() = %d, want 5", b.Len())
	}
}

func BenchmarkEventValidate(b *testing.B) {
	e := NewEvent("tenant-a", time.Now(), map[string]string{"k": "v"}, 1.0)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = e.Validate()
	}
}
