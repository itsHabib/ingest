package event

import (
	"fmt"
	"time"
)

// Event represents a single ingestion event from a tenant.
type Event struct {
	TenantID  string            `json:"tenant_id"`
	Timestamp time.Time         `json:"timestamp"`
	Labels    map[string]string `json:"labels"`
	Value     float64           `json:"value"`
	Payload   []byte            `json:"payload,omitempty"`
}

// Batch groups events by tenant and partition for efficient Kafka production and storage flushing.
type Batch struct {
	TenantID  string    `json:"tenant_id"`
	Partition int32     `json:"partition"`
	Events    []Event   `json:"events"`
	CreatedAt time.Time `json:"created_at"`
}

// NewEvent creates an Event with the given fields.
func NewEvent(tenantID string, ts time.Time, labels map[string]string, value float64) Event {
	return Event{
		TenantID:  tenantID,
		Timestamp: ts,
		Labels:    labels,
		Value:     value,
	}
}

// NewBatch creates an empty Batch for the given tenant and partition.
func NewBatch(tenantID string, partition int32) Batch {
	return Batch{
		TenantID:  tenantID,
		Partition: partition,
		CreatedAt: time.Now(),
	}
}

// Validate checks that the event has a non-empty TenantID and a non-zero Timestamp.
func (e Event) Validate() error {
	if e.TenantID == "" {
		return fmt.Errorf("event: %w", errEmptyTenantID)
	}
	if e.Timestamp.IsZero() {
		return fmt.Errorf("event: %w", errZeroTimestamp)
	}
	return nil
}

var (
	errEmptyTenantID = fmt.Errorf("tenant_id must not be empty")
	errZeroTimestamp = fmt.Errorf("timestamp must not be zero")
)

// Add appends an event to the batch.
func (b *Batch) Add(event Event) {
	b.Events = append(b.Events, event)
}

// Len returns the number of events in the batch.
func (b *Batch) Len() int {
	return len(b.Events)
}
