package gateway

import (
	"testing"
)

func TestNewProducer(t *testing.T) {
	tests := []struct {
		name    string
		brokers []string
		topic   string
	}{
		{"nil_brokers", nil, "test-topic"},
		{"empty_brokers", []string{}, "test-topic"},
		{"empty_topic", []string{"localhost:9092"}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewKafkaProducer(tt.brokers, tt.topic)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestKafkaProducerImplementsProducer(t *testing.T) {
	var _ Producer = (*KafkaProducer)(nil)
}
