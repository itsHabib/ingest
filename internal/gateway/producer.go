package gateway

import (
	"context"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer defines the methods the handler needs from a Kafka producer.
type Producer interface {
	Produce(ctx context.Context, key []byte, value []byte) error
	ProduceAsync(ctx context.Context, key []byte, value []byte, callback func(error))
	ProduceToPartition(ctx context.Context, key []byte, value []byte, partition int32) error
	Close() error
}

// KafkaProducer wraps a franz-go client for producing records to Kafka.
type KafkaProducer struct {
	client *kgo.Client
	topic  string
}

// NewKafkaProducer creates a new Kafka producer connected to the given brokers.
// Records are produced to the specified topic with acks=all for durability.
func NewKafkaProducer(brokers []string, topic string) (*KafkaProducer, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("producer: brokers must not be empty")
	}
	if topic == "" {
		return nil, fmt.Errorf("producer: topic must not be empty")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.DefaultProduceTopic(topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		return nil, fmt.Errorf("producer: create client: %w", err)
	}

	return &KafkaProducer{
		client: client,
		topic:  topic,
	}, nil
}

// Produce synchronously produces a record to Kafka, blocking until acknowledged or error.
// The tenant ID should be set as the key for partition affinity.
func (p *KafkaProducer) Produce(ctx context.Context, key []byte, value []byte) error {
	rec := &kgo.Record{
		Key:   key,
		Value: value,
	}

	// Add tenant_id header for consumer-side filtering
	if len(key) > 0 {
		rec.Headers = []kgo.RecordHeader{
			{Key: "tenant_id", Value: key},
		}
	}

	var wg sync.WaitGroup
	var produceErr error

	wg.Add(1)
	p.client.Produce(ctx, rec, func(r *kgo.Record, err error) {
		_ = r
		produceErr = err
		wg.Done()
	})
	wg.Wait()

	if produceErr != nil {
		return fmt.Errorf("producer: produce: %w", produceErr)
	}
	return nil
}

// ProduceAsync produces a record to Kafka asynchronously. The callback is invoked
// when the record is acknowledged or fails.
func (p *KafkaProducer) ProduceAsync(ctx context.Context, key []byte, value []byte, callback func(error)) {
	rec := &kgo.Record{
		Key:   key,
		Value: value,
	}

	if len(key) > 0 {
		rec.Headers = []kgo.RecordHeader{
			{Key: "tenant_id", Value: key},
		}
	}

	p.client.Produce(ctx, rec, func(r *kgo.Record, err error) {
		_ = r
		if callback != nil {
			callback(err)
		}
	})
}

// ProduceToPartition produces a record to a specific Kafka partition.
// Setting kgo.Record.Partition to a non-negative value causes franz-go to
// bypass its default partitioner and send directly to that partition.
func (p *KafkaProducer) ProduceToPartition(ctx context.Context, key []byte, value []byte, partition int32) error {
	rec := &kgo.Record{
		Key:       key,
		Value:     value,
		Partition: partition,
	}

	if len(key) > 0 {
		rec.Headers = []kgo.RecordHeader{
			{Key: "tenant_id", Value: key},
		}
	}

	var wg sync.WaitGroup
	var produceErr error

	wg.Add(1)
	p.client.Produce(ctx, rec, func(r *kgo.Record, err error) {
		_ = r
		produceErr = err
		wg.Done()
	})
	wg.Wait()

	if produceErr != nil {
		return fmt.Errorf("producer: produce to partition %d: %w", partition, produceErr)
	}
	return nil
}

// Close flushes any pending records and closes the underlying client.
func (p *KafkaProducer) Close() error {
	if err := p.client.Flush(context.Background()); err != nil {
		return fmt.Errorf("producer: flush: %w", err)
	}
	p.client.Close()
	return nil
}
