// Package kafka implements the Kafka producer for the Alertmanager bridge.
//
// Uses franz-go (no CGO) so we get a small static binary in the container
// image. Supports SASL/PLAIN (Confluent Cloud) and SASL/SCRAM-SHA-512.
// Plaintext is allowed only when KAFKA_AUTH_REQUIRED=false.
package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Config holds producer wiring.
type Config struct {
	Brokers       []string // e.g. ["broker.cloud:9092", ...]
	Topic         string   // e.g. "garuda.anomalies.tier1"
	SASLUsername  string   // empty = plaintext (only allowed when AuthRequired=false)
	SASLPassword  string
	SASLMechanism string   // "PLAIN" (Confluent Cloud) or "SCRAM-SHA-512"; defaults to PLAIN
	AuthRequired  bool     // when true, refuse to start without SASL credentials
	UseTLS        bool     // enable TLS (required for Confluent Cloud)
	ClientID      string   // optional; defaults to "garuda-am-bridge"
	CompressionOK bool     // attempt zstd; fall back to none on broker rejection
}

// Producer wraps a franz-go client with our delivery semantics:
// acks=all, idempotent, modest batching for low-volume anomaly traffic.
type Producer struct {
	client *kgo.Client
	topic  string
}

// New constructs a producer.
func New(cfg Config) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka: brokers required")
	}
	if cfg.Topic == "" {
		return nil, errors.New("kafka: topic required")
	}
	if cfg.AuthRequired && cfg.SASLUsername == "" {
		return nil, errors.New("kafka: SASL credentials required (KAFKA_AUTH_REQUIRED=true)")
	}
	if cfg.ClientID == "" {
		cfg.ClientID = "garuda-am-bridge"
	}
	if cfg.SASLMechanism == "" {
		cfg.SASLMechanism = "PLAIN" // default for Confluent Cloud
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.ClientID(cfg.ClientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerBatchCompression(kgo.ZstdCompression(), kgo.NoCompression()),
		kgo.MaxBufferedRecords(1000),
		// We key every record by AnomalySignal.Fingerprint, so a sticky-key
		// partitioner ensures all re-emissions of the same anomaly land on
		// the same partition (preserves ordering for the correlator).
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	}

	// Enable TLS (required for Confluent Cloud and most managed Kafka)
	if cfg.UseTLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{}))
	}

	// Configure SASL authentication
	if cfg.SASLUsername != "" {
		switch cfg.SASLMechanism {
		case "PLAIN":
			mech := plain.Auth{
				User: cfg.SASLUsername,
				Pass: cfg.SASLPassword,
			}.AsMechanism()
			opts = append(opts, kgo.SASL(mech))
		case "SCRAM-SHA-512":
			mech := scram.Auth{
				User: cfg.SASLUsername,
				Pass: cfg.SASLPassword,
			}.AsSha512Mechanism()
			opts = append(opts, kgo.SASL(mech))
		default:
			return nil, fmt.Errorf("kafka: unsupported SASL mechanism: %s", cfg.SASLMechanism)
		}
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new client: %w", err)
	}
	return &Producer{client: client, topic: cfg.Topic}, nil
}

// Publish sends a record synchronously and returns once the broker has acked
// to all in-sync replicas. Returns the produce error (if any).
func (p *Producer) Publish(ctx context.Context, key string, value []byte) error {
	r := &kgo.Record{
		Topic: p.topic,
		Key:   []byte(key),
		Value: value,
	}
	res := p.client.ProduceSync(ctx, r)
	if err := res.FirstErr(); err != nil {
		return err
	}
	return nil
}

// Close flushes outstanding records and tears down the client.
func (p *Producer) Close(ctx context.Context) error {
	return p.client.Flush(ctx)
}
