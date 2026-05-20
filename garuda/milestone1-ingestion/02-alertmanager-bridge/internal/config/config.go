// Package config loads runtime configuration from environment variables.
// Twelve-factor style; the Helm chart maps Values into env vars.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Config is the bridge's runtime configuration.
type Config struct {
	ListenAddr        string
	KafkaBrokers      []string
	KafkaTopic        string
	KafkaUser         string
	KafkaPass         string
	KafkaAuthReq      bool
	KafkaSASLMech     string // "PLAIN" (Confluent Cloud) or "SCRAM-SHA-512"
	KafkaUseTLS       bool   // enable TLS (required for Confluent Cloud)
	WebhookSecret     string // optional shared secret on X-Garuda-Bridge-Secret
	MetricsAddr       string // separate listener so we can scope auth on /webhook
	ShutdownGraceMs   int
}

// FromEnv builds the Config; returns error on missing required vars.
func FromEnv() (*Config, error) {
	c := &Config{
		ListenAddr:      env("LISTEN_ADDR", ":8080"),
		KafkaTopic:      env("KAFKA_TOPIC", "garuda.anomalies.tier1"),
		KafkaUser:       os.Getenv("KAFKA_SASL_USER"),
		KafkaPass:       os.Getenv("KAFKA_SASL_PASS"),
		KafkaSASLMech:   env("KAFKA_SASL_MECHANISM", "PLAIN"), // PLAIN for Confluent Cloud
		WebhookSecret:   os.Getenv("WEBHOOK_SHARED_SECRET"),
		MetricsAddr:     env("METRICS_ADDR", ":9090"),
		ShutdownGraceMs: envInt("SHUTDOWN_GRACE_MS", 10000),
	}

	brokersRaw := os.Getenv("KAFKA_BROKERS")
	if brokersRaw == "" {
		return nil, fmt.Errorf("KAFKA_BROKERS required (comma-separated host:port)")
	}
	for _, b := range strings.Split(brokersRaw, ",") {
		if b = strings.TrimSpace(b); b != "" {
			c.KafkaBrokers = append(c.KafkaBrokers, b)
		}
	}

	authReq, _ := strconv.ParseBool(env("KAFKA_AUTH_REQUIRED", "false"))
	c.KafkaAuthReq = authReq
	if authReq && c.KafkaUser == "" {
		return nil, fmt.Errorf("KAFKA_SASL_USER required when KAFKA_AUTH_REQUIRED=true")
	}

	// Enable TLS by default when auth is required (Confluent Cloud, etc.)
	useTLS, _ := strconv.ParseBool(env("KAFKA_USE_TLS", strconv.FormatBool(authReq)))
	c.KafkaUseTLS = useTLS

	return c, nil
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
