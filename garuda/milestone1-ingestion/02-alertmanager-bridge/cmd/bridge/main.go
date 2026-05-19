// Garuda Alertmanager-to-Kafka bridge.
//
// Receives Alertmanager webhook payloads on /webhook, transforms each
// alert with `garuda_anomaly=true` into the canonical AnomalySignal
// envelope, and produces to garuda.anomalies.tier1 on the chitragupta
// Kafka cluster.
//
// Two listeners:
//   - LISTEN_ADDR (default :8080) — /webhook + /healthz
//   - METRICS_ADDR (default :9090) — /metrics for Prometheus scraping
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/myntra/garuda/alertmanager-bridge/internal/config"
	"github.com/myntra/garuda/alertmanager-bridge/internal/handler"
	"github.com/myntra/garuda/alertmanager-bridge/internal/kafka"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	cfg, err := config.FromEnv()
	if err != nil {
		slog.Error("config load failed", "err", err)
		os.Exit(2)
	}

	prod, err := kafka.New(kafka.Config{
		Brokers:      cfg.KafkaBrokers,
		Topic:        cfg.KafkaTopic,
		SASLUsername: cfg.KafkaUser,
		SASLPassword: cfg.KafkaPass,
		AuthRequired: cfg.KafkaAuthReq,
	})
	if err != nil {
		slog.Error("kafka producer init failed", "err", err)
		os.Exit(2)
	}

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewGoCollector(),
	)

	mk := func(name, help string) prometheus.Counter {
		c := prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "garuda",
			Subsystem: "am_bridge",
			Name:      name,
			Help:      help,
		})
		reg.MustRegister(c)
		return c
	}

	wh := &handler.Webhook{
		Pub:             prod,
		SharedSecret:    cfg.WebhookSecret,
		AlertsTotal:     mk("alerts_received_total", "Alerts received from Alertmanager (any label)."),
		AlertsFiltered:  mk("alerts_filtered_total", "Alerts dropped because garuda_anomaly!=true."),
		AlertsInvalid:   mk("alerts_invalid_total", "Alerts that could not be transformed (missing alertname/cluster/etc.)."),
		AlertsPublished: mk("alerts_published_total", "Anomaly signals published to Kafka."),
		PublishErrors:   mk("publish_errors_total", "Kafka produce errors."),
	}

	mux := http.NewServeMux()
	mux.Handle("/webhook", wh)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	mainSrv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	metricsSrv := &http.Server{
		Addr:              cfg.MetricsAddr,
		Handler:           metricsMux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 2)
	go func() {
		slog.Info("listening", "addr", cfg.ListenAddr)
		if err := mainSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()
	go func() {
		slog.Info("metrics listening", "addr", cfg.MetricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errCh:
		slog.Error("server error", "err", err)
	case sig := <-stop:
		slog.Info("shutdown signal", "sig", sig.String())
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(cfg.ShutdownGraceMs)*time.Millisecond)
	defer cancel()
	_ = mainSrv.Shutdown(ctx)
	_ = metricsSrv.Shutdown(ctx)
	_ = prod.Close(ctx)
	slog.Info("shutdown complete")
}
