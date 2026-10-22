package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/OluwaTobba/monitoring-and-observation-platform/internal/alert"
	"github.com/OluwaTobba/monitoring-and-observation-platform/internal/api"
	"github.com/OluwaTobba/monitoring-and-observation-platform/internal/config"
	"github.com/OluwaTobba/monitoring-and-observation-platform/internal/scrape"
	"github.com/OluwaTobba/monitoring-and-observation-platform/internal/tsdb"
)

func main() {
	cfgPath := flag.String("config", "config.json", "path to the JSON configuration file")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		logger.Error("invalid configuration", "path", *cfgPath, "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store := tsdb.NewStore(cfg.Retention.Std(), cfg.MaxSamplesPerSeries)

	targets := make([]scrape.Target, 0, len(cfg.ScrapeTargets))
	for _, t := range cfg.ScrapeTargets {
		targets = append(targets, scrape.Target{
			Job: t.Job,
			URL: t.URL,
			Interval: t.Interval.Std(),
			Timeout: t.Timeout.Std(),
			Labels: t.Labels,
		})
	}
	scraper := scrape.NewScraper(store, targets, logger)

	rules := make([]alert.Rule, 0, len(cfg.AlertRules))
	for _, r := range cfg.AlertRules {
		rules = append(rules, alert.Rule{
			Name: r.Name,
			Metric: r.Metric,
			Labels: r.Labels,
			Op: alert.Op(r.Op),
			Threshold: r.Threshold,
			For: r.For.Std(),
			Severity: r.Severity,
		})
	}

	notifier := alert.MultiNotifier{alert.LogNotifier{Logger: logger}}
	if cfg.WebhookURL != "" {
		notifier = append(notifier, alert.NewWebhookNotifier(cfg.WebhookURL))
	}
	engine, err := alert.NewEngine(store, rules, notifier, cfg.AlertEvalInterval.Std(), logger)
	if err != nil {
		logger.Error("invalid alert rules", "err", err)
		os.Exit(1)
	}

	apiServer := &api.Server{Store: store, Scraper: scraper, Alerts: engine}
	srv := &http.Server{
		Addr: cfg.Listen,
		Handler: apiServer.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go scraper.Run(ctx)
	go engine.Run(ctx)
	go pruneLoop(ctx, store, logger)

	go func() {
		logger.Info("obsd listening", "addr", cfg.Listen, "targets", len(targets), "rules", len(rules))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("graceful shutdown failed", "err", err)
	}
}

// pruneLoop enforces retention once a minute.
func pruneLoop(ctx context.Context, store *tsdb.Store, logger *slog.Logger) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if n := store.Prune(now); n > 0 {
				logger.Debug("pruned samples", "count", n)
			}
		}
	}
}