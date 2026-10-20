package alert

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"
)

// Notifier delivers alert events somewhere.
type Notifier interface {
	Notify(ctx context.Context, events []Event) error
}

// MultiNotifier fans events out to several notifiers. Errors are joined.
type MultiNotifier []Notifier

// Notify implements Notifier.
func (m MultiNotifier) Notify(ctx context.Context, events []Event) error {
	var errs []error
	for _, n := range m {
		if err := n.Notify(ctx, events); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// LogNotifier writes events to a structured logger.
type LogNotifier struct {
	Logger *slog.Logger
}

// Notify implements Notifier.
func (n LogNotifier) Notify(_ context.Context, events []Event) error {
	for _, ev := range events {
		n.Logger.Info("alert",
			"status", ev.Status,
			"rule", ev.Alert.Rule,
			"severity", ev.Alert.Severity,
			"series", ev.Alert.Series,
			"value", ev.Alert.Value,
		)
	}
	return nil
}

// WebhookNotifier POSTs events as JSON to a URL.
type WebhookNotifier struct {
	URL    string
	Client *http.Client
}

// NewWebhookNotifier returns a notifier with a 5s request timeout.
func NewWebhookNotifier(url string) *WebhookNotifier {
	return &WebhookNotifier{URL: url, Client: &http.Client{Timeout: 5 * time.Second}}
}

// Notify implements Notifier. Any non-2xx response is an error.
func (w *WebhookNotifier) Notify(ctx context.Context, events []Event) error {
	body, err := json.Marshal(map[string]any{"events": events})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.URL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := w.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))

	if resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned %s", resp.Status)
	}
	return nil
}