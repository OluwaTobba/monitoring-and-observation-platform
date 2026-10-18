// Package config loads and validates the JSON configuration for obsd.
package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"time"
)

// Duration is a time.Duration that is read from a JSON string such as "15s".
type Duration time.Duration

// UnmarshalJSON implements json.Unmarshaler.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string such as \"15s\": %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// Std converts to time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// Target is a scrape target as written in the config file.
type Target struct {
	Job      string            `json:"job"`
	URL      string            `json:"url"`
	Interval Duration          `json:"interval"`
	Timeout  Duration          `json:"timeout"`
	Labels   map[string]string `json:"labels"`
}

// Rule is an alert rule as written in the config file.
type Rule struct {
	Name      string            `json:"name"`
	Metric    string            `json:"metric"`
	Labels    map[string]string `json:"labels"`
	Op        string            `json:"op"`
	Threshold float64           `json:"threshold"`
	For       Duration          `json:"for"`
	Severity  string            `json:"severity"`
}

// Config is the top-level obsd configuration.
type Config struct {
	Listen              string   `json:"listen"`
	Retention           Duration `json:"retention"`
	MaxSamplesPerSeries int      `json:"max_samples_per_series"`
	AlertEvalInterval   Duration `json:"alert_eval_interval"`
	WebhookURL          string   `json:"webhook_url"`
	ScrapeTargets       []Target `json:"scrape_targets"`
	AlertRules          []Rule   `json:"alert_rules"`
}

// Load reads and validates a configuration file.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(raw)
}

// Parse decodes and validates configuration bytes. Unknown fields are rejected so that typos fail loudly instead of being silently ignored.
func Parse(raw []byte) (*Config, error) {
	var c Config
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	c.applyDefaults()
	if err := c.validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = ":8080"
	}
	if c.Retention == 0 {
		c.Retention = Duration(time.Hour)
	}
	if c.MaxSamplesPerSeries == 0 {
		c.MaxSamplesPerSeries = 10000
	}
	if c.AlertEvalInterval == 0 {
		c.AlertEvalInterval = Duration(15 * time.Second)
	}
	for i := range c.ScrapeTargets {
		t := &c.ScrapeTargets[i]
		if t.Interval == 0 {
			t.Interval = Duration(15 * time.Second)
		}
		if t.Timeout == 0 {
			t.Timeout = Duration(10 * time.Second)
		}
	}
	for i := range c.AlertRules {
		if c.AlertRules[i].Severity == "" {
			c.AlertRules[i].Severity = "warning"
		}
	}
}

func (c *Config) validate() error {
	var errs []error
	if c.Retention < 0 {
		errs = append(errs, errors.New("retention must not be negative"))
	}
	if c.MaxSamplesPerSeries < 1 {
		errs = append(errs, errors.New("max_samples_per_series must be at least 1"))
	}
	if c.AlertEvalInterval <= 0 {
		errs = append(errs, errors.New("alert_eval_interval must be positive"))
	}
	for i, t := range c.ScrapeTargets {
		if t.Job == "" {
			errs = append(errs, fmt.Errorf("scrape_targets[%d]: job is required", i))
		}
		u, err := url.Parse(t.URL)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			errs = append(errs, fmt.Errorf("scrape_targets[%d]: url must be an absolute http(s) URL", i))
		}
		if t.Timeout >= t.Interval {
			errs = append(errs, fmt.Errorf("scrape_targets[%d]: timeout must be shorter than interval", i))
		}
	}
	return errors.Join(errs...)
}