package config

import (
	"testing"
	"time"
)

func TestDefaultsApplied(t *testing.T) {
	c, err := Parse([]byte(`{"scrape_targets":[{"job":"a","url":"http://x:1/metrics"}]}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.Listen != ":8080" {
		t.Errorf("listen default: got %q", c.Listen)
	}
	if got := c.ScrapeTargets[0].Interval.Std(); got != 15*time.Second {
		t.Errorf("interval default: got %v", got)
	}
	if got := c.ScrapeTargets[0].Timeout.Std(); got != 10*time.Second {
		t.Errorf("timeout default: got %v", got)
	}
}

func TestInvalidConfigRejected(t *testing.T) {
	cases := map[string]string{
		"missing job": `{"scrape_targets":[{"url":"http://x:1"}]}`,
		"bad scheme": `{"scrape_targets":[{"job":"a","url":"ftp://x"}]}`,
		"unknown field": `{"bogus":1}`,
		"timeout too big": `{"scrape_targets":[{"job":"a","url":"http://x:1","interval":"5s","timeout":"10s"}]}`,
		"bad duration": `{"retention":"forever"}`,
	}
	for name, in := range cases {
		if _, err := Parse([]byte(in)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
