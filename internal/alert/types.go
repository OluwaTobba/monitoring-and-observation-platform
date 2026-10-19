package alert

import (
	"errors"
	"fmt"
	"time"

	"github.com/OluwaTobba/monitoring-and-observation-platform/internal/tsdb"
)

type Op string

const (
	OpGT Op = ">"
	OpGE Op = ">="
	OpLT Op = "<"
	OpLE Op = "<="
	OpEQ Op = "=="
	OpNE Op = "!="
)

func (o Op) Compare(value, threshold float64) bool {
	switch o {
	case OpGT:
		return value > threshold
	case OpGE:
		return value >= threshold
	case OpLT:
		return value < threshold
	case OpLE:
		return value <= threshold
	case OpEQ:
		return value == threshold
	case OpNE:
		return value != threshold
	}
	return false
}

type Rule struct {
	Name string
	Metric string
	Labels tsdb.Labels
	Op Op
	Threshold float64
	For time.Duration
	Severity string
	Lookback time.Duration
}

func (r Rule) Validate() error {
	switch {
	case r.Name == "":
		return errors.New("alert rule: name is required")
	case r.Metric == "":
		return fmt.Errorf("rule %q: metric is required", r.Name)
	case r.For < 0:
		return fmt.Errorf("rule %q: 'for' must not be negative", r.Name)
	}
	switch r.Op {
	case OpGT, OpGE, OpLT, OpLE, OpEQ, OpNE:
	default:
		return fmt.Errorf("rule %q: unknown operator %q", r.Name, r.Op)
	}
	return nil
}

type State string

const (
	StateInactive State = "inactive"
	StatePending State = "pending"
	StateFiring State = "firing"
)

type Alert struct {
	Rule string `json:"rule"`
	Severity string `json:"severity"`
	Series tsdb.Labels `json:"series"`
	Value float64 `json:"value"`
	State State `json:"state"`
	Since time.Time `json:"since"`
	FiredAt *time.Time `json:"fired_at,omitempty"`
}

type Event struct {
	Status string `json:"status"`
	Alert Alert `json:"alert"`
	Time time.Time `json:"time"`
}