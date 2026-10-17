// Package scrape collects metrics from Prometheus text-format endpoints and writes them into a time-series appender.
package scrape

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/OluwaTobba/monitoring-and-observation-platform/internal/tsdb"
)

// Point is one parsed exposition line.
type Point struct {
	Name      string
	Labels    tsdb.Labels
	Value     float64
	Timestamp int64
}

// Parse reads the Prometheus text exposition format. HELP and TYPE comments are skipped. A malformed line fails the whole parse, matching Prometheus behaviour for a failed scrape.
func Parse(r io.Reader) ([]Point, error) {
	var points []Point
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	lineNo := 0
	for sc.Scan() {
		lineNo++
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p, err := parseLine(line)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNo, err)
		}
		points = append(points, p)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return points, nil
}

func parseLine(line string) (Point, error) {
	var p Point
	i := 0
	for i < len(line) && isNameByte(line[i], i == 0) {
		i++
	}
	if i == 0 {
		return p, fmt.Errorf("missing metric name: %q", line)
	}
	p.Name = line[:i]
	p.Labels = tsdb.Labels{}

	rest := line[i:]
	if strings.HasPrefix(rest, "{") {
		consumed, labels, err := parseLabels(rest)
		if err != nil {
			return p, err
		}
		p.Labels = labels
		rest = rest[consumed:]
	}

	fields := strings.Fields(rest)
	if len(fields) == 0 || len(fields) > 2 {
		return p, fmt.Errorf("expected value and optional timestamp: %q", line)
	}
	v, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return p, fmt.Errorf("invalid value %q: %w", fields[0], err)
	}
	p.Value = v

	if len(fields) == 2 {
		ts, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			return p, fmt.Errorf("invalid timestamp %q: %w", fields[1], err)
		}
		p.Timestamp = ts
	}
	return p, nil
}

// parseLabels parses a {k="v",...} block starting at s[0] == '{'. It returns the number of bytes consumed, including the closing brace.
func parseLabels(s string) (int, tsdb.Labels, error) {
	labels := tsdb.Labels{}
	i := 1
	for {
		for i < len(s) && (s[i] == ' ' || s[i] == ',') {
			i++
		}
		if i >= len(s) {
			return 0, nil, fmt.Errorf("unterminated label set")
		}
		if s[i] == '}' {
			return i + 1, labels, nil
		}

		start := i
		for i < len(s) && s[i] != '=' && s[i] != ' ' && s[i] != '}' {
			i++
		}
		name := s[start:i]
		if name == "" {
			return 0, nil, fmt.Errorf("empty label name")
		}
		for i < len(s) && s[i] == ' ' {
			i++
		}
		if i >= len(s) || s[i] != '=' {
			return 0, nil, fmt.Errorf("label %q: expected '='", name)
		}
		i++
		for i < len(s) && s[i] == ' ' {
			i++
		}
		if i >= len(s) || s[i] != '"' {
			return 0, nil, fmt.Errorf("label %q: value must be quoted", name)
		}
		i++

		var b strings.Builder
		closed := false
		for i < len(s) {
			c := s[i]
			if c == '"' {
				closed = true
				i++
				break
			}
			if c == '\\' && i+1 < len(s) {
				switch s[i+1] {
				case 'n':
					b.WriteByte('\n')
				case '\\':
					b.WriteByte('\\')
				case '"':
					b.WriteByte('"')
				default:
					b.WriteByte(s[i+1])
				}
				i += 2
				continue
			}
			b.WriteByte(c)
			i++
		}
		if !closed {
			return 0, nil, fmt.Errorf("label %q: unterminated value", name)
		}
		labels[name] = b.String()
	}
}

func isNameByte(c byte, first bool) bool {
	switch {
	case c == '_' || c == ':' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'):
		return true
	case c >= '0' && c <= '9':
		return !first
	}
	return false
}
