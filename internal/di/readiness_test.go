//go:build unit

package di

import (
	"errors"
	"strings"
	"testing"

	"github.com/sentiae/pulse-service/pkg/config"
)

// fakeConsumer drives readinessReasons without a live broker.
type fakeConsumer struct{ err error }

func (f fakeConsumer) AssignmentError() error { return f.err }

// errNoPartitions stands in for platform-kit's fatal "group stable with ZERO
// partitions assigned" error — the state an unreachable broker lands in once
// the assignment deadline passes.
var errNoPartitions = errors.New("consumer group is stable with ZERO partitions assigned: messages will never be fetched")

// TestContainerReadinessKafkaDisabled pins D-162a's "enumerable" half: the
// APP_KAFKA_ENABLED=false fail-open is legal only because it is reported. A
// pulse that is intentionally blind stays READY, but says so.
func TestContainerReadinessKafkaDisabled(t *testing.T) {
	newContainer := func(kafkaEnabled bool) *Container {
		cfg := &config.Config{}
		cfg.Messaging.Kafka.Enabled = kafkaEnabled
		return &Container{Config: cfg}
	}

	t.Run("kafka disabled -> READY but degraded is reported", func(t *testing.T) {
		rd := newContainer(false).Readiness()

		if len(rd.Reasons) != 0 {
			t.Fatalf("reasons = %v, want none (a declared choice is not a failure)", rd.Reasons)
		}
		if len(rd.Degraded) != 1 {
			t.Fatalf("degraded = %v, want exactly 1 entry — an intentionally blind pulse must not look healthy", rd.Degraded)
		}
		if !strings.Contains(rd.Degraded[0], "observing nothing") {
			t.Fatalf("degraded %q does not state the consequence", rd.Degraded[0])
		}
		if !strings.Contains(rd.Degraded[0], "APP_KAFKA_ENABLED") {
			t.Fatalf("degraded %q does not name the flag that caused it", rd.Degraded[0])
		}
	})

	t.Run("kafka enabled -> no degraded claim", func(t *testing.T) {
		// With kafka on, nothing is declared, so the degraded channel must be
		// empty — otherwise "degraded" would be noise rather than signal.
		rd := newContainer(true).Readiness()

		if len(rd.Degraded) != 0 {
			t.Fatalf("degraded = %v, want none", rd.Degraded)
		}
	})
}

func TestReadinessReasons(t *testing.T) {
	healthy := fakeConsumer{err: nil}
	stuck := fakeConsumer{err: errNoPartitions}

	tests := []struct {
		name       string
		wiringErrs []string
		consumers  []namedConsumer
		wantReady  bool
		wantSubstr string
	}{
		{
			name: "all consumers healthy -> READY",
			consumers: []namedConsumer{
				{name: "flow", health: healthy},
				{name: "audit", health: healthy},
				{name: "alert-activity", health: healthy},
				{name: "deploy-activity", health: healthy},
			},
			wantReady: true,
		},
		{
			// The live bug: brokers reachable at bootstrap, then metadata
			// redirects to an address that is not the broker -> zero partitions
			// -> zero events, forever.
			name: "one consumer cannot fetch -> NOT ready",
			consumers: []namedConsumer{
				{name: "flow", health: stuck},
				{name: "audit", health: healthy},
			},
			wantReady:  false,
			wantSubstr: "flow consumer cannot fetch",
		},
		{
			name: "every consumer cannot fetch -> NOT ready",
			consumers: []namedConsumer{
				{name: "flow", health: stuck},
				{name: "audit", health: stuck},
			},
			wantReady:  false,
			wantSubstr: "audit consumer cannot fetch",
		},
		{
			name:       "consumer failed to wire at boot -> NOT ready",
			wiringErrs: []string{"audit consumer not wired: kafka: at least one broker is required"},
			consumers:  []namedConsumer{{name: "flow", health: healthy}},
			wantReady:  false,
			wantSubstr: "audit consumer not wired",
		},
		{
			// Defends the typed-nil trap: a nil consumer in the interface must
			// read as unwired, not panic and not pass as healthy.
			name:       "nil consumer -> NOT ready",
			consumers:  []namedConsumer{{name: "flow", health: nil}},
			wantReady:  false,
			wantSubstr: "flow consumer not wired",
		},
		{
			name:       "wiring error with healthy survivors still -> NOT ready",
			wiringErrs: []string{"flow consumer not wired: boom"},
			consumers: []namedConsumer{
				{name: "audit", health: healthy},
				{name: "alert-activity", health: healthy},
			},
			wantReady:  false,
			wantSubstr: "flow consumer not wired",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reasons := readinessReasons(tt.wiringErrs, tt.consumers)
			gotReady := len(reasons) == 0
			if gotReady != tt.wantReady {
				t.Fatalf("ready = %v, want %v (reasons: %v)", gotReady, tt.wantReady, reasons)
			}
			if tt.wantSubstr == "" {
				return
			}
			joined := strings.Join(reasons, " | ")
			if !strings.Contains(joined, tt.wantSubstr) {
				t.Fatalf("reasons %q missing %q", joined, tt.wantSubstr)
			}
		})
	}
}
