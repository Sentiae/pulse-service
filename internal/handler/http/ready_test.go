//go:build unit

package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestReadyEndpoint drives the exact surface the compose healthcheck probes
// (GET /ready). The healthcheck is `wget -qO- .../ready` with no `|| exit 0`,
// so a non-2xx here is what marks the container unhealthy.
func TestReadyEndpoint(t *testing.T) {
	tests := []struct {
		name       string
		readiness  ReadinessFunc
		wantStatus int
		wantBody   string
		// wantDegradedKey asserts whether the "degraded" key is present. A
		// healthy pulse must NOT carry it: the field only means something if it
		// is absent when nothing is declared.
		wantDegradedKey bool
	}{
		{
			name:       "no reasons -> 200 READY",
			readiness:  func() Readiness { return Readiness{} },
			wantStatus: http.StatusOK,
			wantBody:   "ready",
		},
		{
			name:       "empty (non-nil) reasons -> 200 READY",
			readiness:  func() Readiness { return Readiness{Reasons: []string{}} },
			wantStatus: http.StatusOK,
			wantBody:   "ready",
		},
		{
			name: "consumer cannot fetch -> 503 NOT READY",
			readiness: func() Readiness {
				return Readiness{Reasons: []string{"flow consumer cannot fetch: zero partitions assigned"}}
			},
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   "flow consumer cannot fetch",
		},
		{
			name: "consumer unwired -> 503 NOT READY",
			readiness: func() Readiness {
				return Readiness{Reasons: []string{"audit consumer not wired: kafka: at least one broker is required"}}
			},
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   "audit consumer not wired",
		},
		{
			// D-162a: a declared fail-open stays 200 but must be enumerable —
			// an intentionally blind pulse cannot look like a working one.
			name: "kafka disabled by config -> 200 READY but degraded is visible",
			readiness: func() Readiness {
				return Readiness{Degraded: []string{"kafka disabled by config (APP_KAFKA_ENABLED=false): this ledger is observing nothing"}}
			},
			wantStatus:      http.StatusOK,
			wantBody:        "this ledger is observing nothing",
			wantDegradedKey: true,
		},
		{
			// Fail-closed: an unwired probe cannot prove readiness, so it must
			// not report success.
			name:       "nil readiness probe -> 503 NOT READY",
			readiness:  nil,
			wantStatus: http.StatusServiceUnavailable,
			wantBody:   "readiness probe not wired",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// /ready is unauthenticated and touches none of the other
			// collaborators, so nil is safe for them here.
			s := NewServer(nil, "", nil, nil, nil, tt.readiness, nil)

			rec := httptest.NewRecorder()
			s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ready", nil))

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rec.Code, tt.wantStatus, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), tt.wantBody) {
				t.Fatalf("body %q missing %q", rec.Body.String(), tt.wantBody)
			}
			// Body must be valid JSON so ops tooling can parse the reasons.
			var body map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("body is not valid JSON: %v", err)
			}
			if _, ok := body["degraded"]; ok != tt.wantDegradedKey {
				t.Fatalf("degraded key present = %v, want %v (body: %s)", ok, tt.wantDegradedKey, rec.Body.String())
			}
		})
	}
}

// TestHealthEndpointStaysLiveness pins the split: /health is liveness (the
// process is up) and must NOT fail when consumers are stuck, or an unreachable
// broker would restart-loop the container instead of marking it not-ready.
func TestHealthEndpointStaysLiveness(t *testing.T) {
	s := NewServer(nil, "", nil, nil, nil, func() Readiness {
		return Readiness{Reasons: []string{"flow consumer cannot fetch: zero partitions assigned"}}
	}, nil)

	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("/health status = %d, want %d", rec.Code, http.StatusOK)
	}
}
