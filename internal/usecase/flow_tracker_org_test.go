package usecase

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/sentiae/pulse-service/internal/domain"
)

// fakeSagaResolver is a stub SagaOrgResolver keyed by saga_id. It mirrors the
// real resolver: a saga_id with a started flow resolves to that flow's org; an
// unknown saga_id resolves to uuid.Nil (the miss path).
type fakeSagaResolver struct {
	bySaga map[string]uuid.UUID
	err    error
}

func (f fakeSagaResolver) ResolveSagaOrg(_ context.Context, sagaID string) (uuid.UUID, error) {
	if f.err != nil {
		return uuid.Nil, f.err
	}
	return f.bySaga[sagaID], nil // zero value uuid.Nil on miss
}

func TestResolveEventOrg(t *testing.T) {
	payloadOrg := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	sagaOrg := uuid.MustParse("22222222-2222-2222-2222-222222222222")

	tests := []struct {
		name       string
		payloadOrg string
		sagaID     string
		resolver   fakeSagaResolver
		want       uuid.UUID
		wantErr    bool
	}{
		{
			name:       "payload org used directly",
			payloadOrg: payloadOrg.String(),
			sagaID:     "saga-1",
			resolver:   fakeSagaResolver{bySaga: map[string]uuid.UUID{"saga-1": sagaOrg}},
			want:       payloadOrg, // payload wins even when the saga also resolves
		},
		{
			name:       "saga resolver hit when payload empty",
			payloadOrg: "",
			sagaID:     "saga-1",
			resolver:   fakeSagaResolver{bySaga: map[string]uuid.UUID{"saga-1": sagaOrg}},
			want:       sagaOrg,
		},
		{
			name:       "malformed payload falls through to saga resolver",
			payloadOrg: "not-a-uuid",
			sagaID:     "saga-1",
			resolver:   fakeSagaResolver{bySaga: map[string]uuid.UUID{"saga-1": sagaOrg}},
			want:       sagaOrg,
		},
		{
			name:       "sentinel when no payload org and no saga match",
			payloadOrg: "",
			sagaID:     "unknown-saga",
			resolver:   fakeSagaResolver{bySaga: map[string]uuid.UUID{}},
			want:       domain.PlatformSentinelOrg,
		},
		{
			name:       "resolver error propagates",
			payloadOrg: "",
			sagaID:     "saga-1",
			resolver:   fakeSagaResolver{err: errors.New("db down")},
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveEventOrg(context.Background(), tt.payloadOrg, tt.sagaID, tt.resolver)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (org=%s)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("got org %s, want %s", got, tt.want)
			}
		})
	}
}

// TestLateSagaEventSharesStartOrg proves a later saga step event that carries no
// org in its payload resolves to the SAME org as the org-carrying start event,
// because the resolver keys on saga_id and the start event's flow already holds
// that org. This is the cross-event invariant the RLS org threading depends on.
func TestLateSagaEventSharesStartOrg(t *testing.T) {
	startOrg := uuid.MustParse("33333333-3333-3333-3333-333333333333")
	const sagaID = "saga-shared"

	// The start event carries the org in its payload.
	gotStart, err := resolveEventOrg(context.Background(), startOrg.String(), sagaID, fakeSagaResolver{})
	if err != nil {
		t.Fatalf("start event: unexpected error: %v", err)
	}
	if gotStart != startOrg {
		t.Fatalf("start event org = %s, want %s", gotStart, startOrg)
	}

	// A later step event for the same saga carries NO org; the resolver returns
	// the org of the flow started above (keyed by saga_id).
	resolver := fakeSagaResolver{bySaga: map[string]uuid.UUID{sagaID: gotStart}}
	gotLate, err := resolveEventOrg(context.Background(), "", sagaID, resolver)
	if err != nil {
		t.Fatalf("late event: unexpected error: %v", err)
	}
	if gotLate != startOrg {
		t.Fatalf("late event landed in org %s, want same-flow org %s", gotLate, startOrg)
	}
}
