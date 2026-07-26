//go:build unit

package messaging

import (
	"testing"

	kafka "github.com/sentiae/platform-kit/kafka"
)

// Every consumer topic must equal the topic the PUBLISHER derives for the
// same event type — that equality, not any literal, is the invariant. A
// second, local copy of the derivation (the deleted fallbackTopic) is how the
// doubled-topic bug survived: the copy drifts and the consumer starves.
func TestTopicsForEventTypesMatchPublisherDerivation(t *testing.T) {
	sets := map[string][]string{
		"saga":   sagaEventTypes,
		"alert":  alertEventTypes,
		"deploy": deployEventTypes,
	}

	for name, types := range sets {
		t.Run(name, func(t *testing.T) {
			topics, unregistered := topicsForEventTypes(types)

			// Every derived topic must be one a registered publisher event
			// resolves to. Nothing may be invented locally.
			derivable := map[string]struct{}{}
			for _, e := range kafka.AllEvents() {
				derivable[e.FullTopic("sentiae")] = struct{}{}
			}
			for _, got := range topics {
				if _, ok := derivable[got]; !ok {
					t.Errorf("topic %q is not derived by any registered event — locally invented", got)
				}
			}

			// Each registered type in the set must contribute exactly the
			// topic its publisher derives, and unregistered types must
			// contribute nothing but be reported.
			have := map[string]struct{}{}
			for _, tp := range topics {
				have[tp] = struct{}{}
			}
			reported := map[string]struct{}{}
			for _, u := range unregistered {
				reported[u] = struct{}{}
			}
			for _, et := range types {
				reg, ok := kafka.LookupEvent(et)
				if !ok {
					if _, seen := reported[et]; !seen {
						t.Errorf("event %q is unregistered but was not reported", et)
					}
					continue
				}
				want := reg.FullTopic("sentiae")
				if _, seen := have[want]; !seen {
					t.Errorf("event %q publishes to %q but the consumer does not subscribe to it", et, want)
				}
			}
		})
	}
}

// A single event type resolves to exactly the publisher's topic, for any
// prefix the taxonomy is asked about.
func TestTopicsForEventTypesSingleEvent(t *testing.T) {
	for _, et := range sagaEventTypes {
		reg, ok := kafka.LookupEvent(et)
		if !ok {
			continue
		}
		t.Run(et, func(t *testing.T) {
			topics, unregistered := topicsForEventTypes([]string{et})
			if len(unregistered) != 0 {
				t.Fatalf("unexpected unregistered: %v", unregistered)
			}
			if len(topics) != 1 || topics[0] != reg.FullTopic("sentiae") {
				t.Fatalf("got %v, want [%s] (the publisher's derivation)", topics, reg.FullTopic("sentiae"))
			}
		})
	}
}

// An unregistered event type yields NO topic. platform-kit's publisher
// rejects unregistered types, so any topic guessed for one is a subscription
// to a topic nobody can write — exactly what the old fallback produced.
func TestUnregisteredEventTypeYieldsNoTopic(t *testing.T) {
	const bogus = "saga.definitely_not_registered.started"
	if _, ok := kafka.LookupEvent(bogus); ok {
		t.Skipf("%q is registered; pick another fixture", bogus)
	}

	topics, unregistered := topicsForEventTypes([]string{bogus})
	if len(topics) != 0 {
		t.Fatalf("invented topics %v for an unpublishable event type", topics)
	}
	if len(unregistered) != 1 || unregistered[0] != bogus {
		t.Fatalf("unregistered = %v, want [%s]", unregistered, bogus)
	}
}
