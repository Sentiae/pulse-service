package messaging

import (
	"context"
	"log/slog"

	kafka "github.com/sentiae/platform-kit/kafka"

	"github.com/sentiae/pulse-service/internal/usecase"
	"github.com/sentiae/pulse-service/pkg/logger"
)

// FlowConsumer bridges platform-kit's generic Kafka consumer to the
// FlowTracker. It subscribes to every saga.* event pulse-service cares
// about so the tracker can reconstruct flow state from the event stream.
type FlowConsumer struct {
	consumer *kafka.KafkaConsumer
	tracker  *usecase.FlowTracker
}

// sagaEventTypes is the fixed list of events the tracker observes. These
// mirror the constants in platform-kit/kafka/event_taxonomy.go; if a new
// saga is added we must append here.
var sagaEventTypes = []string{
	// code_test_deploy
	"saga.code_test_deploy.started",
	"saga.code_test_deploy.tests_requested",
	"saga.code_test_deploy.tests_completed",
	"saga.code_test_deploy.gate_evaluated",
	"saga.code_test_deploy.deployment_requested",
	"saga.code_test_deploy.completed",
	"saga.code_test_deploy.failed",

	// spec_driven
	"saga.spec_driven.started",
	"saga.spec_driven.decomposed",
	"saga.spec_driven.canvas_created",
	"saga.spec_driven.session_created",
	"saga.spec_driven.code_written",
	"saga.spec_driven.testing_started",
	"saga.spec_driven.retry_triggered",
	"saga.spec_driven.completed",
	"saga.spec_driven.failed",

	// self_healing
	"saga.self_healing.started",
	"saga.self_healing.diagnosed",
	"saga.self_healing.approval_required",
	"saga.self_healing.spec_created",
	"saga.self_healing.deployed",
	"saga.self_healing.verified",
	"saga.self_healing.completed",
	"saga.self_healing.failed",
	"saga.self_healing.rolled_back",

	// nl_query
	"saga.nl_query.started",
	"saga.nl_query.translated",
	"saga.nl_query.executed",
	"saga.nl_query.rendered",
	"saga.nl_query.completed",
	"saga.nl_query.failed",

	// import_analyze_canvas (note: uses canvas.saga prefix)
	"canvas.saga.import_analyze_canvas.started",
	"canvas.saga.import_analyze_canvas.nodes_created",
	"canvas.saga.import_analyze_canvas.edges_inferred",
	"canvas.saga.import_analyze_canvas.layout_applied",
	"canvas.saga.import_analyze_canvas.completed",
	"canvas.saga.import_analyze_canvas.failed",

	// spec_shipping — emitted by work-service when a spec transitions
	// to "shipped". Single-step flow used to surface spec lifecycle
	// in the Pulse activity feed.
	"saga.spec_shipping.started",
	"saga.spec_shipping.completed",
}

// topicsForEvents maps saga event types to their full Kafka topics, deduped.
// We look each one up in the platform-kit event taxonomy so the topic
// derivation lives in one place (the taxonomy owns topicFromEventType).
func topicsForEvents() []string {
	set := map[string]struct{}{}
	for _, t := range sagaEventTypes {
		if reg, ok := kafka.LookupEvent(t); ok {
			set[reg.FullTopic("sentiae")] = struct{}{}
			continue
		}
		// Fallback for events not yet registered: compute using the
		// documented convention "sentiae.{domain}.{resource}".
		set[fallbackTopic(t)] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for t := range set {
		out = append(out, t)
	}
	return out
}

// fallbackTopic mirrors platform-kit's internal topicFromEventType
// derivation. Kept as a best-effort fallback for events that haven't been
// registered in the taxonomy yet.
func fallbackTopic(eventType string) string {
	// "a.b.c.d" → "sentiae.a.b"
	first, second := -1, -1
	for i, r := range eventType {
		if r == '.' {
			if first == -1 {
				first = i
			} else if second == -1 {
				second = i
				break
			}
		}
	}
	if second > 0 {
		return "sentiae." + eventType[:second]
	}
	return "sentiae." + eventType
}

// NewFlowConsumer creates the consumer and registers handlers for every
// saga event type.
func NewFlowConsumer(brokers []string, groupID string, tracker *usecase.FlowTracker) (*FlowConsumer, error) {
	topics := topicsForEvents()

	cons, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers: brokers,
		GroupID: groupID,
		Topics:  topics,
		Logger:  slog.Default(),
		// Schema validation is a nice-to-have but for Pulse we don't own any
		// of these events; skip validation so we don't drop messages when
		// another service ships a minor schema tweak.
		DisableSchemaValidation: true,
	})
	if err != nil {
		return nil, err
	}

	handler := func(ctx context.Context, event kafka.CloudEvent) error {
		return tracker.OnEvent(ctx, event)
	}
	for _, t := range sagaEventTypes {
		cons.Subscribe(t, handler)
	}

	logger.Info("pulse consumer registered %d event types across %d topics", len(sagaEventTypes), len(topics))
	return &FlowConsumer{consumer: cons, tracker: tracker}, nil
}

// Start blocks until ctx is done.
func (c *FlowConsumer) Start(ctx context.Context) error {
	return c.consumer.Start(ctx)
}
