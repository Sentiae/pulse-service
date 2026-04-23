package messaging

import (
	"context"
	"log/slog"

	kafka "github.com/sentiae/platform-kit/kafka"

	"github.com/sentiae/pulse-service/internal/usecase"
	"github.com/sentiae/pulse-service/pkg/logger"
)

// AuditConsumer subscribes to every registered CloudEvent topic so the
// AuditRecorder can write a row to pulse_event_audit for every platform
// event. This is the §19.4 "catch-all" consumer that underpins the
// platform-wide audit log.
//
// It uses a distinct consumer group from FlowConsumer so both can
// receive every message independently.
type AuditConsumer struct {
	consumer *kafka.KafkaConsumer
	recorder *usecase.AuditRecorder
}

// NewAuditConsumer wires the recorder to every registered topic.
func NewAuditConsumer(brokers []string, groupID string, recorder *usecase.AuditRecorder) (*AuditConsumer, error) {
	// KnownTopics returns the de-duplicated full topic list for every
	// event registered in platform-kit's taxonomy. New services get
	// audit coverage for free as long as they register their events.
	topics := kafka.KnownTopics("sentiae")

	cons, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:                 brokers,
		GroupID:                 groupID,
		Topics:                  topics,
		Logger:                  slog.Default(),
		DisableSchemaValidation: true,
	})
	if err != nil {
		return nil, err
	}

	// Subscribe to every registered event type. We register a single
	// handler across all of them because AuditRecorder does its own
	// per-event classification.
	handler := func(ctx context.Context, event kafka.CloudEvent) error {
		return recorder.OnEvent(ctx, event)
	}
	seen := map[string]bool{}
	for _, e := range kafka.AllEvents() {
		if seen[e.Type] {
			continue
		}
		seen[e.Type] = true
		cons.Subscribe(e.Type, handler)
	}

	logger.Info("pulse audit consumer registered %d event types across %d topics", len(seen), len(topics))
	return &AuditConsumer{consumer: cons, recorder: recorder}, nil
}

// Start blocks until ctx is done.
func (c *AuditConsumer) Start(ctx context.Context) error {
	return c.consumer.Start(ctx)
}
