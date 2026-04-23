package messaging

import (
	"context"
	"log/slog"

	kafka "github.com/sentiae/platform-kit/kafka"

	"github.com/sentiae/pulse-service/internal/usecase"
	"github.com/sentiae/pulse-service/pkg/logger"
)

// alertEventTypes is the set of ops.alert.* CloudEvents the
// AlertTracker observes. We include both the canonical "fired" name
// and the backward-compat "triggered" name because ops-service dual-
// publishes both.
var alertEventTypes = []string{
	"ops.alert.triggered",
	"ops.alert.fired",
	"ops.alert.acknowledged",
	"ops.alert.resolved",
}

// deployEventTypes covers both the canonical long-form ops.deployment.*
// events and the ops.deploy.* aliases the DeploymentUseCase +
// DeployExecutor emit during rollout. Subscribing to both means we
// can't miss a progression regardless of which name the publisher used.
var deployEventTypes = []string{
	"ops.deployment.started",
	"ops.deployment.progressed",
	"ops.deployment.completed",
	"ops.deployment.failed",
	"ops.deployment.rolled_back",
	"ops.deploy.created",
	"ops.deploy.started",
	"ops.deploy.in_progress",
	"ops.deploy.completed",
	"ops.deploy.failed",
	"ops.deploy.rolled_back",
}

// AlertActivityConsumer bridges ops.alert.* CloudEvents to the
// AlertTracker. Uses its own consumer group so it receives every
// message independently of the saga/flow consumer.
type AlertActivityConsumer struct {
	consumer *kafka.KafkaConsumer
	tracker  *usecase.AlertTracker
}

// DeployActivityConsumer bridges ops.deployment.*/ops.deploy.*
// CloudEvents to the DeployTracker.
type DeployActivityConsumer struct {
	consumer *kafka.KafkaConsumer
	tracker  *usecase.DeployTracker
}

// NewAlertActivityConsumer wires the tracker to the alert topics.
func NewAlertActivityConsumer(brokers []string, groupID string, tracker *usecase.AlertTracker) (*AlertActivityConsumer, error) {
	topics := topicsForEventTypes(alertEventTypes)
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
	handler := func(ctx context.Context, ev kafka.CloudEvent) error {
		return tracker.OnEvent(ctx, ev)
	}
	for _, t := range alertEventTypes {
		cons.Subscribe(t, handler)
	}
	logger.Info("pulse alert activity consumer registered %d event types across %d topics", len(alertEventTypes), len(topics))
	return &AlertActivityConsumer{consumer: cons, tracker: tracker}, nil
}

// Start blocks until ctx is done.
func (c *AlertActivityConsumer) Start(ctx context.Context) error {
	return c.consumer.Start(ctx)
}

// NewDeployActivityConsumer wires the tracker to deploy lifecycle topics.
func NewDeployActivityConsumer(brokers []string, groupID string, tracker *usecase.DeployTracker) (*DeployActivityConsumer, error) {
	topics := topicsForEventTypes(deployEventTypes)
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
	handler := func(ctx context.Context, ev kafka.CloudEvent) error {
		return tracker.OnEvent(ctx, ev)
	}
	for _, t := range deployEventTypes {
		cons.Subscribe(t, handler)
	}
	logger.Info("pulse deploy activity consumer registered %d event types across %d topics", len(deployEventTypes), len(topics))
	return &DeployActivityConsumer{consumer: cons, tracker: tracker}, nil
}

// Start blocks until ctx is done.
func (c *DeployActivityConsumer) Start(ctx context.Context) error {
	return c.consumer.Start(ctx)
}

// topicsForEventTypes derives the deduped Kafka topic list for a bare
// event-type list. Mirrors the helper in flow_consumer.go so both
// consumers share the same fallback semantics when the event hasn't
// been registered in the taxonomy yet.
func topicsForEventTypes(types []string) []string {
	set := map[string]struct{}{}
	for _, t := range types {
		if reg, ok := kafka.LookupEvent(t); ok {
			set[reg.FullTopic("sentiae")] = struct{}{}
			continue
		}
		set[fallbackTopic(t)] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for t := range set {
		out = append(out, t)
	}
	return out
}
