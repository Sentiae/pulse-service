package events

import (
	kafka "github.com/sentiae/platform-kit/kafka"
)

// Re-exports keep the rest of pulse-service oblivious to the platform-kit
// package layout.
type (
	Publisher  = kafka.Publisher
	EventData  = kafka.EventData
	CloudEvent = kafka.CloudEvent
)

// Event type constants that Pulse itself emits. Registered in
// platform-kit/kafka/event_taxonomy.go under the "pulse" domain.
const (
	SourceName = "pulse-service"

	EventFlowCreated       = "pulse.flow.created"
	EventFlowStepStarted   = "pulse.flow.step_started"
	EventFlowStepCompleted = "pulse.flow.step_completed"
	EventFlowCompleted     = "pulse.flow.completed"
	EventFlowFailed        = "pulse.flow.failed"
)

// NewKafkaPublisher creates a platform-kit-backed publisher. Falls back to
// a no-op publisher when disabled or when the broker is unreachable so the
// service stays up during local dev.
func NewKafkaPublisher(brokers []string, enabled bool) Publisher {
	if !enabled {
		return kafka.NewNoopPublisher()
	}
	pub, err := kafka.NewPublisher(kafka.PublisherConfig{
		Brokers:     brokers,
		Source:      SourceName,
		TopicPrefix: "sentiae",
	})
	if err != nil {
		return kafka.NewNoopPublisher()
	}
	return pub
}
