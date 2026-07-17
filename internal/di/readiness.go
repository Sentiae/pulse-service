package di

import (
	"fmt"

	httphandler "github.com/sentiae/pulse-service/internal/handler/http"
)

// consumerHealth is the readiness signal every wired consumer exposes. Kept
// as an interface so readinessReasons can be driven without a live broker.
type consumerHealth interface {
	AssignmentError() error
}

// namedConsumer pairs a consumer with the name reported in readiness reasons.
type namedConsumer struct {
	name   string
	health consumerHealth
}

// readinessReasons returns the reasons pulse cannot do its job. An empty
// result means ready.
//
// Fail-closed on purpose. Pulse IS the platform's audit/activity ledger: a
// pulse that observes no events is not a degraded pulse, it is a silent one.
// Before this, a consumer that failed to wire was logged-and-forgotten and the
// container reported healthy while consuming exactly zero events
// (#pulse-kafka-brokers-misconfig). Absence of a working consumer must never
// read as permission to be healthy.
func readinessReasons(wiringErrs []string, consumers []namedConsumer) []string {
	reasons := make([]string, 0, len(wiringErrs)+len(consumers))
	reasons = append(reasons, wiringErrs...)
	for _, c := range consumers {
		// A typed-nil in the interface would panic on the call below; treat any
		// nil consumer as unwired rather than trusting the caller.
		if c.health == nil {
			reasons = append(reasons, fmt.Sprintf("%s consumer not wired", c.name))
			continue
		}
		if err := c.health.AssignmentError(); err != nil {
			reasons = append(reasons, fmt.Sprintf("%s consumer cannot fetch: %v", c.name, err))
		}
	}
	return reasons
}

// consumerHealths lists the wired consumers. Nil consumers are omitted: their
// wiring error is already recorded in wiringErrs, so including them here would
// double-report the same failure.
func (c *Container) consumerHealths() []namedConsumer {
	out := make([]namedConsumer, 0, 4)
	if c.FlowConsumer != nil {
		out = append(out, namedConsumer{name: "flow", health: c.FlowConsumer})
	}
	if c.AuditConsumer != nil {
		out = append(out, namedConsumer{name: "audit", health: c.AuditConsumer})
	}
	if c.AlertActivityConsumer != nil {
		out = append(out, namedConsumer{name: "alert-activity", health: c.AlertActivityConsumer})
	}
	if c.DeployActivityConsumer != nil {
		out = append(out, namedConsumer{name: "deploy-activity", health: c.DeployActivityConsumer})
	}
	return out
}

// kafkaDisabledDegraded is the reported form of the one legal fail-open here.
const kafkaDisabledDegraded = "kafka disabled by config (APP_KAFKA_ENABLED=false): this ledger is observing nothing"

// Readiness reports pulse's readiness verdict. Wired into the HTTP /ready
// route, which the compose healthcheck probes.
func (c *Container) Readiness() httphandler.Readiness {
	// Kafka switched off is an explicit operator choice, not a failure, so it
	// stays READY: pulse still serves the REST API for historical lookups, and
	// reporting a deliberate choice as a failure only trains operators to
	// ignore readiness.
	//
	// This is a legal fail-open ONLY because it is named, explicit AND
	// reported (D-162a) — not because a blind ledger is harmless. It is not
	// legal by virtue of the flag alone: strip the Degraded line below and this
	// branch becomes the same silent-zero-events hole as the wrong broker port,
	// merely authorized. Every deployed environment sets APP_KAFKA_ENABLED=true
	// (.env.shared), so this branch is dev/local only.
	if !c.Config.Messaging.Kafka.Enabled {
		return httphandler.Readiness{Degraded: []string{kafkaDisabledDegraded}}
	}
	return httphandler.Readiness{
		Reasons: readinessReasons(c.wiringErrs, c.consumerHealths()),
	}
}
