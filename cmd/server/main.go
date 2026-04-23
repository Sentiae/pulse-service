package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	pkkafka "github.com/sentiae/platform-kit/kafka"
	"github.com/sentiae/pulse-service/internal/di"
	"github.com/sentiae/pulse-service/pkg/config"
	"github.com/sentiae/pulse-service/pkg/logger"
)

// maybeRegisterKafkaSchemas runs the G17 schema-registry bootstrap.
// Gated by APP_KAFKA_REGISTER_SCHEMAS_ON_BOOT=true.
func maybeRegisterKafkaSchemas() {
	if os.Getenv("APP_KAFKA_REGISTER_SCHEMAS_ON_BOOT") != "true" {
		return
	}
	url := os.Getenv("APP_KAFKA_SCHEMA_REGISTRY_URL")
	if url == "" {
		return
	}
	prefix := os.Getenv("APP_KAFKA_TOPIC_PREFIX")
	if prefix == "" {
		prefix = "sentiae"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	registry := pkkafka.NewSchemaRegistry(url)
	result := pkkafka.RegisterAllSchemas(ctx, registry, prefix)
	if len(result.Errors) > 0 {
		log.Printf("schema-registry bootstrap: registered=%d skipped=%d errors=%d (first: %s)",
			result.Registered, result.Skipped, len(result.Errors), result.Errors[0])
		return
	}
	log.Printf("schema-registry bootstrap: registered %d schemas", result.Registered)
}

func main() {
	logger.Init("info")
	logger.Info("Starting pulse-service (flow visualization)...")
	go maybeRegisterKafkaSchemas()

	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("Failed to load configuration: %v", err)
	}
	logger.Info("Configuration loaded: env=%s, http_port=%s", cfg.App.Environment, cfg.Server.HTTP.Port)

	container, err := di.NewContainer(cfg)
	if err != nil {
		logger.Fatal("Failed to initialize container: %v", err)
	}
	defer container.Close()

	addr := fmt.Sprintf("%s:%s", cfg.Server.HTTP.Host, cfg.Server.HTTP.Port)
	httpServer := &http.Server{
		Addr:        addr,
		Handler:     container.HTTPServer,
		ReadTimeout: cfg.Server.HTTP.Timeouts.Read,
		// WebSocket writes may stay open indefinitely; leave WriteTimeout
		// unset (defaults to 0 = no timeout) so /flows/stream doesn't get
		// cut off at 15s.
		IdleTimeout: cfg.Server.HTTP.Timeouts.Idle,
	}

	go func() {
		logger.Info("HTTP server listening on %s", addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("HTTP server failed: %v", err)
		}
	}()

	consumerCtx, consumerCancel := context.WithCancel(context.Background())
	defer consumerCancel()
	go container.StartConsumers(consumerCtx)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	logger.Info("Received signal %s, shutting down...", sig)

	shutdownTimeout := cfg.Server.HTTP.Timeouts.Shutdown
	if shutdownTimeout == 0 {
		shutdownTimeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		logger.Error("HTTP shutdown error: %v", err)
	}
	logger.Info("pulse-service stopped")
}
