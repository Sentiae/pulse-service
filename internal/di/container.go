package di

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	httphandler "github.com/sentiae/pulse-service/internal/handler/http"
	"github.com/sentiae/pulse-service/internal/infrastructure/messaging"
	"github.com/sentiae/pulse-service/internal/repository/postgres"
	"github.com/sentiae/pulse-service/internal/usecase"
	"github.com/sentiae/pulse-service/pkg/config"
	"github.com/sentiae/pulse-service/pkg/events"
	"github.com/sentiae/pulse-service/pkg/logger"
)

// Container wires all of pulse-service's collaborators together.
type Container struct {
	Config *config.Config
	DB     *gorm.DB

	FlowRepo      *postgres.FlowRepository
	FlowTracker   *usecase.FlowTracker
	AuditRecorder *usecase.AuditRecorder
	Aggregator    *usecase.Aggregator
	AlertTracker  *usecase.AlertTracker
	DeployTracker *usecase.DeployTracker

	FlowConsumer           *messaging.FlowConsumer
	AuditConsumer          *messaging.AuditConsumer
	AlertActivityConsumer  *messaging.AlertActivityConsumer
	DeployActivityConsumer *messaging.DeployActivityConsumer

	HTTPServer *httphandler.Server
	Publisher  events.Publisher
}

// NewContainer constructs the container. It is the only place that knows
// how the service is wired.
func NewContainer(cfg *config.Config) (*Container, error) {
	c := &Container{Config: cfg}
	if err := c.initDatabase(); err != nil {
		return nil, fmt.Errorf("init database: %w", err)
	}
	c.initInfrastructure()
	c.initRepositories()
	c.initUseCases()
	if err := c.initConsumers(); err != nil {
		return nil, fmt.Errorf("init consumers: %w", err)
	}
	c.initHandlers()
	return c, nil
}

func (c *Container) initDatabase() error {
	port, err := strconv.Atoi(c.Config.Database.Postgres.Port)
	if err != nil {
		port = 5432
	}
	logLevel := gormlogger.Warn
	switch c.Config.Database.Postgres.LogLevel {
	case "info":
		logLevel = gormlogger.Info
	case "error":
		logLevel = gormlogger.Error
	case "silent":
		logLevel = gormlogger.Silent
	}
	db, err := postgres.NewDB(postgres.Config{
		Host:     c.Config.Database.Postgres.Host,
		Port:     port,
		User:     c.Config.Database.Postgres.User,
		Password: c.Config.Database.Postgres.Password,
		Database: c.Config.Database.Postgres.Database,
		SSLMode:  c.Config.Database.Postgres.SSLMode,
		LogLevel: logLevel,
	})
	if err != nil {
		return err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	sqlDB.SetMaxOpenConns(c.Config.Database.Postgres.Pool.MaxOpenConns)
	sqlDB.SetMaxIdleConns(c.Config.Database.Postgres.Pool.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(c.Config.Database.Postgres.Pool.MaxLifetime)
	sqlDB.SetConnMaxIdleTime(c.Config.Database.Postgres.Pool.MaxIdleTime)
	c.DB = db
	if err := postgres.AutoMigrate(db); err != nil {
		return fmt.Errorf("auto-migrate: %w", err)
	}
	logger.Info("Database connected and migrated")
	return nil
}

func (c *Container) initInfrastructure() {
	publish := c.Config.Messaging.Kafka.Enabled && c.Config.Features.EventPublishing
	c.Publisher = events.NewKafkaPublisher(c.Config.GetKafkaBrokers(), publish)
	if publish {
		ensureCtx, ensureCancel := context.WithTimeout(context.Background(), 15*time.Second)
		if err := c.Publisher.EnsureTopics(ensureCtx); err != nil {
			log.Printf("Warning: pulse-service Kafka EnsureTopics failed: %v (continuing)", err)
		}
		ensureCancel()
	}
}

func (c *Container) initRepositories() {
	c.FlowRepo = postgres.NewFlowRepository(c.DB)
}

func (c *Container) initUseCases() {
	c.FlowTracker = usecase.NewFlowTracker(c.FlowRepo, c.Publisher)
	c.AuditRecorder = usecase.NewAuditRecorder(c.FlowRepo, c.Publisher)
	c.AlertTracker = usecase.NewAlertTracker()
	c.DeployTracker = usecase.NewDeployTracker()
}

func (c *Container) initConsumers() error {
	if !c.Config.Messaging.Kafka.Enabled {
		logger.Info("Kafka disabled — pulse will not observe saga events")
		return nil
	}
	cons, err := messaging.NewFlowConsumer(
		c.Config.GetKafkaBrokers(),
		c.Config.Messaging.Kafka.GroupID,
		c.FlowTracker,
	)
	if err != nil {
		// Don't fail the service on consumer wiring error — Pulse still
		// serves the REST API for historical flow lookups.
		logger.Error("flow consumer not started: %v", err)
	} else {
		c.FlowConsumer = cons
	}

	// Audit consumer uses a separate group id so it receives every event
	// independently of the saga consumer.
	auditGroup := c.Config.Messaging.Kafka.GroupID + "-audit"
	audit, err := messaging.NewAuditConsumer(
		c.Config.GetKafkaBrokers(),
		auditGroup,
		c.AuditRecorder,
	)
	if err != nil {
		logger.Error("audit consumer not started: %v", err)
	} else {
		c.AuditConsumer = audit
	}

	// §3.1/§3.2 activity consumers. Separate group ids so neither the
	// flow nor audit consumer eats these events out from under us.
	alertGroup := c.Config.Messaging.Kafka.GroupID + "-alert-activity"
	alertCons, err := messaging.NewAlertActivityConsumer(
		c.Config.GetKafkaBrokers(),
		alertGroup,
		c.AlertTracker,
	)
	if err != nil {
		logger.Error("alert activity consumer not started: %v", err)
	} else {
		c.AlertActivityConsumer = alertCons
	}

	deployGroup := c.Config.Messaging.Kafka.GroupID + "-deploy-activity"
	deployCons, err := messaging.NewDeployActivityConsumer(
		c.Config.GetKafkaBrokers(),
		deployGroup,
		c.DeployTracker,
	)
	if err != nil {
		logger.Error("deploy activity consumer not started: %v", err)
	} else {
		c.DeployActivityConsumer = deployCons
	}
	return nil
}

func (c *Container) initHandlers() {
	c.HTTPServer = httphandler.NewServer(c.FlowTracker, c.AuditRecorder)
	c.HTTPServer.SetActivityTrackers(c.AlertTracker, c.DeployTracker)
	// §3 Pulse aggregator — gRPC fan-out to ops + work services.
	// Enabled when at least one OPS_SERVICE_GRPC / WORK_SERVICE_GRPC
	// address is set. Fail-open: aggregator-less pulse still serves
	// flow endpoints.
	cfg := loadAggregatorConfig()
	if cfg.OpsConn != nil || cfg.WorkConn != nil {
		c.Aggregator = usecase.NewAggregator(cfg)
		c.HTTPServer.SetAggregator(c.Aggregator)
		logger.Info("pulse aggregator enabled (ops=%v work=%v)", cfg.OpsConn != nil, cfg.WorkConn != nil)
	}
}

// loadAggregatorConfig dials gRPC connections to ops + work services
// from env-configured addresses (OPS_SERVICE_GRPC / WORK_SERVICE_GRPC,
// e.g. "ops-service:50051"). Missing env = nil conn = signal skipped.
func loadAggregatorConfig() usecase.AggregatorConfig {
	cfg := usecase.AggregatorConfig{}
	if addr := os.Getenv("OPS_SERVICE_GRPC"); addr != "" {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			cfg.OpsConn = conn
		} else {
			logger.Error("pulse aggregator: dial ops %s: %v", addr, err)
		}
	}
	if addr := os.Getenv("WORK_SERVICE_GRPC"); addr != "" {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			cfg.WorkConn = conn
		} else {
			logger.Error("pulse aggregator: dial work %s: %v", addr, err)
		}
	}
	return cfg
}

// StartConsumers blocks; call in a goroutine.
func (c *Container) StartConsumers(ctx context.Context) {
	consumers := []struct {
		name  string
		start func(context.Context) error
	}{
		{"flow", startOr(c.FlowConsumer)},
		{"audit", startOr(c.AuditConsumer)},
		{"alert-activity", startOr(c.AlertActivityConsumer)},
		{"deploy-activity", startOr(c.DeployActivityConsumer)},
	}

	done := make(chan struct{}, len(consumers))
	active := 0
	for _, cns := range consumers {
		if cns.start == nil {
			continue
		}
		active++
		cns := cns
		go func() {
			if err := cns.start(ctx); err != nil {
				logger.Error("%s consumer error: %v", cns.name, err)
			}
			done <- struct{}{}
		}()
	}
	if active == 0 {
		logger.Info("no consumers configured")
		return
	}
	for i := 0; i < active; i++ {
		<-done
	}
}

// startOr returns the consumer's Start method as a plain func, or nil
// if the consumer isn't wired. Avoids reflection in the hot path.
func startOr(c any) func(context.Context) error {
	switch v := c.(type) {
	case *messaging.FlowConsumer:
		if v == nil {
			return nil
		}
		return v.Start
	case *messaging.AuditConsumer:
		if v == nil {
			return nil
		}
		return v.Start
	case *messaging.AlertActivityConsumer:
		if v == nil {
			return nil
		}
		return v.Start
	case *messaging.DeployActivityConsumer:
		if v == nil {
			return nil
		}
		return v.Start
	}
	return nil
}

// Close tears down shared resources.
func (c *Container) Close() {
	if c.Publisher != nil {
		_ = c.Publisher.Close()
	}
	if c.DB != nil {
		if sqlDB, err := c.DB.DB(); err == nil {
			_ = sqlDB.Close()
		}
	}
}
