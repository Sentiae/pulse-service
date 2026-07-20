package di

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	pkconfig "github.com/sentiae/platform-kit/config"
	"github.com/sentiae/platform-kit/grpcclient"
	pkgmiddleware "github.com/sentiae/platform-kit/middleware"
	"github.com/sentiae/platform-kit/spiffe"
	"github.com/sentiae/platform-kit/tenant"
	"github.com/sentiae/platform-kit/tenantdb"
	"github.com/spiffe/go-spiffe/v2/workloadapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
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

	// TenantResolver resolves owning orgs via the D-072 SECURITY DEFINER rls_*
	// functions; the SagaOrgResolver for FlowTracker and the OrgResolver for the
	// HTTP by-id handlers. Built on the app pool.
	TenantResolver *postgres.TenantResolverRepo

	// JWKSValidator validates BFF-forwarded user Bearer tokens for the HTTP auth
	// middleware (D-073). Nil when RLS enforcement is off and JWKS is unavailable.
	JWKSValidator pkgmiddleware.TokenValidator

	FlowConsumer           *messaging.FlowConsumer
	AuditConsumer          *messaging.AuditConsumer
	AlertActivityConsumer  *messaging.AlertActivityConsumer
	DeployActivityConsumer *messaging.DeployActivityConsumer

	// wiringErrs records consumers that failed to construct at boot. Pulse keeps
	// serving the REST API (a wiring failure must not crash-loop the service) but
	// reports NOT ready, so the failure cannot pass as healthy. Written once
	// during initConsumers, read-only afterwards.
	wiringErrs []string

	HTTPServer *httphandler.Server
	Publisher  events.Publisher

	// mtlsSource is the shared SPIFFE X509 source for pulse's outbound gRPC
	// dials (Phase 2 mTLS mesh). Nil when APP_GRPC_MTLS_MODE is off or the
	// workload API is unavailable — grpcclient.Dial then falls back to insecure.
	mtlsSource *workloadapi.X509Source
}

// NewContainer constructs the container. It is the only place that knows
// how the service is wired.
func NewContainer(cfg *config.Config) (*Container, error) {
	c := &Container{Config: cfg}
	if err := c.initDatabase(); err != nil {
		return nil, fmt.Errorf("init database: %w", err)
	}
	if err := c.initAuth(); err != nil {
		return nil, fmt.Errorf("init auth: %w", err)
	}
	c.initInfrastructure()
	c.initRepositories()
	c.initUseCases()
	if err := c.initConsumers(); err != nil {
		return nil, fmt.Errorf("init consumers: %w", err)
	}
	c.initMTLSSource()
	c.initHandlers()
	return c, nil
}

// initMTLSSource builds the one shared SPIFFE X509 source for pulse's outbound
// gRPC dials when APP_GRPC_MTLS_MODE is not off. On workload-API failure it
// degrades to nil so grpcclient.Dial falls back to insecure.
func (c *Container) initMTLSSource() {
	if pkconfig.MTLSMode() == pkconfig.MTLSModeOff {
		return
	}
	src, err := spiffe.NewSource(context.Background())
	if err != nil {
		logger.Error("SPIFFE client source unavailable, gRPC clients degrade to insecure: %v", err)
		return
	}
	c.mtlsSource = src
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
	pg := c.Config.Database.Postgres

	// OWNER connection for schema DDL (golang-migrate baseline incl. RLS) — D-070
	// role split. Uses MigrateUser/MigratePassword when set, else falls back to the
	// app creds so an unsplit deploy connects as the same role as before.
	// Short-lived and closed immediately after schema setup so no DDL-capable pool
	// lingers.
	ownerUser, ownerPassword := pg.User, pg.Password
	if pg.MigrateUser != "" {
		ownerUser, ownerPassword = pg.MigrateUser, pg.MigratePassword
	}
	ownerDB, err := postgres.NewDB(postgres.Config{
		Host:     pg.Host,
		Port:     port,
		User:     ownerUser,
		Password: ownerPassword,
		Database: pg.Database,
		SSLMode:  pg.SSLMode,
		LogLevel: logLevel,
	})
	if err != nil {
		return fmt.Errorf("open owner connection: %w", err)
	}

	// Schema substrate (CLAUDE.md §24): golang-migrate is the authoritative source
	// (D-178). RunMigrations applies the embedded baseline on the OWNER connection —
	// tables + indexes + FK + tenant_isolation RLS + SECURITY DEFINER org resolvers
	// all live in migrations/0001_baseline.up.sql, replacing the old
	// ApplyPreMigrate + AutoMigrate + ApplyRLSObjects boot path. Idempotent: an
	// already-current DB is a no-op.
	version, applied, err := postgres.RunMigrations(ownerDB)
	if err != nil {
		closeOwnerDB(ownerDB)
		return fmt.Errorf("run migrations: %w", err)
	}
	logger.Info("Database migrations completed: schema_version=%d applied=%t", version, applied)
	closeOwnerDB(ownerDB)

	// APP pool (long-lived). Post-flip these are the NOBYPASSRLS app creds.
	db, err := postgres.NewDB(postgres.Config{
		Host:     pg.Host,
		Port:     port,
		User:     pg.User,
		Password: pg.Password,
		Database: pg.Database,
		SSLMode:  pg.SSLMode,
		LogLevel: logLevel,
	})
	if err != nil {
		return err
	}
	sqlDB, err := db.DB()
	if err != nil {
		return err
	}
	sqlDB.SetMaxOpenConns(pg.Pool.MaxOpenConns)
	sqlDB.SetMaxIdleConns(pg.Pool.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(pg.Pool.MaxLifetime)
	sqlDB.SetConnMaxIdleTime(pg.Pool.MaxIdleTime)
	c.DB = db

	// P4 RLS read-path enforcement (D-071), flag-gated. Registering the Enforce
	// plugin auto-stamps every non-tx statement with the acting org; the boot
	// posture assertion then fails LOUD if enforcement is on while the app pool
	// still connects as a BYPASSRLS/superuser role. Registration BEFORE assertion.
	// Flag off → not registered → behavior-neutral shadow.
	if config.RLSEnforceEnabled() {
		if err := db.Use(tenantdb.Enforce()); err != nil {
			return fmt.Errorf("register RLS enforce plugin: %w", err)
		}
		logger.Info("RLS Enforce plugin registered on app pool (read-path enforcement ON)")
		if err := tenantdb.AssertPosture(db, tenantdb.PostureEnforced); err != nil {
			return fmt.Errorf("RLS boot posture assertion failed: %w", err)
		}
		logger.Info("RLS boot posture verified (app role is RLS-enforced)")
	}

	logger.Info("Database connected and migrated")
	return nil
}

// closeOwnerDB closes the underlying *sql.DB of the short-lived owner gorm
// connection after schema setup, ignoring errors.
func closeOwnerDB(db *gorm.DB) {
	if sqlDB, err := db.DB(); err == nil {
		_ = sqlDB.Close()
	}
}

// initAuth builds the JWKS-backed user-token validator for the HTTP auth
// middleware (D-073). Fail-boot when RLS enforcement is ON and JWKS is empty or
// the validator build fails (a silently auth-off service reverts to the
// cross-tenant leak). When enforcement is OFF an unavailable validator degrades
// to nil (auth disabled, behavior-neutral shadow).
func (c *Container) initAuth() error {
	jwks, err := tenant.NewJWKSValidator(tenant.JWKSConfig{
		JWKSURL: c.Config.Security.Auth.JWKSURL,
		Issuer:  c.Config.Security.Auth.JWTIssuer,
	})
	if config.RLSEnforceEnabled() {
		if c.Config.Security.Auth.JWKSURL == "" {
			return fmt.Errorf("RLS enforcement is on but APP_AUTH_JWKS_URL is empty: refusing to boot without user-JWT validation (D-073)")
		}
		if err != nil {
			return fmt.Errorf("RLS enforcement is on but building the JWKS validator failed: %w (D-073)", err)
		}
	} else if err != nil {
		logger.Warn("JWKS validator unavailable; HTTP JWT auth disabled (RLS enforcement off): %v", err)
		c.JWKSValidator = nil
		return nil
	}
	logger.Info("HTTP JWT auth enabled via JWKS (issuer: %s)", c.Config.Security.Auth.JWTIssuer)
	c.JWKSValidator = jwks
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
	// Org resolver (D-072) — built on the app pool; the SagaOrgResolver for
	// FlowTracker and the OrgResolver for the HTTP by-id handlers. Constructed
	// BEFORE initUseCases so FlowTracker receives it.
	c.TenantResolver = postgres.NewTenantResolverRepo(c.DB)
}

func (c *Container) initUseCases() {
	c.FlowTracker = usecase.NewFlowTracker(c.FlowRepo, c.Publisher, c.TenantResolver)
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
		// Don't fail the service on consumer wiring error — Pulse still serves
		// the REST API for historical flow lookups — but record it so /ready
		// reports NOT ready. Logging alone let this go unnoticed for months.
		c.recordWiringErr("flow", err)
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
		c.recordWiringErr("audit", err)
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
		c.recordWiringErr("alert-activity", err)
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
		c.recordWiringErr("deploy-activity", err)
	} else {
		c.DeployActivityConsumer = deployCons
	}
	return nil
}

// recordWiringErr logs a consumer wiring failure and records it so /ready
// reports NOT ready. Boot continues: an unreachable broker must surface as
// not-ready, never as a crash-loop.
func (c *Container) recordWiringErr(name string, err error) {
	logger.Error("%s consumer not started: %v", name, err)
	c.wiringErrs = append(c.wiringErrs, fmt.Sprintf("%s consumer not wired: %v", name, err))
}

func (c *Container) initHandlers() {
	c.HTTPServer = httphandler.NewServer(
		c.JWKSValidator,
		c.Config.Security.Auth.ServiceAPIKey,
		c.TenantResolver,
		c.FlowTracker,
		c.AuditRecorder,
		c.Readiness,
	)
	c.HTTPServer.SetActivityTrackers(c.AlertTracker, c.DeployTracker)
	// §3 Pulse aggregator — gRPC fan-out to ops + work services.
	// Enabled when at least one OPS_SERVICE_GRPC / WORK_SERVICE_GRPC
	// address is set. Fail-open: aggregator-less pulse still serves
	// flow endpoints.
	cfg := loadAggregatorConfig(c.mtlsSource)
	if cfg.OpsConn != nil || cfg.WorkConn != nil {
		c.Aggregator = usecase.NewAggregator(cfg)
		c.HTTPServer.SetAggregator(c.Aggregator)
		logger.Info("pulse aggregator enabled (ops=%v work=%v)", cfg.OpsConn != nil, cfg.WorkConn != nil)
	}
}

// loadAggregatorConfig dials gRPC connections to ops + work services
// from env-configured addresses (OPS_SERVICE_GRPC / WORK_SERVICE_GRPC,
// e.g. "ops-service:50051"). Missing env = nil conn = signal skipped.
func loadAggregatorConfig(src *workloadapi.X509Source) usecase.AggregatorConfig {
	cfg := usecase.AggregatorConfig{}
	// ops-service + work-service validate x-api-key on inbound gRPC. Attach it
	// (via a dial-time client interceptor) to every outbound call so pulse
	// isn't rejected once these addresses are configured.
	apiKey := os.Getenv("APP_GRPC_SERVICE_API_KEY")
	mode := pkconfig.MTLSMode()
	if addr := os.Getenv("OPS_SERVICE_GRPC"); addr != "" {
		conn, err := grpcclient.Dial(context.Background(), grpcclient.Config{
			Endpoint:      addr,
			Mode:          mode,
			Source:        src,
			ServerService: "ops",
		}, grpc.WithChainUnaryInterceptor(serviceAuthInterceptor(apiKey)))
		if err == nil {
			cfg.OpsConn = conn
		} else {
			logger.Error("pulse aggregator: dial ops %s: %v", addr, err)
		}
	}
	if addr := os.Getenv("WORK_SERVICE_GRPC"); addr != "" {
		conn, err := grpcclient.Dial(context.Background(), grpcclient.Config{
			Endpoint:      addr,
			Mode:          mode,
			Source:        src,
			ServerService: "work",
		}, grpc.WithChainUnaryInterceptor(serviceAuthInterceptor(apiKey)))
		if err == nil {
			cfg.WorkConn = conn
		} else {
			logger.Error("pulse aggregator: dial work %s: %v", addr, err)
		}
	}
	return cfg
}

// Service-to-service gRPC auth headers. ops-service + work-service validate
// x-api-key on inbound calls; x-service-name is attribution only.
const (
	serviceAPIKeyHeader = "x-api-key"
	serviceNameHeader   = "x-service-name"
	pulseServiceName    = "pulse-service"
)

// serviceAuthInterceptor returns a unary client interceptor that attaches the
// shared service API key + pulse's service-name to every outbound call, unless
// the caller already supplied them. An empty apiKey (dev/compose where
// downstream auth is disabled) makes the interceptor a no-op.
func serviceAuthInterceptor(apiKey string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		if apiKey != "" {
			md, _ := metadata.FromOutgoingContext(ctx)
			if md.Get(serviceAPIKeyHeader) == nil {
				ctx = metadata.AppendToOutgoingContext(ctx,
					serviceAPIKeyHeader, apiKey,
					serviceNameHeader, pulseServiceName,
				)
			}
		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
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
	if c.mtlsSource != nil {
		_ = c.mtlsSource.Close()
	}
}
