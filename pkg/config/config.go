package config

import (
	"fmt"
	"strings"
	"time"

	pkconfig "github.com/sentiae/platform-kit/config"
)

// Config is the root config object for pulse-service.
type Config struct {
	App       AppConfig       `mapstructure:"app"`
	Logging   LoggingConfig   `mapstructure:"logging"`
	Server    ServerConfig    `mapstructure:"server"`
	Database  DatabaseConfig  `mapstructure:"database"`
	Messaging MessagingConfig `mapstructure:"messaging"`
	Features  FeaturesConfig  `mapstructure:"features"`
}

type AppConfig struct {
	Name        string `mapstructure:"name"`
	Version     string `mapstructure:"version"`
	Environment string `mapstructure:"environment"`
}

type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
	Output string `mapstructure:"output"`
}

type ServerConfig struct {
	HTTP HTTPConfig `mapstructure:"http"`
	GRPC GRPCConfig `mapstructure:"grpc"`
}

type HTTPConfig struct {
	Enabled  bool           `mapstructure:"enabled"`
	Host     string         `mapstructure:"host"`
	Port     string         `mapstructure:"port"`
	BasePath string         `mapstructure:"base_path"`
	Timeouts TimeoutsConfig `mapstructure:"timeouts"`
}

type TimeoutsConfig struct {
	Read     time.Duration `mapstructure:"read"`
	Write    time.Duration `mapstructure:"write"`
	Idle     time.Duration `mapstructure:"idle"`
	Shutdown time.Duration `mapstructure:"shutdown"`
}

type GRPCConfig struct {
	Enabled bool   `mapstructure:"enabled"`
	Host    string `mapstructure:"host"`
	Port    string `mapstructure:"port"`
}

type DatabaseConfig struct {
	Postgres PostgresConfig `mapstructure:"postgres"`
}

type PostgresConfig struct {
	Host     string     `mapstructure:"host"`
	Port     string     `mapstructure:"port"`
	User     string     `mapstructure:"user"`
	Password string     `mapstructure:"password"`
	Database string     `mapstructure:"database"`
	SSLMode  string     `mapstructure:"ssl_mode"`
	Pool     PoolConfig `mapstructure:"pool"`
	LogLevel string     `mapstructure:"log_level"`
}

type PoolConfig struct {
	MaxOpenConns int           `mapstructure:"max_open_conns"`
	MaxIdleConns int           `mapstructure:"max_idle_conns"`
	MaxLifetime  time.Duration `mapstructure:"max_lifetime"`
	MaxIdleTime  time.Duration `mapstructure:"max_idle_time"`
}

type MessagingConfig struct {
	Kafka KafkaConfig `mapstructure:"kafka"`
}

type KafkaConfig struct {
	Enabled  bool              `mapstructure:"enabled"`
	Brokers  string            `mapstructure:"brokers"`
	ClientID string            `mapstructure:"client_id"`
	GroupID  string            `mapstructure:"group_id"`
	Topics   KafkaTopicsConfig `mapstructure:"topics"`
}

type KafkaTopicsConfig struct {
	Prefix      string `mapstructure:"prefix"`
	PulseEvents string `mapstructure:"pulse_events"`
}

type FeaturesConfig struct {
	EventPublishing bool `mapstructure:"event_publishing"`
}

func Load() (*Config, error) {
	var cfg Config
	err := pkconfig.Load(&cfg, pkconfig.Options{
		EnvPrefix:   "APP",
		ConfigPaths: []string{"configs", "."},
		Defaults: map[string]any{
			"app.name":        "pulse-service",
			"app.version":     "dev",
			"app.environment": "development",

			"logging.level":  "info",
			"logging.format": "json",
			"logging.output": "stdout",

			"server.http.enabled":           true,
			"server.http.host":              "0.0.0.0",
			"server.http.port":              "8086",
			"server.http.base_path":         "/api/v1",
			"server.http.timeouts.read":     "15s",
			"server.http.timeouts.write":    "15s",
			"server.http.timeouts.idle":     "60s",
			"server.http.timeouts.shutdown": "30s",

			"server.grpc.enabled": true,
			"server.grpc.host":    "0.0.0.0",
			"server.grpc.port":    "50086",

			"database.postgres.host":                "localhost",
			"database.postgres.port":                "5432",
			"database.postgres.user":                "postgres",
			"database.postgres.password":            "postgres",
			"database.postgres.database":            "pulse_service",
			"database.postgres.ssl_mode":            "disable",
			"database.postgres.pool.max_open_conns": 25,
			"database.postgres.pool.max_idle_conns": 10,
			"database.postgres.pool.max_lifetime":   "5m",
			"database.postgres.pool.max_idle_time":  "10m",
			"database.postgres.log_level":           "warn",

			"messaging.kafka.enabled":             true,
			"messaging.kafka.brokers":             "localhost:9092",
			"messaging.kafka.client_id":           "pulse-service-1",
			"messaging.kafka.group_id":            "pulse-service",
			"messaging.kafka.topics.prefix":       "sentiae",
			"messaging.kafka.topics.pulse_events": "sentiae.pulse.events",

			"features.event_publishing": true,
		},
		BindEnvs: [][2]string{
			{"app.name", "APP_APP_NAME"},
			{"app.environment", "APP_APP_ENVIRONMENT"},
			{"logging.level", "APP_LOGGING_LEVEL"},

			{"server.http.host", "APP_SERVER_HTTP_HOST"},
			{"server.http.port", "APP_SERVER_PORT"},
			{"server.grpc.host", "APP_SERVER_GRPC_HOST"},
			{"server.grpc.port", "APP_GRPC_PORT"},

			{"database.postgres.host", "APP_DATABASE_HOST"},
			{"database.postgres.port", "APP_DATABASE_PORT"},
			{"database.postgres.user", "APP_DATABASE_USER"},
			{"database.postgres.password", "APP_DATABASE_PASSWORD"},
			{"database.postgres.database", "APP_DATABASE_NAME"},
			{"database.postgres.ssl_mode", "APP_DATABASE_SSL_MODE"},

			{"messaging.kafka.enabled", "APP_KAFKA_ENABLED"},
			{"messaging.kafka.brokers", "APP_KAFKA_BROKERS"},
			{"messaging.kafka.client_id", "APP_KAFKA_CLIENT_ID"},
			{"messaging.kafka.group_id", "APP_KAFKA_GROUP_ID"},
			{"messaging.kafka.topics.prefix", "APP_KAFKA_TOPIC_PREFIX"},

			{"features.event_publishing", "APP_FEATURES_EVENT_PUBLISHING"},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("loading config: %w", err)
	}
	return &cfg, nil
}

func (c *Config) GetDatabaseURL() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		c.Database.Postgres.Host, c.Database.Postgres.Port,
		c.Database.Postgres.User, c.Database.Postgres.Password,
		c.Database.Postgres.Database, c.Database.Postgres.SSLMode)
}

func (c *Config) GetKafkaBrokers() []string {
	return strings.Split(c.Messaging.Kafka.Brokers, ",")
}
