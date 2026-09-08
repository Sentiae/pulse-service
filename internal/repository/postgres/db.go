package postgres

import (
	"fmt"
	"os"

	"github.com/sentiae/platform-kit/gormlog"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Config contains the DB connection parameters.
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string
	// LogLevel is a gormlog level name ("silent", "error", "warn", "info"),
	// not a gorm enum: the ORM logger is built by gormlog.New, which parses the
	// name itself and refuses an unrecognised one. gorm's own logger type
	// cannot be named here because importing it is what D-400 forbids.
	LogLevel string
}

// NewDB opens a GORM Postgres connection.
func NewDB(cfg Config) (*gorm.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database, cfg.SSLMode)
	// The prior value was gormlogger.Default.LogMode(cfg.LogLevel) — gorm's own
	// logger, which leaves Config.ParameterizedQueries unset and therefore
	// inlines every bound value into the SQL it writes. Warn is not safe:
	// (*logger).Trace guards its error branch on >= Error and its slow branch on
	// >= Warn, so at Warn both fire and both Explain with the values rendered
	// in. gormlog.New sets the flag, asserts gorm.ParamsFilter before returning,
	// and takes over gorm's RecorderParamsFilter global so the (*gorm.DB).Scan
	// bypass is closed too (D-400).
	gormLogger, err := gormlog.New(os.Stdout, cfg.LogLevel)
	if err != nil {
		return nil, fmt.Errorf("build gorm logger: %w", err)
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: gormLogger,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	return db, nil
}
