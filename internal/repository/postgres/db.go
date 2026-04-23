package postgres

import (
	"fmt"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/sentiae/pulse-service/internal/domain"
)

// Config contains the DB connection parameters.
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string
	LogLevel gormlogger.LogLevel
}

// NewDB opens a GORM Postgres connection.
func NewDB(cfg Config) (*gorm.DB, error) {
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.Database, cfg.SSLMode)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: gormlogger.Default.LogMode(cfg.LogLevel),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	return db, nil
}

// AutoMigrate creates / updates the pulse schema.
func AutoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(
		&domain.Flow{},
		&domain.FlowStep{},
		&domain.EventAudit{},
	)
}
