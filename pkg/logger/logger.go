package logger

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

var defaultLogger *slog.Logger

func Init(level string) {
	var logLevel slog.Level
	switch strings.ToLower(level) {
	case "debug":
		logLevel = slog.LevelDebug
	case "warn", "warning":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel})
	defaultLogger = slog.New(handler)
	slog.SetDefault(defaultLogger)
}

func Info(msg string, args ...any)  { slog.Info(fmt.Sprintf(msg, args...)) }
func Error(msg string, args ...any) { slog.Error(fmt.Sprintf(msg, args...)) }
func Warn(msg string, args ...any)  { slog.Warn(fmt.Sprintf(msg, args...)) }
func Debug(msg string, args ...any) { slog.Debug(fmt.Sprintf(msg, args...)) }

func Fatal(msg string, args ...any) {
	slog.Error(fmt.Sprintf(msg, args...))
	os.Exit(1)
}
