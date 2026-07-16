package driverapp

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"unicode/utf8"

	buildversion "github.com/urlab-ai/scaleway-file-storage-subdir-csi/internal/version"
	"github.com/urlab-ai/scaleway-file-storage-subdir-csi/pkg/config"
)

const maxStructuredLogValueBytes = 2048

// configureRuntimeLogging installs the process-wide JSON logger before any
// component dependency is initialized. The handler bounds and flattens every
// string and error so untrusted provider, CSI, path, and Kubernetes values
// cannot forge records or create unbounded log entries.
func configureRuntimeLogging(level string, component config.Component, driverName string) (*slog.Logger, error) {
	var configuredLevel slog.Level
	switch level {
	case "debug":
		configuredLevel = slog.LevelDebug
	case "info":
		configuredLevel = slog.LevelInfo
	case "warn":
		configuredLevel = slog.LevelWarn
	case "error":
		configuredLevel = slog.LevelError
	default:
		return nil, fmt.Errorf("runtime log level %q is unsupported", level)
	}
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: configuredLevel,
		ReplaceAttr: func(_ []string, attribute slog.Attr) slog.Attr {
			switch attribute.Value.Kind() {
			case slog.KindString:
				attribute.Value = slog.StringValue(boundedLogValue(attribute.Value.String()))
			case slog.KindAny:
				if err, ok := attribute.Value.Any().(error); ok {
					attribute.Value = slog.StringValue(boundedLogValue(err.Error()))
				}
			}
			return attribute
		},
	})
	logger := slog.New(handler).With(
		"component", component,
		"driver_name", driverName,
		"version", buildversion.Version,
	)
	slog.SetDefault(logger)
	return logger, nil
}

func boundedLogValue(value string) string {
	value = strings.NewReplacer("\x00", " ", "\r", " ", "\n", " ").Replace(strings.ToValidUTF8(value, "?"))
	if len(value) <= maxStructuredLogValueBytes {
		return value
	}
	value = value[:maxStructuredLogValueBytes]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

func logRuntimeCompletion(ctx context.Context, logger *slog.Logger, err error) {
	if err != nil {
		logger.ErrorContext(ctx, "driver component stopped with error", "error", err)
		return
	}
	logger.InfoContext(ctx, "driver component stopped")
}
