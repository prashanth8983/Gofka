package logger

import (
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	// Global logger instance
	log *zap.Logger

	// Sugar logger for convenience
	sugar *zap.SugaredLogger
)

// Config represents logger configuration
type Config struct {
	Level      string `json:"level"`       // debug, info, warn, error
	Encoding   string `json:"encoding"`    // json or console
	OutputPath string `json:"output_path"` // stdout, stderr, or file path
	Component  string `json:"component"`   // broker, controller, etc.
}

// Init initializes the logger with the given configuration
func Init(cfg Config) error {
	// Parse log level
	level := zapcore.InfoLevel
	switch cfg.Level {
	case "debug":
		level = zapcore.DebugLevel
	case "info":
		level = zapcore.InfoLevel
	case "warn":
		level = zapcore.WarnLevel
	case "error":
		level = zapcore.ErrorLevel
	default:
		level = zapcore.InfoLevel
	}

	// Create encoder config
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "timestamp",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "message",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	// Create encoder
	var encoder zapcore.Encoder
	if cfg.Encoding == "json" {
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	} else {
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	}

	// Create writer
	var writer zapcore.WriteSyncer
	switch cfg.OutputPath {
	case "stdout":
		writer = zapcore.AddSync(os.Stdout)
	case "stderr":
		writer = zapcore.AddSync(os.Stderr)
	default:
		file, err := os.OpenFile(cfg.OutputPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("failed to open log file: %w", err)
		}
		writer = zapcore.AddSync(file)
	}

	// Create core
	core := zapcore.NewCore(encoder, writer, level)

	// Create logger
	log = zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))

	// Add component field
	if cfg.Component != "" {
		log = log.With(zap.String("component", cfg.Component))
	}

	sugar = log.Sugar()

	return nil
}

// InitDefault initializes the logger with default configuration
func InitDefault(component string) {
	Init(Config{
		Level:      "info",
		Encoding:   "console",
		OutputPath: "stdout",
		Component:  component,
	})
}

// Get returns the global logger instance
func Get() *zap.Logger {
	if log == nil {
		InitDefault("gofka")
	}
	return log
}

// Sugar returns the sugared logger instance
func Sugar() *zap.SugaredLogger {
	if sugar == nil {
		InitDefault("gofka")
	}
	return sugar
}

// Debug logs a debug message
func Debug(msg string, fields ...zap.Field) {
	Get().Debug(msg, fields...)
}

// Info logs an info message
func Info(msg string, fields ...zap.Field) {
	Get().Info(msg, fields...)
}

// Warn logs a warning message
func Warn(msg string, fields ...zap.Field) {
	Get().Warn(msg, fields...)
}

// Error logs an error message
func Error(msg string, fields ...zap.Field) {
	Get().Error(msg, fields...)
}

// Fatal logs a fatal message and exits
func Fatal(msg string, fields ...zap.Field) {
	Get().Fatal(msg, fields...)
}

// With creates a child logger with additional fields
func With(fields ...zap.Field) *zap.Logger {
	return Get().With(fields...)
}

// Structured logging helpers

// LogRequest logs an incoming request
func LogRequest(apiKey string, clientID string, correlationID int32) {
	Info("request received",
		zap.String("api_key", apiKey),
		zap.String("client_id", clientID),
		zap.Int32("correlation_id", correlationID),
	)
}

// LogResponse logs an outgoing response
func LogResponse(apiKey string, correlationID int32, duration time.Duration, err error) {
	if err != nil {
		Error("request failed",
			zap.String("api_key", apiKey),
			zap.Int32("correlation_id", correlationID),
			zap.Duration("duration", duration),
			zap.Error(err),
		)
	} else {
		Info("request completed",
			zap.String("api_key", apiKey),
			zap.Int32("correlation_id", correlationID),
			zap.Duration("duration", duration),
		)
	}
}

// LogProducedMessage logs a produced message
func LogProducedMessage(topic string, partition int32, offset int64, size int) {
	Debug("message produced",
		zap.String("topic", topic),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset),
		zap.Int("size", size),
	)
}

// LogConsumedMessage logs a consumed message
func LogConsumedMessage(topic string, partition int32, offset int64, group string) {
	Debug("message consumed",
		zap.String("topic", topic),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset),
		zap.String("group", group),
	)
}

// LogConsumerGroupEvent logs consumer group events
func LogConsumerGroupEvent(event string, group string, memberID string, generation int32) {
	Info("consumer group event",
		zap.String("event", event),
		zap.String("group", group),
		zap.String("member_id", memberID),
		zap.Int32("generation", generation),
	)
}

// LogRaftStateChange logs Raft state changes
func LogRaftStateChange(nodeID string, oldState, newState string, term uint64) {
	Info("raft state change",
		zap.String("node_id", nodeID),
		zap.String("old_state", oldState),
		zap.String("new_state", newState),
		zap.Uint64("term", term),
	)
}

// LogStorageEvent logs storage events
func LogStorageEvent(event string, topic string, partition int32, details map[string]interface{}) {
	fields := []zap.Field{
		zap.String("event", event),
		zap.String("topic", topic),
		zap.Int32("partition", partition),
	}

	for k, v := range details {
		fields = append(fields, zap.Any(k, v))
	}

	Info("storage event", fields...)
}