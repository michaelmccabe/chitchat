package logger

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestInitLogger_LevelsAndFormats(t *testing.T) {
	tests := []struct {
		name     string
		level    string
		format   string
		logFunc  func(l *slog.Logger)
		expected string
	}{
		{
			name:   "Info text format",
			level:  "info",
			format: "text",
			logFunc: func(l *slog.Logger) {
				l.Info("test info message", "key", "val")
			},
			expected: "level=INFO msg=\"test info message\" key=val",
		},
		{
			name:   "Debug text format",
			level:  "debug",
			format: "text",
			logFunc: func(l *slog.Logger) {
				l.Debug("test debug message", "num", 42)
			},
			expected: "level=DEBUG msg=\"test debug message\" num=42",
		},
		{
			name:   "JSON format",
			level:  "info",
			format: "json",
			logFunc: func(l *slog.Logger) {
				l.Info("json message", "status", "ok")
			},
			expected: `"msg":"json message","status":"ok"`,
		},
		{
			name:   "Warn level filtering",
			level:  "warn",
			format: "text",
			logFunc: func(l *slog.Logger) {
				l.Info("should be ignored")
				l.Warn("warning message")
			},
			expected: "level=WARN msg=\"warning message\"",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := InitLogger(tc.level, tc.format, &buf)
			tc.logFunc(logger)

			output := buf.String()
			if !strings.Contains(output, tc.expected) {
				t.Errorf("expected log output to contain %q, got %q", tc.expected, output)
			}
		})
	}
}
