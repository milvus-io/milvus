package pulsarlog

import (
	"fmt"

	plog "github.com/apache/pulsar-client-go/pulsar/log"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

var _ plog.Logger = (*logger)(nil)

// NewLogger creates a new pulsar logger.
func NewLogger() plog.Logger {
	return &logger{log.With(zap.String("component", "pulsar"))}
}

type logger struct {
	inner *log.MLogger
}

func (l *logger) SubLogger(fields plog.Fields) plog.Logger {
	return &logger{l.inner.With(exportFields(fields)...)}
}

func (l *logger) WithFields(fields plog.Fields) plog.Entry {
	return &logger{l.inner.With(exportFields(fields)...)}
}

func (l *logger) WithField(name string, value interface{}) plog.Entry {
	fs := exportFields(plog.Fields{name: value})
	return &logger{l.inner.With(fs...)}
}

func (l *logger) WithError(err error) plog.Entry {
	return &logger{l.inner.With(zap.Error(err))}
}

func (l *logger) Debug(args ...interface{}) {
	l.inner.Debug(tryDetermineMessage(args...))
}

func (l *logger) Info(args ...interface{}) {
	l.inner.Info(tryDetermineMessage(args...))
}

func (l *logger) Warn(args ...interface{}) {
	l.inner.Warn(tryDetermineMessage(args...))
}

func (l *logger) Error(args ...interface{}) {
	l.inner.Error(tryDetermineMessage(args...))
}

func (l *logger) Debugf(format string, args ...interface{}) {
	l.inner.Debug(fmt.Sprintf(format, args...))
}

func (l *logger) Infof(format string, args ...interface{}) {
	l.inner.Info(fmt.Sprintf(format, args...))
}

func (l *logger) Warnf(format string, args ...interface{}) {
	l.inner.Warn(fmt.Sprintf(format, args...))
}

func (l *logger) Errorf(format string, args ...interface{}) {
	l.inner.Error(fmt.Sprintf(format, args...))
}

func tryDetermineMessage(value ...interface{}) string {
	return fmt.Sprint(value...)
}

func exportFields(fields plog.Fields) []zap.Field {
	fs := make([]zap.Field, 0, 2*len(fields))
	for k, v := range fields {
		switch v := v.(type) {
		case string:
			fs = append(fs, zap.String(k, v))
		case int:
			fs = append(fs, zap.Int(k, v))
		case bool:
			fs = append(fs, zap.Bool(k, v))
		case float64:
			fs = append(fs, zap.Float64(k, v))
		case []byte:
			fs = append(fs, zap.Binary(k, v))
		case error:
			fs = append(fs, zap.Error(v))
		default:
			fs = append(fs, zap.Any(k, v))
		}
	}
	return fs
}
