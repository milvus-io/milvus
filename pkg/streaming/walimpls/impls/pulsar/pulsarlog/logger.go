package pulsarlog

import (
	"context"

	plog "github.com/apache/pulsar-client-go/pulsar/log"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

var _ plog.Logger = (*logger)(nil)

// NewLogger creates a new pulsar logger.
// TODO: currently, pulsar client will log a huge message when logging,
// so we only log the first msg without format the log.
func NewLogger() plog.Logger {
	return &logger{mlog.With(mlog.FieldComponent("pulsar"))}
}

type logger struct {
	inner *mlog.Logger
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
	return &logger{l.inner.With(mlog.Err(err))}
}

func (l *logger) Debug(args ...interface{}) {
	l.logWithLevel(zap.DebugLevel, args...)
}

func (l *logger) Info(args ...interface{}) {
	l.logWithLevel(zap.InfoLevel, args...)
}

func (l *logger) Warn(args ...interface{}) {
	l.logWithLevel(zap.WarnLevel, args...)
}

func (l *logger) Error(args ...interface{}) {
	l.logWithLevel(zap.ErrorLevel, args...)
}

func (l *logger) Debugf(format string, args ...interface{}) {
	l.logWithLevel(zap.DebugLevel, format)
}

func (l *logger) Infof(format string, args ...interface{}) {
	l.logWithLevel(zap.InfoLevel, format)
}

func (l *logger) Warnf(format string, args ...interface{}) {
	l.logWithLevel(zap.WarnLevel, format)
}

func (l *logger) Errorf(format string, args ...interface{}) {
	l.logWithLevel(zap.ErrorLevel, format)
}

func (l *logger) logWithLevel(level zapcore.Level, args ...interface{}) {
	if len(args) == 0 {
		return
	}
	if msg, ok := args[0].(string); ok {
		l.inner.WithOptions(zap.AddCallerSkip(2)).Log(context.TODO(), level, msg)
	} else {
		l.inner.WithOptions(zap.AddCallerSkip(2)).Log(context.TODO(), level, "unknown log message type")
	}
}

func exportFields(fields plog.Fields) []mlog.Field {
	fs := make([]mlog.Field, 0, 2*len(fields))
	for k, v := range fields {
		switch v := v.(type) {
		case string:
			fs = append(fs, mlog.String(k, v))
		case int:
			fs = append(fs, mlog.Int(k, v))
		case bool:
			fs = append(fs, mlog.Bool(k, v))
		case float64:
			fs = append(fs, mlog.Float64(k, v))
		case []byte:
			fs = append(fs, mlog.Binary(k, v))
		case error:
			fs = append(fs, mlog.Err(v))
		default:
			fs = append(fs, mlog.Any(k, v))
		}
	}
	return fs
}
