package mlog

import (
	"strings"
	"sync/atomic"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zapgrpc"
	"google.golang.org/grpc/grpclog"
)

// grpcLogger is installed once, before packages importing mlog can create gRPC
// clients (in particular, the remote configuration client's etcd connection).
// SetLoggerV2 itself is not safe to call while gRPC goroutines are running.
var grpcLogger = &grpcLoggerForwarder{}

type grpcLoggerBackend struct {
	grpclog.LoggerV2
}

type grpcLoggerForwarder struct {
	backend atomic.Pointer[grpcLoggerBackend]
}

func init() {
	grpcLogger.backend.Store(&grpcLoggerBackend{newDefaultGRPCLogger()})
	grpclog.SetLoggerV2(grpcLogger)
}

// ReplaceGRPCLogger updates the backend without replacing gRPC's global logger.
func ReplaceGRPCLogger(logger *zap.Logger, level string) {
	// Compensate for the forwarding method below. Keep NewGRPCLogger's existing
	// caller and gRPC Depth logging behavior (zapgrpc does not implement Depth).
	backend := NewGRPCLogger(logger.WithOptions(zap.AddCallerSkip(1)), level)
	grpcLogger.backend.Store(&grpcLoggerBackend{backend})
}

func (l *grpcLoggerForwarder) Info(args ...any)   { l.backend.Load().Info(args...) }
func (l *grpcLoggerForwarder) Infoln(args ...any) { l.backend.Load().Infoln(args...) }
func (l *grpcLoggerForwarder) Infof(format string, args ...any) {
	l.backend.Load().Infof(format, args...)
}
func (l *grpcLoggerForwarder) Warning(args ...any)   { l.backend.Load().Warning(args...) }
func (l *grpcLoggerForwarder) Warningln(args ...any) { l.backend.Load().Warningln(args...) }
func (l *grpcLoggerForwarder) Warningf(format string, args ...any) {
	l.backend.Load().Warningf(format, args...)
}
func (l *grpcLoggerForwarder) Error(args ...any)   { l.backend.Load().Error(args...) }
func (l *grpcLoggerForwarder) Errorln(args ...any) { l.backend.Load().Errorln(args...) }
func (l *grpcLoggerForwarder) Errorf(format string, args ...any) {
	l.backend.Load().Errorf(format, args...)
}
func (l *grpcLoggerForwarder) Fatal(args ...any)   { l.backend.Load().Fatal(args...) }
func (l *grpcLoggerForwarder) Fatalln(args ...any) { l.backend.Load().Fatalln(args...) }
func (l *grpcLoggerForwarder) Fatalf(format string, args ...any) {
	l.backend.Load().Fatalf(format, args...)
}
func (l *grpcLoggerForwarder) V(level int) bool { return l.backend.Load().V(level) }

// NewGRPCLogger adapts a zap logger to gRPC's logger interface.
func NewGRPCLogger(logger *zap.Logger, level string) grpclog.LoggerV2 {
	grpcLevel := grpcLogLevel(level)
	if logger.Core().Enabled(grpcLevel) {
		logger = logger.WithOptions(zap.IncreaseLevel(grpcLevel))
	}
	return zapgrpc.NewLogger(logger)
}

func grpcLogLevel(level string) zapcore.Level {
	switch {
	case strings.EqualFold(level, "INFO"):
		return zapcore.InfoLevel
	case strings.EqualFold(level, "WARNING"):
		return zapcore.WarnLevel
	default:
		return zapcore.ErrorLevel
	}
}
