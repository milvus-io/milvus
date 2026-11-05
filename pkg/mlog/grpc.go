package mlog

import (
	"strings"
	"sync/atomic"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zapgrpc"
	"google.golang.org/grpc/grpclog"
)

// grpcLogger is a stable grpclog.LoggerV2 implementation. It is installed once
// during package initialization and its backend is swapped atomically when the
// full log configuration becomes available.
//
// grpclog.SetLoggerV2 is explicitly documented as not mutex protected and must
// be called before any gRPC function. Calling it again later races with gRPC
// background goroutines that read the global logger. Keeping the installed
// logger stable and replacing only its backend avoids that race.
type grpcLogger struct {
	backend atomic.Pointer[grpcLoggerBackend]
}

type grpcLoggerBackend struct {
	logger grpclog.LoggerV2
}

var globalGRPCLogger = &grpcLogger{}

func (l *grpcLogger) set(logger *zap.Logger, level string) {
	// The forwarding methods add one stack frame on top of the zap adapter,
	// so skip it to preserve gRPC caller depth.
	backend := NewGRPCLogger(logger.WithOptions(zap.AddCallerSkip(1)), level)
	l.backend.Store(&grpcLoggerBackend{logger: backend})
}

func (l *grpcLogger) load() grpclog.LoggerV2 {
	// initGRPCLogger installs a backend before publishing this logger to gRPC.
	return l.backend.Load().logger
}

// Info implements grpclog.LoggerV2.
func (l *grpcLogger) Info(args ...any) { l.load().Info(args...) }

// Infoln implements grpclog.LoggerV2.
func (l *grpcLogger) Infoln(args ...any) { l.load().Infoln(args...) }

// Infof implements grpclog.LoggerV2.
func (l *grpcLogger) Infof(format string, args ...any) { l.load().Infof(format, args...) }

// Warning implements grpclog.LoggerV2.
func (l *grpcLogger) Warning(args ...any) { l.load().Warning(args...) }

// Warningln implements grpclog.LoggerV2.
func (l *grpcLogger) Warningln(args ...any) { l.load().Warningln(args...) }

// Warningf implements grpclog.LoggerV2.
func (l *grpcLogger) Warningf(format string, args ...any) { l.load().Warningf(format, args...) }

// Error implements grpclog.LoggerV2.
func (l *grpcLogger) Error(args ...any) { l.load().Error(args...) }

// Errorln implements grpclog.LoggerV2.
func (l *grpcLogger) Errorln(args ...any) { l.load().Errorln(args...) }

// Errorf implements grpclog.LoggerV2.
func (l *grpcLogger) Errorf(format string, args ...any) { l.load().Errorf(format, args...) }

// Fatal implements grpclog.LoggerV2.
func (l *grpcLogger) Fatal(args ...any) { l.load().Fatal(args...) }

// Fatalln implements grpclog.LoggerV2.
func (l *grpcLogger) Fatalln(args ...any) { l.load().Fatalln(args...) }

// Fatalf implements grpclog.LoggerV2.
func (l *grpcLogger) Fatalf(format string, args ...any) { l.load().Fatalf(format, args...) }

// V implements grpclog.LoggerV2.
func (l *grpcLogger) V(level int) bool { return l.load().V(level) }

// NewGRPCLogger adapts a zap logger to gRPC's logger interface.
func NewGRPCLogger(logger *zap.Logger, level string) grpclog.LoggerV2 {
	grpcLevel := grpcLogLevel(level)
	if logger.Core().Enabled(grpcLevel) {
		logger = logger.WithOptions(zap.IncreaseLevel(grpcLevel))
	}
	return zapgrpc.NewLogger(logger)
}

// initGRPCLogger installs the stable forwarding logger. It must be called
// during mlog initialization, before any gRPC user starts.
func initGRPCLogger(logger *zap.Logger) {
	// The bootstrap logger uses WriteThenPanic for mlog's own fatal logs, but
	// gRPC fatal logs must keep the default process-exit behavior.
	globalGRPCLogger.set(logger.WithOptions(zap.OnFatal(zapcore.WriteThenFatal)), "WARNING")
	grpclog.SetLoggerV2(globalGRPCLogger)
}

// ReplaceGRPCLogger atomically replaces the logger used by gRPC. It is safe to
// call while other goroutines are logging through gRPC.
func ReplaceGRPCLogger(logger *zap.Logger, level string) {
	globalGRPCLogger.set(logger, level)
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
