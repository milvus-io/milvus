package mlog

import (
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zapgrpc"
	"google.golang.org/grpc/grpclog"
)

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
