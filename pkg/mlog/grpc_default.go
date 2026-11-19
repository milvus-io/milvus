package mlog

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	etcdlogutil "go.etcd.io/etcd/client/pkg/v3/logutil"
	// The client can replace grpclog in init when ETCD_CLIENT_DEBUG is set.
	// Make that initialization a dependency, before installing our forwarder.
	_ "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zapgrpc"
	"google.golang.org/grpc/grpclog"
)

// gRPC exposes neither its current logger nor its environment-based constructor.
// Preserve its startup defaults until SetupLogger supplies the Milvus backend,
// including Fatal exiting instead of mlog's default panic-on-fatal behavior.
func newDefaultGRPCLogger() grpclog.LoggerV2 {
	if debug := os.Getenv("ETCD_CLIENT_DEBUG"); debug != "" {
		// Preserve the early etcd debug logger as well. The client's init has
		// already reported invalid values; don't emit that warning twice.
		level := zapcore.InfoLevel
		if debug != "true" {
			if err := level.Set(debug); err != nil {
				level = zapcore.InfoLevel
			}
		}
		logger, err := etcdlogutil.CreateDefaultZapLogger(level)
		if err != nil {
			panic(err)
		}
		return zapgrpc.NewLogger(logger.Named("etcd-client"))
	}
	info, warning, errOutput := io.Discard, io.Discard, io.Discard
	severity := 4 // Unrecognized severity disables all output, including Fatal.
	switch os.Getenv("GRPC_GO_LOG_SEVERITY_LEVEL") {
	case "", "ERROR", "error":
		errOutput, severity = os.Stderr, 2
	case "WARNING", "warning":
		warning, severity = os.Stderr, 1
	case "INFO", "info":
		info, severity = os.Stderr, 0
	}
	var verbosity int
	if value, err := strconv.Atoi(os.Getenv("GRPC_GO_LOG_VERBOSITY_LEVEL")); err == nil {
		verbosity = value
	}
	logger := grpclog.NewLoggerV2WithVerbosity(info, warning, errOutput, verbosity)
	if strings.EqualFold(os.Getenv("GRPC_GO_LOG_FORMATTER"), "json") {
		return &defaultGRPCJSONLogger{LoggerV2: logger, severity: severity}
	}
	return logger
}

// The public gRPC constructor only supports text. Keep its JSON startup format
// too, without depending on gRPC's internal logger globals or private APIs.
type defaultGRPCJSONLogger struct {
	grpclog.LoggerV2 // Supplies gRPC's default verbosity semantics.
	severity         int
}

func (l *defaultGRPCJSONLogger) output(severity int, message func() string) {
	if severity < l.severity {
		return
	}
	data, _ := json.Marshal(map[string]string{
		"severity": [...]string{"INFO", "WARNING", "ERROR", "FATAL"}[severity],
		"message":  message(),
	})
	_, _ = os.Stderr.Write(append(data, '\n'))
}

func (l *defaultGRPCJSONLogger) Info(args ...any) {
	l.output(0, func() string { return fmt.Sprint(args...) })
}

func (l *defaultGRPCJSONLogger) Infoln(args ...any) {
	l.output(0, func() string { return fmt.Sprintln(args...) })
}

func (l *defaultGRPCJSONLogger) Infof(format string, args ...any) {
	l.output(0, func() string { return fmt.Sprintf(format, args...) })
}

func (l *defaultGRPCJSONLogger) Warning(args ...any) {
	l.output(1, func() string { return fmt.Sprint(args...) })
}

func (l *defaultGRPCJSONLogger) Warningln(args ...any) {
	l.output(1, func() string { return fmt.Sprintln(args...) })
}

func (l *defaultGRPCJSONLogger) Warningf(format string, args ...any) {
	l.output(1, func() string { return fmt.Sprintf(format, args...) })
}

func (l *defaultGRPCJSONLogger) Error(args ...any) {
	l.output(2, func() string { return fmt.Sprint(args...) })
}

func (l *defaultGRPCJSONLogger) Errorln(args ...any) {
	l.output(2, func() string { return fmt.Sprintln(args...) })
}

func (l *defaultGRPCJSONLogger) Errorf(format string, args ...any) {
	l.output(2, func() string { return fmt.Sprintf(format, args...) })
}

func (l *defaultGRPCJSONLogger) Fatal(args ...any) {
	l.output(3, func() string { return fmt.Sprint(args...) })
	os.Exit(1)
}

func (l *defaultGRPCJSONLogger) Fatalln(args ...any) {
	l.output(3, func() string { return fmt.Sprintln(args...) })
	os.Exit(1)
}

func (l *defaultGRPCJSONLogger) Fatalf(format string, args ...any) {
	l.output(3, func() string { return fmt.Sprintf(format, args...) })
	os.Exit(1)
}
