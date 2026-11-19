package mlog

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/grpc/grpclog"
)

func grpcTestCommand(t *testing.T, name string) *exec.Cmd {
	t.Helper()
	executable, err := os.Executable()
	require.NoError(t, err)
	args := []string{"-test.run=^" + name + "$", "-test.count=1"}
	if dir := flag.Lookup("test.gocoverdir"); dir != nil && dir.Value.String() != "" {
		args = append(args, "-test.gocoverdir="+dir.Value.String())
	}
	return exec.Command(executable, args...) // #nosec G204 -- Re-execute this test binary with fixed test flags.
}

type grpcFormatProbe struct {
	calls *int
}

func (p grpcFormatProbe) String() string {
	*p.calls++
	return "probe"
}

func TestDefaultGRPCLogger(t *testing.T) {
	t.Setenv("ETCD_CLIENT_DEBUG", "")
	t.Setenv("GRPC_GO_LOG_SEVERITY_LEVEL", "NONE")
	for _, format := range []string{"", "json"} {
		t.Run(format, func(t *testing.T) {
			t.Setenv("GRPC_GO_LOG_FORMATTER", format)
			for _, verbosity := range []string{"", "invalid", "999999999999999999999999999999", "2"} {
				t.Setenv("GRPC_GO_LOG_VERBOSITY_LEVEL", verbosity)
				logger := newDefaultGRPCLogger()
				require.True(t, logger.V(0))
				require.Equal(t, verbosity == "2", logger.V(2))
				require.False(t, logger.V(3))
				var calls int
				probe := grpcFormatProbe{calls: &calls}
				logger.Info(probe)
				logger.Infoln(probe)
				logger.Infof("%s", probe)
				logger.Warning(probe)
				logger.Warningln(probe)
				logger.Warningf("%s", probe)
				logger.Error(probe)
				logger.Errorln(probe)
				logger.Errorf("%s", probe)
				require.Zero(t, calls, "disabled startup logs must not format arguments")
			}
		})
	}
}

func TestDefaultGRPCLoggerEtcdDebug(t *testing.T) {
	for _, debug := range []string{"true", "debug", "warn", "invalid"} {
		t.Run(debug, func(t *testing.T) {
			t.Setenv("ETCD_CLIENT_DEBUG", debug)
			logger := newDefaultGRPCLogger()
			require.Equal(t, debug != "warn", logger.V(0))
			require.True(t, logger.V(2))
		})
	}
}

func TestGRPCLoggerConcurrentReplace(t *testing.T) {
	original := grpcLogger.backend.Load()
	t.Cleanup(func() { grpcLogger.backend.Store(original) })
	ReplaceGRPCLogger(zap.NewNop(), "INFO")
	var readers sync.WaitGroup
	stop := make(chan struct{})
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-stop:
				return
			default:
				grpclog.Info("concurrent backend update")
				grpclog.Warningf("update %d", 1)
				grpclog.Errorln("update")
				grpclog.InfoDepth(2, "update")
				grpclog.WarningDepth(2, "update")
				grpclog.ErrorDepth(2, "update")
				grpclog.V(1)
			}
		}
	}()
	for i := 0; i < 100; i++ {
		ReplaceGRPCLogger(zap.NewNop(), "WARNING")
		runtime.Gosched()
	}
	close(stop)
	readers.Wait()
}

func TestGRPCLoggerStartup(t *testing.T) {
	if os.Getenv("MILVUS_TEST_GRPC_STARTUP") == "1" {
		require.True(t, grpclog.V(2))
		require.False(t, grpclog.V(3))
		grpclog.Info("info", 1)
		grpclog.Infoln("info", 2)
		grpclog.Infof("info %d", 3)
		grpclog.Warning("warning", 1)
		grpclog.Warningln("warning", 2)
		grpclog.Warningf("warning %d", 3)
		grpclog.Error("error", 1)
		grpclog.Errorln("error", 2)
		grpclog.Errorf("error %d", 3)
		logger, _, err := InitLogger(&Config{Level: "info", Stdout: true})
		require.NoError(t, err)
		ReplaceGRPCLogger(logger, "INFO")
		grpclog.Info("configured backend")
		return
	}
	for _, severity := range []string{"", "ERROR", "warning", "info", "NONE"} {
		for _, format := range []string{"", "JsOn"} {
			t.Run(severity+"/"+format, func(t *testing.T) {
				cmd := grpcTestCommand(t, "TestGRPCLoggerStartup")
				cmd.Env = append(os.Environ(), "MILVUS_TEST_GRPC_STARTUP=1",
					"GRPC_GO_LOG_SEVERITY_LEVEL="+severity, "GRPC_GO_LOG_VERBOSITY_LEVEL=2", "GRPC_GO_LOG_FORMATTER="+format)
				var stdout, stderr bytes.Buffer
				cmd.Stdout, cmd.Stderr = &stdout, &stderr
				require.NoError(t, cmd.Run(), "%s\n%s", &stdout, &stderr)
				require.Contains(t, stdout.String(), "configured backend")
				require.NotContains(t, stderr.String(), "configured backend")
				if severity == "NONE" {
					require.Empty(t, stderr.String())
					return
				}
				lines := strings.Split(strings.TrimSpace(stderr.String()), "\n")
				expectedCount := map[string]int{"": 3, "ERROR": 3, "warning": 6, "info": 9}[severity]
				require.Len(t, lines, expectedCount)
				if format != "" {
					for _, line := range lines {
						var entry map[string]string
						require.NoError(t, json.Unmarshal([]byte(line), &entry))
						require.Len(t, entry, 2)
						require.Contains(t, []string{"INFO", "WARNING", "ERROR"}, entry["severity"])
						require.NotEmpty(t, entry["message"])
					}
				}
			})
		}
	}
}

func TestGRPCLoggerFatal(t *testing.T) {
	if method := os.Getenv("MILVUS_TEST_GRPC_FATAL"); method != "" {
		if os.Getenv("MILVUS_TEST_GRPC_CONFIGURED") == "1" {
			logger, _, err := InitLogger(&Config{Level: "info", Stdout: true})
			require.NoError(t, err)
			ReplaceGRPCLogger(logger, "ERROR")
		}
		switch method {
		case "Fatal":
			grpclog.Fatal("fatal marker")
		case "Fatalln":
			grpclog.Fatalln("fatal marker")
		case "Fatalf":
			grpclog.Fatalf("fatal %s", "marker")
		case "FatalDepth":
			grpclog.FatalDepth(2, "fatal marker")
		}
		t.Fatal("Fatal returned")
	}
	for _, method := range []string{"Fatal", "Fatalln", "Fatalf", "FatalDepth"} {
		for _, configured := range []string{"0", "1"} {
			t.Run(method+"/"+configured, func(t *testing.T) {
				cmd := grpcTestCommand(t, "TestGRPCLoggerFatal")
				cmd.Env = append(os.Environ(), "MILVUS_TEST_GRPC_FATAL="+method,
					"MILVUS_TEST_GRPC_CONFIGURED="+configured, "GRPC_GO_LOG_SEVERITY_LEVEL=ERROR", "GRPC_GO_LOG_FORMATTER=json")
				out, err := cmd.CombinedOutput()
				var exitErr *exec.ExitError
				require.ErrorAs(t, err, &exitErr, "%s", out)
				require.Equal(t, 1, exitErr.ExitCode(), "%s", out)
				require.Contains(t, string(out), "fatal marker")
			})
		}
	}
}

func TestGRPCLoggerForwarding(t *testing.T) {
	original := grpcLogger.backend.Load()
	t.Cleanup(func() { grpcLogger.backend.Store(original) })
	for _, level := range []string{"INFO", "info", "WARNING", "warning", "ERROR", "", "unknown"} {
		t.Run(level, func(t *testing.T) {
			atomicLevel := zap.NewAtomicLevelAt(zap.DebugLevel)
			core, entries := observer.New(atomicLevel)
			logger := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(3), zap.OnFatal(zapcore.WriteThenPanic))
			direct := NewGRPCLogger(logger, level)
			ReplaceGRPCLogger(logger, level)
			// Skip the zapgrpc/printer and test callback frames so all methods
			// identify the call site below, including zapgrpc's Fatal wrappers.
			for i, call := range []func(grpclog.LoggerV2){
				func(l grpclog.LoggerV2) { l.Info("info", 1) },
				func(l grpclog.LoggerV2) { l.Infoln("info", 1) },
				func(l grpclog.LoggerV2) { l.Infof("info %d", 1) },
				func(l grpclog.LoggerV2) { l.Warning("warning", 1) },
				func(l grpclog.LoggerV2) { l.Warningln("warning", 1) },
				func(l grpclog.LoggerV2) { l.Warningf("warning %d", 1) },
				func(l grpclog.LoggerV2) { l.Error("error", 1) },
				func(l grpclog.LoggerV2) { l.Errorln("error", 1) },
				func(l grpclog.LoggerV2) { l.Errorf("error %d", 1) },
				func(l grpclog.LoggerV2) { l.Fatal("fatal", 1) },
				func(l grpclog.LoggerV2) { l.Fatalln("fatal", 1) },
				func(l grpclog.LoggerV2) { l.Fatalf("fatal %d", 1) },
			} {
				var expected []observer.LoggedEntry
				for _, delegate := range []grpclog.LoggerV2{direct, grpcLogger} {
					if i >= 9 {
						require.Panics(t, func() { call(delegate) }) // Fatal uses WriteThenPanic.
					} else {
						call(delegate)
					}
					got := entries.TakeAll()
					for i := range got {
						// Time is not part of the adapter contract.
						got[i].Time = zapcore.Entry{}.Time
					}
					if delegate == direct {
						expected = got
					} else {
						require.Equal(t, expected, got)
					}
				}
			}
			for _, dynamicLevel := range []zapcore.Level{zap.DebugLevel, zap.WarnLevel, zap.ErrorLevel} {
				atomicLevel.SetLevel(dynamicLevel)
				for v := 0; v <= 4; v++ {
					require.Equal(t, direct.V(v), grpcLogger.V(v))
				}
			}
		})
	}
}

func TestGRPCLoggerDepthFallback(t *testing.T) {
	original := grpcLogger.backend.Load()
	t.Cleanup(func() { grpcLogger.backend.Store(original) })
	core, entries := observer.New(zap.DebugLevel)
	ReplaceGRPCLogger(zap.New(core), "INFO")
	grpclog.InfoDepth(4, "info", 1)
	grpclog.WarningDepth(3, "warning", 2)
	grpclog.ErrorDepth(2, "error", 3)
	require.Equal(t, 3, entries.Len())
	for i, message := range []string{"info 1", "warning 2", "error 3"} {
		require.Equal(t, message, entries.All()[i].Message)
	}
}
