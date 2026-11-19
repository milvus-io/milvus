package logutil

import (
	"flag"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	_ "go.etcd.io/etcd/client/v3" // Exercise the client's optional global logger initialization.
	"google.golang.org/grpc/grpclog"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

func TestSetupLoggerWithEtcdDebug(t *testing.T) {
	if os.Getenv("MILVUS_TEST_ETCD_DEBUG_CHILD") == "1" {
		SetupLogger(&mlog.Config{Level: "info", GrpcLevel: "ERROR", Stdout: true})
		grpclog.Info("grpc info should be filtered")
		grpclog.Error("grpc error reaches Milvus backend")
		return
	}
	executable, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.Command(executable, "-test.run=^TestSetupLoggerWithEtcdDebug$", "-test.count=1") // #nosec G204 -- Re-execute this test binary with fixed test flags.
	if dir := flag.Lookup("test.gocoverdir"); dir != nil && dir.Value.String() != "" {
		cmd.Args = append(cmd.Args, "-test.gocoverdir="+dir.Value.String())
	}
	cmd.Env = append(os.Environ(), "MILVUS_TEST_ETCD_DEBUG_CHILD=1", "ETCD_CLIENT_DEBUG=debug")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", out)
	require.NotContains(t, string(out), "grpc info should be filtered")
	require.Contains(t, string(out), `[ERROR]`)
	require.Contains(t, string(out), "grpc error reaches Milvus backend")
}

// SetupLogger's once and gRPC's logger are process globals. A child process
// exercises the first setup while gRPC readers already exist, just as remote
// configuration creates etcd connections before roles.setupLogger at startup.
func TestSetupLoggerWithGRPCReaders(t *testing.T) {
	if os.Getenv("MILVUS_TEST_GRPC_LOGGER_CHILD") != "1" {
		executable, err := os.Executable()
		require.NoError(t, err)
		cmd := exec.Command(executable, "-test.run=^TestSetupLoggerWithGRPCReaders$", "-test.count=1") // #nosec G204 -- Re-execute this test binary with fixed test flags.
		if dir := flag.Lookup("test.gocoverdir"); dir != nil && dir.Value.String() != "" {
			cmd.Args = append(cmd.Args, "-test.gocoverdir="+dir.Value.String())
		}
		cmd.Env = append(os.Environ(), "MILVUS_TEST_GRPC_LOGGER_CHILD=1", "GRPC_GO_LOG_SEVERITY_LEVEL=NONE")
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", out)
		return
	}

	stop := make(chan struct{})
	ready := make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		grpclog.InfoDepth(0, "before setup")
		close(ready)
		for {
			select {
			case <-stop:
				return
			default:
				grpclog.Info("startup")
				grpclog.Warningln("startup")
				grpclog.Errorf("startup %d", 1)
				grpclog.InfoDepth(2, "startup")
				grpclog.WarningDepth(2, "startup")
				grpclog.ErrorDepth(2, "startup")
				grpclog.V(1)
				runtime.Gosched()
			}
		}
	}()
	<-ready
	runtime.Gosched()
	SetupLogger(&mlog.Config{Level: "debug", GrpcLevel: "INFO"})
	close(stop)
	readers.Wait()
}
