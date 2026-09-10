package testcases

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/milvus-io/milvus/tests/go_client/testcases/helper"
)

func TestMain(m *testing.M) {
	if code := ensureExternalTablePythonDeps(); code != 0 {
		os.Exit(code)
	}
	os.Exit(helper.RunTests(m))
}

func ensureExternalTablePythonDeps() int {
	if !shouldInstallExternalTablePythonDeps() {
		return 0
	}

	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		fmt.Fprintln(os.Stderr, "failed to locate go client test directory")
		return 1
	}

	scriptPath := filepath.Join(filepath.Dir(thisFile), "..", "scripts", "install_external_table_python_deps.sh")
	cmd := exec.Command("bash", scriptPath) // #nosec G204
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to install external table python deps: %v\n", err)
		return 1
	}
	return 0
}

func shouldInstallExternalTablePythonDeps() bool {
	if isTruthyEnv("GO_CLIENT_SKIP_EXTERNAL_TABLE_DEPS_INSTALL") {
		return false
	}
	if isTruthyEnv("GO_CLIENT_INSTALL_EXTERNAL_TABLE_DEPS") {
		return true
	}

	host := hostFromMilvusAddr(helper.URIFromTestArgs(os.Args))
	serviceName, _, _ := strings.Cut(host, ".")
	return strings.HasPrefix(serviceName, "gosdk-") && strings.HasSuffix(serviceName, "-milvus")
}

func isTruthyEnv(key string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}
