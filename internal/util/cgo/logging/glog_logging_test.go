//go:build test
// +build test

package logging

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestInitGoogleLoggingWithZapSinkNoFileOutput is a regression test for
// https://github.com/milvus-io/milvus/issues/53221: after
// InitGoogleLoggingWithZapSink(), glog must not write any log file to the
// temp directory. C++ logs are forwarded to the zap sink through GoZapSink,
// so file logging has to be fully disabled instead of falling back to the
// temp directory (e.g. /tmp) when FLAGS_log_dir is empty.
func TestInitGoogleLoggingWithZapSinkNoFileOutput(t *testing.T) {
	tmpDir := t.TempDir()

	// Point glog's temp-directory fallback at our fresh directory. If file
	// logging is ever re-enabled, glog writes milvus.*.log.* files here.
	oldTmpdir, hadTmpdir := os.LookupEnv("TMPDIR")
	oldTmp, hadTmp := os.LookupEnv("TMP")
	oldTestTmpdir, hadTestTmpdir := os.LookupEnv("TEST_TMPDIR")
	t.Cleanup(func() {
		restoreEnv(t, "TMPDIR", oldTmpdir, hadTmpdir)
		restoreEnv(t, "TMP", oldTmp, hadTmp)
		restoreEnv(t, "TEST_TMPDIR", oldTestTmpdir, hadTestTmpdir)
	})
	require.NoError(t, os.Setenv("TMPDIR", tmpDir))
	require.NoError(t, os.Setenv("TMP", tmpDir))
	require.NoError(t, os.Setenv("TEST_TMPDIR", tmpDir))

	InitGoogleLoggingWithZapSink()

	// Emit one log per non-fatal severity; GLOG_FATAL would abort the process.
	for _, severity := range []glogSeverity{glogInfo, glogWarning, glogError} {
		GoogleLoggingAtLevel(severity, "glog file-output regression test")
	}

	entries, err := os.ReadDir(tmpDir)
	require.NoError(t, err)
	require.Empty(t, entries,
		"glog must not write log files to the temp directory (issue #53221)")
}

func restoreEnv(t *testing.T, key, value string, existed bool) {
	t.Helper()
	if existed {
		require.NoError(t, os.Setenv(key, value))
	} else {
		require.NoError(t, os.Unsetenv(key))
	}
}
