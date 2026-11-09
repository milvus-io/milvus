// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package failpoint

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunPrebuiltFixture(t *testing.T) {
	fixture := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(fixture, "failpoint-fixture"), nil, 0o600))
	t.Setenv("MILVUS_FAILPOINT_WORK_DIR", fixture)
	t.Setenv("MILVUS_WORK_DIR", "")

	called := false
	require.Equal(t, 7, run(func() int {
		called = true
		require.Equal(t, fixture, os.Getenv("MILVUS_WORK_DIR"))
		return 7
	}))
	require.True(t, called)
	require.FileExists(t, filepath.Join(fixture, "failpoint-fixture"))
}

func TestRunRejectsInvalidEnvironment(t *testing.T) {
	for _, name := range []string{"missing checkout", "missing marker", "temporary directory failure"} {
		t.Run(name, func(t *testing.T) {
			t.Setenv("MILVUS_FAILPOINT_WORK_DIR", "")
			t.Setenv("MILVUS_WORK_DIR", "")
			switch name {
			case "missing marker":
				t.Setenv("MILVUS_FAILPOINT_WORK_DIR", t.TempDir())
			case "temporary directory failure":
				t.Setenv("MILVUS_WORK_DIR", t.TempDir())
				t.Setenv("TMPDIR", filepath.Join(t.TempDir(), "missing"))
			}

			called := false
			require.Equal(t, 1, run(func() int {
				called = true
				return 0
			}))
			require.False(t, called, "invalid fixtures must not start the test suite")
		})
	}
}

func TestRunBuildsAndCleansFixture(t *testing.T) {
	for _, tc := range []struct {
		name       string
		script     string
		wantCalled bool
		wantCode   int
	}{
		{"success", `touch "$1/failpoint-fixture"`, true, 7},
		{"build failure", "exit 9", false, 1},
		{"missing marker", "exit 0", false, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "checkout with spaces")
			require.NoError(t, os.MkdirAll(filepath.Join(root, "scripts"), 0o700))
			// Record the build destination outside the fixture to verify cleanup even on failure.
			record := filepath.Join(root, "build-destination")
			t.Setenv("FAILPOINT_TEST_DESTINATION", record)
			script := "set -eu\nprintf '%s' \"$1\" > \"$FAILPOINT_TEST_DESTINATION\"\n" + tc.script + "\n"
			require.NoError(t, os.WriteFile(filepath.Join(root, "scripts", "build_failpoint_fixture.sh"), []byte(script), 0o600))
			t.Setenv("MILVUS_FAILPOINT_WORK_DIR", "")
			t.Setenv("MILVUS_WORK_DIR", root)

			called := false
			code := run(func() int {
				called = true
				destination, err := os.ReadFile(record)
				require.NoError(t, err)
				require.Equal(t, string(destination), os.Getenv("MILVUS_WORK_DIR"))
				require.FileExists(t, filepath.Join(string(destination), "failpoint-fixture"))
				return 7
			})
			require.Equal(t, tc.wantCode, code)
			require.Equal(t, tc.wantCalled, called)
			destination, err := os.ReadFile(record)
			require.NoError(t, err)
			require.NotEmpty(t, destination)
			require.NoDirExists(t, string(destination), "temporary fixtures must be removed after success or failure")
		})
	}
}
