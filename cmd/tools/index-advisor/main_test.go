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

package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// runWith runs the CLI with c's dependencies, capturing its output and using
// the default rule engine when c has none.
func runWith(c *cli, args ...string) (string, string, error) {
	var out, errOut bytes.Buffer
	c.stdout, c.stderr = &out, &errOut
	if c.engine == nil {
		c.engine = defaultRuleEngine()
	}
	err := c.run(args)
	return out.String(), errOut.String(), err
}

func runCLI(args ...string) (string, string, error) {
	return runWith(&cli{}, args...)
}

func TestRunUsage(t *testing.T) {
	_, stderr, err := runCLI()
	assert.True(t, errors.Is(err, merr.ErrParameterMissing))
	assert.Contains(t, stderr, "Usage:")

	_, stderr, err = runCLI("-h")
	assert.NoError(t, err, "-h prints the usage and flags without failing")
	assert.Contains(t, stderr, "--snapshot FILE")
	assert.Contains(t, stderr, "-field-stats")

	_, _, err = runCLI("frobnicate")
	assert.True(t, errors.Is(err, merr.ErrParameterInvalid))
}

// run evaluates columns with the engine it is given.
func TestRunUsesInjectedEngine(t *testing.T) {
	stdout, stderr, err := runWith(&cli{engine: &ruleEngine{}}, "--access-log", "testdata/access_log.txt", "--snapshot", "testdata/snapshot.json")
	require.NoError(t, err, stderr)
	assert.NotContains(t, stdout, `"index_type": "AUTOINDEX"`, "an engine without rules proposes nothing")
	assert.Contains(t, stdout, `"action": "no_action"`)
}

func TestRunInputErrors(t *testing.T) {
	_, _, err := runCLI("--access-log", "testdata/access_log.txt")
	assert.ErrorContains(t, err, "--snapshot")
	for _, args := range [][]string{
		{"--snapshot", "testdata/missing.json"},
		{"--snapshot", "testdata/snapshot.json", "--field-stats", "testdata/missing.json"},
		{"--snapshot", "testdata/snapshot.json", "--milvus-config", "testdata/missing.yaml"},
		{"--snapshot", "testdata/snapshot.json", "--access-log", "testdata/missing.txt"},
		{"--snapshot", "testdata/snapshot.json", "--output", filepath.Join(t.TempDir(), "no", "such", "dir.json")},
	} {
		_, _, err := runCLI(append([]string{"--access-log", "testdata/access_log.txt"}, args...)...)
		assert.Error(t, err, strings.Join(args, " "))
	}
}

func TestRunOutputFile(t *testing.T) {
	out := filepath.Join(t.TempDir(), "advice.json")
	stdout, stderr, err := runCLI("--access-log", "testdata/access_log.txt", "--snapshot", "testdata/snapshot.json", "--output", out)
	require.NoError(t, err, stderr)
	assert.Empty(t, stdout)
	got, err := os.ReadFile(out)
	require.NoError(t, err)
	want, err := os.ReadFile("testdata/advice.json")
	require.NoError(t, err)
	assert.Equal(t, string(want), string(got))
}

// Unknown input keys are reported on stderr and do not stop the run.
func TestRunWarnsOnUnknownKeys(t *testing.T) {
	snapshot := filepath.Join(t.TempDir(), "snapshot.json")
	require.NoError(t, os.WriteFile(snapshot, []byte(`{"format_version": 1, "exported_by": "script",
		"collections": [{"name": "products", "fields": [{"name": "id", "data_type": "Int64", "is_primary_key": true}]}]}`), 0o600))
	stats := filepath.Join(t.TempDir(), "stats.json")
	require.NoError(t, os.WriteFile(stats, []byte(`{"format_version": 1, "source": "s", "collected_at": "2026-10-15T00:00:00Z", "scope": "x", "note": ""}`), 0o600))

	stdout, stderr, err := runCLI("--access-log", "testdata/access_log.txt", "--snapshot", snapshot, "--field-stats", stats)
	require.NoError(t, err, stderr)
	assert.Equal(t, "index-advisor: warning: snapshot file "+snapshot+": ignoring unknown key exported_by\n"+
		"index-advisor: warning: stats file "+stats+": ignoring unknown key note\n", stderr)
	assert.Contains(t, stdout, `"inputs"`)
}
