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

package paramtable

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestHTTPConfig_Init(t *testing.T) {
	params := ComponentParam{}
	params.Init(NewBaseTable(SkipRemote(true)))
	cfg := &params.HTTPCfg
	assert.Equal(t, cfg.Enabled.GetAsBool(), true)
	assert.True(t, cfg.EnableV1.GetAsBool())
	assert.Equal(t, cfg.DebugMode.GetAsBool(), false)
	assert.Equal(t, cfg.Port.GetValue(), "")
	assert.Equal(t, cfg.AcceptTypeAllowInt64.GetValue(), "true")
	assert.Equal(t, cfg.EnablePprof.GetAsBool(), true)
	assert.Equal(t, cfg.DQLAdmissionEnabled.GetAsBool(), true)
	assert.Equal(t, 5*time.Second, cfg.ReadHeaderTimeout.GetAsDurationByParse())
	assert.Equal(t, time.Duration(0), cfg.ReadTimeout.GetAsDurationByParse())
	assert.Equal(t, time.Duration(0), cfg.WriteTimeout.GetAsDurationByParse())
	assert.Equal(t, 300*time.Second, cfg.IdleTimeout.GetAsDurationByParse())
	assert.Equal(t, 16777216, cfg.MaxHeaderBytes.GetAsInt())
	assert.Equal(t, cfg.EnableWebUI.GetAsBool(), true)
}

func TestHTTPConfig_TimeoutOverrides(t *testing.T) {
	params := ComponentParam{}
	base := NewBaseTable(SkipRemote(true))
	params.Init(base)
	cfg := &params.HTTPCfg

	base.Save("proxy.http.readHeaderTimeout", "7s")
	base.Save("proxy.http.readTimeout", "8s")
	base.Save("proxy.http.writeTimeout", "9s")
	base.Save("proxy.http.idleTimeout", "10s")
	base.Save("proxy.http.maxHeaderBytes", "2048")

	assert.Equal(t, 7*time.Second, cfg.ReadHeaderTimeout.GetAsDurationByParse())
	assert.Equal(t, 8*time.Second, cfg.ReadTimeout.GetAsDurationByParse())
	assert.Equal(t, 9*time.Second, cfg.WriteTimeout.GetAsDurationByParse())
	assert.Equal(t, 10*time.Second, cfg.IdleTimeout.GetAsDurationByParse())
	assert.Equal(t, 2048, cfg.MaxHeaderBytes.GetAsInt())
}

func TestHTTPConfig_V1Override(t *testing.T) {
	base := NewBaseTable(SkipRemote(true), SkipEnv(true), Files([]string{}))
	t.Cleanup(base.mgr.Close)
	cfg := httpConfig{}
	cfg.init(base)
	assert.True(t, cfg.EnableV1.GetAsBool(), "existing deployments without this key keep V1 enabled")
	require.NoError(t, base.Save(cfg.EnableV1.Key, "false"))
	assert.False(t, cfg.EnableV1.GetAsBool())
	assert.True(t, cfg.Enabled.GetAsBool(), "the route switch must not disable the HTTP listener")
}

func httpBudgetFixture(t *testing.T, yaml string) *httpConfig {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "milvus.yaml"), []byte(yaml), 0o600))
	t.Setenv("MILVUSCONF", dir)
	base := NewBaseTable(Files([]string{"milvus.yaml"}), SkipRemote(true), SkipEnv(true), Interval(0))
	t.Cleanup(base.mgr.Close)
	cfg := &httpConfig{}
	cfg.init(base)
	return cfg
}

func TestHTTPConfigBudgetDefaultsToApproved120Seconds(t *testing.T) {
	cfg := httpBudgetFixture(t, "proxy:\n  http:\n    requestTimeoutMs: 30000\n")
	got, err := cfg.ParseRequestBudgetPolicy()
	require.NoError(t, err)
	require.Equal(t, 120*time.Second, got.OverallTimeoutBudget)
	require.Equal(t, 5*time.Second, got.ReadHeaderTimeout)
}

func TestHTTPConfigBudgetDoesNotRequireIOIdle(t *testing.T) {
	cfg := httpBudgetFixture(t, "proxy:\n  http:\n    overallTimeoutBudget: 120s\n")
	got, err := cfg.ParseRequestBudgetPolicy()
	require.NoError(t, err)
	require.Equal(t, 120*time.Second, got.OverallTimeoutBudget)
	require.Equal(t, 5*time.Second, got.ReadHeaderTimeout)
	require.Equal(t, 300*time.Second, got.MaxConnectionIdleInterval)
}

func TestHTTPConfigBudgetAllowsOverallShorterThanHeader(t *testing.T) {
	cfg := httpBudgetFixture(t, "proxy:\n  http:\n    overallTimeoutBudget: 4s\n    readHeaderTimeout: 5s\n")
	got, err := cfg.ParseRequestBudgetPolicy()
	require.NoError(t, err)
	require.Equal(t, 4*time.Second, got.OverallTimeoutBudget)
	require.Equal(t, 5*time.Second, got.ReadHeaderTimeout)
}

func TestHTTPConfigBudgetMigratesLegacyIdle(t *testing.T) {
	cfg := httpBudgetFixture(t, "proxy:\n  http:\n    overallTimeoutBudget: 120s\n    idleTimeout: 25s\n")
	got, err := cfg.ParseRequestBudgetPolicy()
	require.NoError(t, err)
	require.Equal(t, 120*time.Second, got.OverallTimeoutBudget)
	require.Equal(t, 5*time.Second, got.ReadHeaderTimeout)
	require.Equal(t, 25*time.Second, got.MaxConnectionIdleInterval)

	cfg = httpBudgetFixture(t, "proxy:\n  http:\n    overallTimeoutBudget: 120s\n")
	got, err = cfg.ParseRequestBudgetPolicy()
	require.NoError(t, err)
	require.Equal(t, 300*time.Second, got.MaxConnectionIdleInterval)
}

func TestHTTPConfigBudgetDetectsExplicitIdleConflict(t *testing.T) {
	base := "proxy:\n  http:\n    overallTimeoutBudget: 120s\n"
	for _, tt := range []struct {
		name, old, current string
		conflict           bool
	}{
		{"different", "300s", "301s", true},
		{"old explicit default", "300s", "20s", true},
		{"equivalent", "300s", "5m", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := httpBudgetFixture(t, base+"    idleTimeout: "+tt.old+"\n    maxConnectionIdleInterval: "+tt.current+"\n")
			got, err := cfg.ParseRequestBudgetPolicy()
			if tt.conflict {
				require.ErrorIs(t, err, merr.ErrServiceInternal)
				require.Contains(t, err.Error(), "idleTimeout")
				require.Contains(t, err.Error(), "maxConnectionIdleInterval")
			} else {
				require.NoError(t, err)
				require.Equal(t, 300*time.Second, got.MaxConnectionIdleInterval)
			}
		})
	}
}

func TestHTTPConfigBudgetRejectsLegacyReadWrite(t *testing.T) {
	for _, key := range []string{"readTimeout", "writeTimeout"} {
		t.Run(key, func(t *testing.T) {
			cfg := httpBudgetFixture(t, "proxy:\n  http:\n    overallTimeoutBudget: 120s\n    "+key+": 10s\n")
			_, err := cfg.ParseRequestBudgetPolicy()
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.True(t, strings.Contains(err.Error(), key) && strings.Contains(err.Error(), "clear"), err)
		})
	}
}

func TestHTTPConfigBudgetRejectsLegacyRequestTimeoutOverride(t *testing.T) {
	cfg := httpBudgetFixture(t, "proxy:\n  http:\n    requestTimeoutMs: 120000\n")
	_, err := cfg.ParseRequestBudgetPolicy()
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Contains(t, err.Error(), "requestTimeoutMs")
	require.Contains(t, err.Error(), "overallTimeoutBudget")
}

func TestHTTPConfigBudgetRejectsMalformedAndOutOfRange(t *testing.T) {
	for _, tt := range []struct{ key, value string }{
		{"overallTimeoutBudget", "not-a-duration"},
		{"overallTimeoutBudget", "0s"},
		{"readHeaderTimeout", "bad"},
		{"maxConnectionIdleInterval", "-1s"},
		{"idleTimeout", "bad"},
		{"readTimeout", "bad"},
	} {
		t.Run(tt.key+"="+tt.value, func(t *testing.T) {
			overall := "120s"
			if tt.key == "overallTimeoutBudget" {
				overall = tt.value
			}
			yaml := "proxy:\n  http:\n    overallTimeoutBudget: " + overall + "\n"
			if tt.key != "overallTimeoutBudget" {
				yaml += "    " + tt.key + ": " + tt.value + "\n"
			}
			cfg := httpBudgetFixture(t, yaml)
			_, err := cfg.ParseRequestBudgetPolicy()
			if !errors.Is(err, merr.ErrServiceInternal) || !strings.Contains(err.Error(), tt.key) {
				t.Fatalf("error = %v, want system config error naming %s", err, tt.key)
			}
		})
	}
}

func TestHTTPConfigBudgetRejectsExplicitEmptyDuration(t *testing.T) {
	for _, key := range []string{
		"overallTimeoutBudget", "readHeaderTimeout",
		"maxConnectionIdleInterval", "readTimeout", "writeTimeout", "idleTimeout",
	} {
		t.Run(key, func(t *testing.T) {
			overall := "120s"
			if key == "overallTimeoutBudget" {
				overall = ""
			}
			yaml := "proxy:\n  http:\n    overallTimeoutBudget: " + overall + "\n"
			if key != "overallTimeoutBudget" {
				yaml += "    " + key + ": \n"
			}
			cfg := httpBudgetFixture(t, yaml)
			_, err := cfg.ParseRequestBudgetPolicy()
			if !errors.Is(err, merr.ErrServiceInternal) || !strings.Contains(err.Error(), key) {
				t.Fatalf("error = %v, want system config error naming %s", err, key)
			}
		})
	}
}

func TestHTTPConfigBudgetAcceptsExplicitLegacyReadWriteZero(t *testing.T) {
	cfg := httpBudgetFixture(t, "proxy:\n  http:\n    overallTimeoutBudget: 120s\n    readTimeout: 0s\n    writeTimeout: 0s\n")
	got, err := cfg.ParseRequestBudgetPolicy()
	require.NoError(t, err)
	require.Equal(t, 120*time.Second, got.OverallTimeoutBudget)
}
