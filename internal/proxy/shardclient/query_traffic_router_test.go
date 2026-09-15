// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shardclient

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/proxy/shardclient/querytraffic"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
)

func TestSessionQueryTrafficLabelProviderNilSession(t *testing.T) {
	p := NewSessionQueryTrafficLabelProvider(nil)

	source, err := p.GetSourceLabels(context.Background())
	require.NoError(t, err)
	assert.Nil(t, source)

	nodeLabels, err := p.GetNodeLabels(context.Background(), []int64{1, 2})
	require.NoError(t, err)
	assert.Nil(t, nodeLabels)
}

func TestSessionQueryTrafficLabelProviderGetSourceLabels(t *testing.T) {
	session := &sessionutil.Session{}
	session.ServerLabels = map[string]string{"AZ": "az1"}
	p := NewSessionQueryTrafficLabelProvider(session)

	source, err := p.GetSourceLabels(context.Background())
	require.NoError(t, err)
	assert.Equal(t, querytraffic.Labels{"AZ": "az1"}, source)
}

func TestCollectQueryTrafficNodeLabels(t *testing.T) {
	sessions := map[string]*sessionutil.Session{
		"node-1": {SessionRaw: sessionutil.SessionRaw{ServerID: 1, ServerLabels: map[string]string{"AZ": "az1"}}},
		"node-2": {SessionRaw: sessionutil.SessionRaw{ServerID: 2, ServerLabels: map[string]string{"AZ": "az2"}}},
		"node-3": {SessionRaw: sessionutil.SessionRaw{ServerID: 3}},
	}

	labels := collectQueryTrafficNodeLabels(sessions, []int64{1, 3, 99})
	assert.Equal(t, map[int64]querytraffic.Labels{
		1: {"AZ": "az1"},
		3: nil,
	}, labels)

	assert.Empty(t, collectQueryTrafficNodeLabels(nil, []int64{1}))
	assert.Empty(t, collectQueryTrafficNodeLabels(sessions, nil))
}

// countingRulesConfig returns a rules string that can be switched between
// calls, and counts every Rules() read.
type countingRulesConfig struct {
	raw   atomic.Value // string
	calls atomic.Int64
}

func (c *countingRulesConfig) Enabled() bool { return true }

func (c *countingRulesConfig) Rules() string {
	c.calls.Add(1)
	v, _ := c.raw.Load().(string)
	return v
}

// TestGetPolicyCachesBadConfig pins the M4 fix: a config that fails to parse
// or compile is cached together with its error, so a hot-loaded bad config is
// not re-parsed and re-compiled on every request.
func TestGetPolicyCachesBadConfig(t *testing.T) {
	cfg := &countingRulesConfig{}
	cfg.raw.Store(`[{"name":"local","match":{"sourceLabels":{"exists":["AZ"]}},"routes":[{"name":"local","weight":100,"destinationLabels":{"eq":{"AZ":"${source.AZ}"}}}]}]`)
	r := newQueryTrafficRouter(cfg, staticQueryTrafficLabelProvider{})

	policy, err := r.getPolicy()
	require.NoError(t, err)
	require.NotNil(t, policy)

	// Same raw: policy is served from cache, no re-parse.
	_, err = r.getPolicy()
	require.NoError(t, err)
	require.Equal(t, int64(2), cfg.calls.Load(), "each getPolicy reads the raw once")

	// A bad config is cached including its error: repeated calls do not
	// re-parse or re-compile, they just return the cached error.
	cfg.raw.Store(`[{"name":"local","match":{"sourceLabels":{"exists":["AZ"]}},"routes":[{"name":"local","weight":100,"destinationLabels":{"eq":{"AZ":"${source.AZ"}}}]}]`)
	callsBefore := cfg.calls.Load()
	_, err = r.getPolicy()
	require.Error(t, err)
	_, err = r.getPolicy()
	require.Error(t, err)
	require.Equal(t, callsBefore+2, cfg.calls.Load(), "each getPolicy reads the raw once even for a bad config")
}

// TestGetPolicyCachesParseError verifies the parse-error path caches the
// error, so a bad-config request storm parses the malformed input once.
func TestGetPolicyCachesParseError(t *testing.T) {
	cfg := &countingRulesConfig{}
	cfg.raw.Store(`[{"name":"local",}`) // malformed JSON: array parse fails
	r := newQueryTrafficRouter(cfg, staticQueryTrafficLabelProvider{})

	_, err := r.getPolicy()
	require.Error(t, err)

	// The error is cached: the second call must not re-parse, only re-read the raw.
	calls := cfg.calls.Load()
	_, err = r.getPolicy()
	require.Error(t, err)
	require.Equal(t, calls+1, cfg.calls.Load())
}

// TestQueryTrafficRoutingConfigValidGauge pins the config-valid state gauge:
// it is 1 for a valid config (including the default empty config), flips to 0
// when the config becomes invalid, and back to 1 after the config is fixed.
func TestQueryTrafficRoutingConfigValidGauge(t *testing.T) {
	const validRules = `[{"name":"local","match":{"sourceLabels":{"exists":["AZ"]}},"routes":[{"name":"local","weight":100,"destinationLabels":{"eq":{"AZ":"${source.AZ}"}}}]}]`
	const invalidRules = `[{"name":"local",}` // malformed JSON

	cfg := &countingRulesConfig{}
	cfg.raw.Store(validRules)
	r := newQueryTrafficRouter(cfg, staticQueryTrafficLabelProvider{})

	_, err := r.getPolicy()
	require.NoError(t, err)
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ProxyQueryTrafficRoutingConfigValid),
		"valid config must report the config-valid gauge as 1")

	// A hot-loaded bad config flips the gauge to 0.
	cfg.raw.Store(invalidRules)
	_, err = r.getPolicy()
	require.Error(t, err)
	assert.Equal(t, float64(0), testutil.ToFloat64(metrics.ProxyQueryTrafficRoutingConfigValid),
		"invalid config must report the config-valid gauge as 0")

	// Fixing the config flips the gauge back to 1.
	cfg.raw.Store(validRules)
	_, err = r.getPolicy()
	require.NoError(t, err)
	assert.Equal(t, float64(1), testutil.ToFloat64(metrics.ProxyQueryTrafficRoutingConfigValid),
		"fixed config must report the config-valid gauge back to 1")
}
