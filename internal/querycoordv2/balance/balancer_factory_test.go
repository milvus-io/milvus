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

package balance

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/querycoordv2/assign"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestBalancerFactoryEpochPolicyForFrozenSelection(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.QueryCoordCfg.Balancer.Key, meta.RoundRobinBalancerName)
	params.Save(params.QueryCoordCfg.AutoBalanceChannel.Key, "true")
	streamingutil.SetStreamingServiceEnabled()
	t.Cleanup(func() {
		streamingutil.UnsetStreamingServiceEnabled()
		params.Reset(params.QueryCoordCfg.Balancer.Key)
		params.Reset(params.QueryCoordCfg.AutoBalanceChannel.Key)
	})

	factory := NewBalancerFactory(nil, nil, nil, nil)
	tests := []struct {
		name         string
		balancer     string
		config       EpochPolicyConfig
		supported    bool
		channelLevel bool
	}{
		{
			name:      "score without streaming",
			balancer:  meta.ScoreBasedBalancerName,
			config:    EpochPolicyConfig{AutoBalanceChannel: true, StreamingServiceEnabled: false},
			supported: true,
		},
		{
			name:      "score with streaming but channel balance disabled",
			balancer:  meta.ScoreBasedBalancerName,
			config:    EpochPolicyConfig{AutoBalanceChannel: false, StreamingServiceEnabled: true},
			supported: true,
		},
		{
			name:      "score with streaming channel balance",
			balancer:  meta.ScoreBasedBalancerName,
			config:    EpochPolicyConfig{AutoBalanceChannel: true, StreamingServiceEnabled: true},
			supported: false,
		},
		{
			name:         "channel level without streaming",
			balancer:     meta.ChannelLevelScoreBalancerName,
			config:       EpochPolicyConfig{StreamingServiceEnabled: false},
			supported:    true,
			channelLevel: true,
		},
		{
			name:      "channel level with streaming",
			balancer:  meta.ChannelLevelScoreBalancerName,
			config:    EpochPolicyConfig{StreamingServiceEnabled: true},
			supported: false,
		},
		{name: "round robin", balancer: meta.RoundRobinBalancerName},
		{name: "row count", balancer: meta.RowCountBasedBalancerName},
		{name: "multiple target", balancer: meta.MultiTargetBalancerName},
		{name: "unknown", balancer: "UnknownBalancerType"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy, supported := factory.GetEpochPolicyFor(test.balancer, test.config)
			require.Equal(t, test.supported, supported)
			if !test.supported {
				require.Nil(t, policy)
				return
			}
			require.IsType(t, &scoreEpochPolicy{}, policy)
			scorePolicy := policy.(*scoreEpochPolicy)
			require.Equal(t, test.channelLevel, scorePolicy.channelLevel)
		})
	}
}

func TestBalancerFactory_GetBalancer(t *testing.T) {
	paramtable.Init()
	assign.ResetGlobalAssignPolicyFactoryForTest()
	assign.InitGlobalAssignPolicyFactory(nil, nil, nil, nil, nil)
	t.Cleanup(assign.ResetGlobalAssignPolicyFactoryForTest)
	t.Cleanup(func() {
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.Balancer.Key)
	})

	f := NewBalancerFactory(nil, nil, nil, nil)

	t.Run("default balancer", func(t *testing.T) {
		paramtable.Get().Reset(paramtable.Get().QueryCoordCfg.Balancer.Key)

		balancer := f.GetBalancer()
		assert.IsType(t, &ChannelLevelScoreBalancer{}, balancer)
	})

	t.Run("fallback logic", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().QueryCoordCfg.Balancer.Key, "UnknownBalancerType")

		balancer := f.GetBalancer()
		assert.IsType(t, &ChannelLevelScoreBalancer{}, balancer)
	})

	t.Run("explicit score based balancer", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().QueryCoordCfg.Balancer.Key, meta.ScoreBasedBalancerName)

		balancer := f.GetBalancer()
		assert.IsType(t, &ScoreBasedBalancer{}, balancer)
	})

	t.Run("explicit round robin balancer", func(t *testing.T) {
		paramtable.Get().Save(paramtable.Get().QueryCoordCfg.Balancer.Key, meta.RoundRobinBalancerName)

		balancer := f.GetBalancer()
		assert.IsType(t, &RoundRobinBalancer{}, balancer)
	})
}
