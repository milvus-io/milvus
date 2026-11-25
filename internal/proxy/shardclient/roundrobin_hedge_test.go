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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestRoundRobinHedgePreservesPrimaryCursor(t *testing.T) {
	paramtable.Init()
	lb := &LBPolicyImpl{}
	for _, count := range []int{2, 3} {
		balancer := NewRoundRobinBalancer()
		leaders := make([]NodeInfo, count)
		for i := range leaders {
			leaders[i] = NodeInfo{NodeID: int64(i + 1), Serviceable: true}
		}
		for i := int64(1); i <= 100; i++ {
			excluded := typeutil.NewUniqueSet()
			primary, selected, err := lb.selectCandidate(context.Background(), balancer, nil, ChannelWorkload{}, leaders, &excluded, false)
			require.NoError(t, err)
			require.True(t, selected)
			require.Equal(t, i, balancer.idx.Load())
			excluded.Insert(primary.NodeID)
			hedge, selected, err := lb.selectCandidate(context.Background(), balancer, nil, ChannelWorkload{}, leaders, &excluded, true)
			require.NoError(t, err)
			require.True(t, selected)
			require.NotEqual(t, primary.NodeID, hedge.NodeID)
			require.Equal(t, i, balancer.idx.Load(), "speculation must not advance the next primary")
			require.Equal(t, i, balancer.hedgeIdx.Load())
		}
	}
}
