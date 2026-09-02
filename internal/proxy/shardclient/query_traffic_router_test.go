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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/proxy/shardclient/querytraffic"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
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
