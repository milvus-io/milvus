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

package pipeline

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestTTNodeLifecycleWithoutCheckpointReporter(t *testing.T) {
	paramtable.Init()
	calls := 0
	node := newTTNode(&nodeConfig{vChannelName: "test-channel", dropCallback: func() { calls++ }})
	closeMsg := []Msg{&FlowGraphMsg{BaseMsg: flowgraph.NewBaseMsg(true)}}
	require.Equal(t, closeMsg, node.Operate(closeMsg))
	require.Empty(t, node.Operate([]Msg{&FlowGraphMsg{}}))
	require.Zero(t, calls)
	dropMsg := []Msg{&FlowGraphMsg{dropCollection: true}}
	require.Empty(t, node.Operate(dropMsg))
	require.Equal(t, 1, calls)
	require.Empty(t, node.Operate(closeMsg))
	node.Close()
}
