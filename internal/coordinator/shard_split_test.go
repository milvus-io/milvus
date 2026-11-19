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

package coordinator

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord"
	"github.com/milvus-io/milvus/internal/datacoord/split"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/util/mock_message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRegisterShardSplitConnectsTheCoordinators(t *testing.T) {
	ctx := context.Background()
	t.Cleanup(func() {
		registry.ResetConsumerAckObservers()
		rootcoord.RegisterSplitShardAckHook(nil)
		rootcoord.RegisterSplitShardAppliedHook(nil)
	})
	dataCoord, rootCoord := &datacoord.Server{}, &rootcoord.Core{}

	var issued *rootcoord.SplitShardRequest
	issueErr := errors.New("mock")
	patchIssue := mockey.Mock((*rootcoord.Core).BroadcastSplitShard).To(
		func(_ *rootcoord.Core, _ context.Context, req *rootcoord.SplitShardRequest) (*types.BroadcastAppendResult, error) {
			issued = req
			return nil, issueErr
		}).Build()
	defer patchIssue.UnPatch()
	var captured datacoord.SplitShardIssuer
	patchSet := mockey.Mock((*datacoord.Server).SetSplitShardIssuer).To(
		func(_ *datacoord.Server, issuer datacoord.SplitShardIssuer) { captured = issuer }).Build()
	defer patchSet.UnPatch()

	registerShardSplit(dataCoord, rootCoord)

	// DataCoord issues through RootCoord, which plans the residues itself.
	request := split.Request{CollectionID: 100, SplitTaskID: 7, Source: "s", Targets: [2]string{"t1", "t2"}}
	admitErr := errors.New("refused under the key")
	admitted := 0
	admit := func(context.Context) error {
		admitted++
		return admitErr
	}
	require.ErrorIs(t, captured(ctx, request, admit), issueErr)
	// DataCoord's admission is carried into RootCoord's plan, which runs it.
	require.NotNil(t, issued.Admit)
	require.ErrorIs(t, issued.Admit(ctx), admitErr)
	require.Equal(t, 1, admitted)
	issued.Admit = nil
	require.Equal(t, &rootcoord.SplitShardRequest{CollectionID: 100, SplitTaskID: 7, Source: "s", Targets: [2]string{"t1", "t2"}}, issued)

	// The consumer acks of SplitShard reach DataCoord, which is not recovered
	// here and refuses, so that the StreamingNode sends them again.
	msg := mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().MessageType().Return(message.MessageTypeSplitShard)
	require.ErrorIs(t, registry.CallConsumerAckObserver(ctx, msg, false), merr.ErrServiceNotReady)
}
