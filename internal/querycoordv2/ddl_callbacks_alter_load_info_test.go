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

package querycoordv2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func buildAlterLoadConfigBroadcastResult(collectionID int64) message.BroadcastResultAlterLoadConfigMessageV2 {
	controlChannel := "_ctrl_channel"
	broadcastMsg := message.NewAlterLoadConfigMessageBuilderV2().
		WithHeader(&messagespb.AlterLoadConfigMessageHeader{
			CollectionId: collectionID,
			Replicas: []*messagespb.LoadReplicaConfig{
				{ReplicaId: 1, ResourceGroupName: "__default_resource_group"},
			},
		}).
		WithBody(&messagespb.AlterLoadConfigMessageBody{}).
		WithBroadcast([]string{controlChannel}).
		MustBuildBroadcast()

	return message.BroadcastResultAlterLoadConfigMessageV2{
		Message: message.MustAsBroadcastAlterLoadConfigMessageV2(broadcastMsg),
		Results: map[string]*message.AppendResult{
			controlChannel: {},
		},
	}
}

// TestAlterLoadConfigV2AckCallback verifies that the ack callback swallows the
// dropped-sentinel error (so the broadcaster stops retrying forever) while still
// propagating any other error from the load job.
func TestAlterLoadConfigV2AckCallback(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	s := &Server{}
	result := buildAlterLoadConfigBroadcastResult(1000)

	mockey.PatchConvey("dropped sentinel is acked with no-op", t, func() {
		mockey.Mock((*job.LoadCollectionJob).Execute).
			Return(merr.WrapErrChannelDroppedSentinel("_ctrl_channel")).Build()

		err := s.alterLoadConfigV2AckCallback(ctx, result)
		assert.NoError(t, err)
	})

	mockey.PatchConvey("generic error is propagated", t, func() {
		expectedErr := errors.New("broker unavailable")
		mockey.Mock((*job.LoadCollectionJob).Execute).Return(expectedErr).Build()

		err := s.alterLoadConfigV2AckCallback(ctx, result)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, expectedErr))
	})
}
