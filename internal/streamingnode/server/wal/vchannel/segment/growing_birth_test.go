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

package segment

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func TestGrowingBirthRequestUsesWALCreateSegment(t *testing.T) {
	raw := message.NewCreateSegmentMessageBuilderV2().WithVChannel("p_100v1").
		WithHeader(&message.CreateSegmentMessageHeader{CollectionId: 100, PartitionId: 10, SegmentId: 1}).
		WithBody(&message.CreateSegmentMessageBody{}).MustBuildMutable().WithTimeTick(200).
		WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(200))
	born := newSegmentAssignmentMetaFromCreateSegmentMessage(message.MustAsImmutableCreateSegmentMessageV2(raw))
	// Both a fresh module and the proto round-trip used by recovery must carry
	// the original WAL birth, regardless of its later checkpoint timestamp.
	born.CheckpointTimeTick = 400
	bytes, err := proto.Marshal(born)
	require.NoError(t, err)
	recovered := &streamingpb.SegmentAssignmentMeta{}
	require.NoError(t, proto.Unmarshal(bytes, recovered))
	for _, meta := range []*streamingpb.SegmentAssignmentMeta{born, recovered} {
		req := buildEnsureGrowingSegmentRequest(meta)
		require.Equal(t, uint64(200), req.GetCreateSegmentTimeTick())
		require.Equal(t, int64(100), req.GetCollectionId())
		require.True(t, req.GetIsCreatedByStreaming())
	}
	require.Zero(t, buildEnsureGrowingSegmentRequest(&streamingpb.SegmentAssignmentMeta{}).GetCreateSegmentTimeTick())
}
