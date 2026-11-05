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

package redo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type upsertReadyShard struct {
	shards.ShardManager
	key shards.PartitionUniqueKey
}

func (s *upsertReadyShard) WaitUntilGrowingSegmentReady(key shards.PartitionUniqueKey) (<-chan struct{}, error) {
	s.key = key
	ready := make(chan struct{})
	close(ready)
	return ready, nil
}

func TestUpsertWaitsForTargetPartition(t *testing.T) {
	s := &upsertReadyShard{}
	r := &redoAppendInterceptor{shardManager: s, gracefulStop: make(chan struct{})}
	msg := message.NewUpsertMessageBuilderV2().WithVChannel("p_10v0").
		WithHeader(&message.UpsertMessageHeader{CollectionId: 10, Partition: &message.PartitionSegmentAssignment{PartitionId: 20}}).
		WithBody(&message.UpsertMessageBody{}).MustBuildMutable()
	require.NoError(t, r.waitUntilGrowingSegmentReady(context.Background(), msg))
	require.Equal(t, shards.PartitionUniqueKey{CollectionID: 10, PartitionID: 20}, s.key)
}
