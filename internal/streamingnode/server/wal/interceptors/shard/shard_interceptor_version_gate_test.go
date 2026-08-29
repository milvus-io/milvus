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

package shard

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/shard/mock_shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// TestShardInterceptorWriteBeforeMaterializationGate verifies that the
// write-before function materialization only runs when the version-gated
// config is effective:
//   - gate off (legacy)   -> materializeFunctionFields is NOT called, append succeeds;
//   - gate on (activated) -> materializeFunctionFields IS called before append.
func TestShardInterceptorWriteBeforeMaterializationGate(t *testing.T) {
	mockErr := errors.New("mock materialize error")
	vchannel := "v1-gated"
	collectionID := int64(99010)

	b := NewInterceptorBuilder()
	shardManager := mock_shards.NewMockShardManager(t)
	shardManager.EXPECT().Logger().Return(mlog.With()).Maybe()
	// handleInsertMessage checks the collection schema version before the
	// materialization gate (master-specific); stub it to pass through so the
	// tests exercise the gate itself.
	shardManager.EXPECT().CheckIfCollectionSchemaVersionMatch(mock.Anything).Return(int32(0), nil)
	i := b.Build(&interceptors.InterceptorBuildParam{
		ShardManager: shardManager,
	})
	defer i.Close()
	ctx := context.Background()
	appender := func(ctx context.Context, msg message.MutableMessage) (message.MessageID, error) {
		return rmq.NewRmqID(1), nil
	}

	buildInsert := func() message.MutableMessage {
		return message.NewInsertMessageBuilderV1().
			WithVChannel(vchannel).
			WithHeader(&messagespb.InsertMessageHeader{
				CollectionId: collectionID,
				Partitions: []*messagespb.PartitionSegmentAssignment{
					{
						PartitionId: 1,
						Rows:        1,
						BinarySize:  100,
					},
				},
			}).
			WithBody(&msgpb.InsertRequest{}).
			MustBuildMutable().WithTimeTick(1)
	}

	// Patch materializeFunctionFields so we can observe whether the gate lets
	// it run (returning an error makes the append fail when it is called).
	m := mockey.Mock((*shardInterceptor).materializeFunctionFields).
		Return(mockErr).Build()
	defer m.UnPatch()

	item := &paramtable.Get().FunctionCfg.EnableWriteBeforeMaterialization

	t.Run("gate off keeps legacy format (materialize not called)", func(t *testing.T) {
		// Swap the config value to the sentinel: the gate is not flipped, so
		// the pre-switch value (false) takes effect.
		old := item.SwapTempValue("auto")
		defer item.SwapTempValue(old)

		shardManager.EXPECT().AssignSegment(mock.Anything).
			Return(&shards.AssignSegmentResult{SegmentID: 1, Acknowledge: atomic.NewInt32(1)}, nil).Once()
		msgID, err := i.DoAppend(ctx, buildInsert(), appender)
		assert.NoError(t, err)
		assert.NotNil(t, msgID)
	})

	t.Run("gate on materializes before append", func(t *testing.T) {
		// Swap the config value to the target (flipped value): the gate is
		// resolved and materialization runs.
		old := item.SwapTempValue("true")
		defer item.SwapTempValue(old)

		// materializeFunctionFields returns mockErr -> the append must fail
		// before reaching AssignSegment.
		msgID, err := i.DoAppend(ctx, buildInsert(), appender)
		require.Error(t, err)
		assert.Nil(t, msgID)
	})
}
