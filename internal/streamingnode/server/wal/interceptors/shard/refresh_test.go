package shard

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type refreshShardManager struct{ shards.ShardManager }

func (*refreshShardManager) AlterCollection(message.MutableAlterCollectionMessageV2) ([]int64, error) {
	panic("unpatched")
}

func TestRefreshInstallsSchemaWithoutSecondAppend(t *testing.T) {
	for _, mode := range []string{"legacy", "target", "failure"} {
		t.Run(mode, func(t *testing.T) {
			impl := &shardInterceptor{shardManager: &refreshShardManager{}}
			var target *schemapb.CollectionSchema
			if mode != "legacy" {
				target = &schemapb.CollectionSchema{Version: 2, ExternalSource: "s3://new"}
			}
			installed := false
			patch := mockey.Mock((*refreshShardManager).AlterCollection).To(func(msg message.MutableAlterCollectionMessageV2) ([]int64, error) {
				installed = true
				require.EqualValues(t, 10, msg.TimeTick())
				require.True(t, proto.Equal(target, msg.MustBody().GetUpdates().GetSchema()))
				if mode == "failure" {
					return nil, merr.ErrServiceInternal
				}
				return nil, nil
			}).Build()
			defer patch.UnPatch()
			msg := message.NewRefreshExternalCollectionMessageBuilderV2().WithVChannel("v1").WithHeader(&message.RefreshExternalCollectionMessageHeader{CollectionId: 1}).WithBody(&message.RefreshExternalCollectionMessageBody{Schema: target}).MustBuildMutable().WithTimeTick(10)
			appends := 0
			_, err := impl.handleRefreshExternalCollection(t.Context(), msg, func(_ context.Context, actual message.MutableMessage) (message.MessageID, error) {
				appends++
				require.Equal(t, message.MessageTypeRefreshExternalCollection, actual.MessageType())
				return rmq.NewRmqID(10), nil
			})
			require.Equal(t, mode != "legacy", installed)
			if mode == "failure" {
				require.Error(t, err)
				require.Zero(t, appends)
			} else {
				require.NoError(t, err)
				require.Equal(t, 1, appends)
			}
		})
	}
}
