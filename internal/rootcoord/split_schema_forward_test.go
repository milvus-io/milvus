package rootcoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/metastore/model"
	qvresource "github.com/milvus-io/milvus/internal/views/coord/resource"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type splitSchemaWAL struct {
	streaming.WALAccesser
	append func(context.Context, message.MutableMessage) (*types.AppendResult, error)
}

func (w *splitSchemaWAL) RawAppend(ctx context.Context, msg message.MutableMessage, _ ...streaming.AppendOption) (*types.AppendResult, error) {
	return w.append(ctx, msg)
}

func TestForwardSchemaToSplitSources(t *testing.T) {
	ctx := context.Background()
	schema := &schemapb.CollectionSchema{Name: "c", Version: 4}
	coll := &model.Collection{CollectionID: 1}
	require.NoError(t, forwardSchemaToSplitSources(ctx, nil, coll, schema), "never-split collection needs no lifecycle resolver")
	coll.SplitLineages = []*model.ShardSplitLineage{{SourceVChannel: "p0_1v0"}, {SourceVChannel: "p1_1v1"}}
	require.ErrorIs(t, forwardSchemaToSplitSources(ctx, nil, coll, schema), merr.ErrServiceNotReady)
	var seen []message.MutableMessage
	w := &splitSchemaWAL{append: func(_ context.Context, msg message.MutableMessage) (*types.AppendResult, error) {
		seen = append(seen, msg)
		return &types.AppendResult{}, nil
	}}
	patch := mockey.Mock(streaming.WAL).Return(w).Build()
	defer patch.UnPatch()
	r := &qvresource.Resources{SplitSources: func(_ context.Context, id int64) ([]string, error) {
		require.Equal(t, int64(1), id)
		// The first source has retired locally; its permanent RootCoord
		// lineage remains, but it must receive no more local schema records.
		return []string{"p1_1v1"}, nil
	}}
	require.NoError(t, forwardSchemaToSplitSources(ctx, r, coll, schema))
	require.Len(t, seen, 1)
	require.Equal(t, "p1_1v1", seen[0].VChannel())
	require.True(t, seen[0].IsUnreplicable())
	require.Nil(t, seen[0].ReplicateHeader())
	require.Nil(t, seen[0].BroadcastHeader(), "callback must not start another broadcast")
	forwarded := message.MustAsMutableSchemaChangeMessageV2(seen[0])
	require.Equal(t, int64(1), forwarded.Header().GetCollectionId())
	require.True(t, proto.Equal(schema, forwarded.MustBody().GetSchema()))

	// Append failure must keep the original callback retryable and preserve
	// its cause; retry resolves the live sources again on this cluster.
	w.append = func(context.Context, message.MutableMessage) (*types.AppendResult, error) {
		return nil, merr.ErrServiceUnavailable
	}
	require.ErrorIs(t, forwardSchemaToSplitSources(ctx, r, coll, schema), merr.ErrServiceUnavailable)
	r.SplitSources = func(context.Context, int64) ([]string, error) { return nil, merr.ErrServiceUnavailable }
	require.ErrorIs(t, forwardSchemaToSplitSources(ctx, r, coll, schema), merr.ErrServiceUnavailable)
	r.SplitSources = func(context.Context, int64) ([]string, error) { return []string{"foreign"}, nil }
	require.ErrorIs(t, forwardSchemaToSplitSources(ctx, r, coll, schema), merr.ErrDataIntegrity)
}
