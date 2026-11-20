package dml

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/proxy/metacache"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

type invalidationTestWAL struct{ streaming.WALAccesser }

func TestInsertAppendReturnsMixedOutcomeWithoutReplay(t *testing.T) {
	cache := newTestCache()
	vchannels := []string{"p_10v0", "p_10v1", "p_10v2"}
	// The write path reads the collection's routing and channel list on every
	// attempt; a collection with no modulus has never been split.
	mockTest(t, (*metacache.MetaCache).GetCollectionInfo, &collectionInfo{CollID: 10, VChannels: vchannels}, nil)
	task := &InsertTask{
		baseTask: baseTask{MetaCache: cache}, ctx: t.Context(), collectionID: 10,
		insertMsg: &msgstream.InsertMsg{InsertRequest: &msgpb.InsertRequest{Base: &commonpb.MsgBase{}}},
		result:    &milvuspb.MutationResult{},
	}
	wal := &invalidationTestWAL{}
	patch := mockey.Mock(streaming.WAL).Return(wal).Build()
	defer patch.UnPatch()
	// One message per shard, so the append's per-shard outcomes settle against
	// the messages that carried them.
	msgs := make([]message.MutableMessage, 0, len(vchannels))
	for _, vchannel := range vchannels {
		msg, err := message.NewInsertMessageBuilderV1().
			WithVChannel(vchannel).
			WithHeader(&message.InsertMessageHeader{CollectionId: 10}).
			WithBody(&msgpb.InsertRequest{CollectionID: 10}).
			BuildMutable()
		require.NoError(t, err)
		msgs = append(msgs, msg)
	}
	patch = mockey.Mock(repackInsertDataForStreamingService).Return(msgs, make([][]int, len(msgs)), nil).Build()
	defer patch.UnPatch()
	appends, invalidations := 0, 0
	patch = mockey.Mock((*invalidationTestWAL).AppendMessagesWithOptions).To(func(_ *invalidationTestWAL, _ context.Context, _ []message.MutableMessage, _ ...streaming.AppendOption) streaming.AppendResponses {
		appends++
		return streaming.AppendResponses{Responses: []streaming.AppendResponse{
			{Error: status.NewOnShutdownError("first shard shutting down")},
			{}, // This shard committed and must not be replayed.
			{Error: status.NewSchemaVersionMismatch("later shard advanced")},
		}}
	}).Build()
	defer patch.UnPatch()
	patch = mockey.Mock((*metacache.MetaCache).RemoveCollectionsByID).To(func(_ *metacache.MetaCache, _ context.Context, id int64) []string {
		require.EqualValues(t, 10, id)
		invalidations++
		return nil
	}).Build()
	defer patch.UnPatch()
	require.NoError(t, task.Execute(t.Context()))
	require.Equal(t, 1, appends)
	// A later shard mismatch expires the schema for the next request without
	// replaying this partially committed append or replacing its first error.
	require.Equal(t, 1, invalidations)
	require.Contains(t, task.result.GetStatus().GetReason(), "first shard shutting down")
}
