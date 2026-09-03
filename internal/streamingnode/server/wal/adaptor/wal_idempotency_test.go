package adaptor_test

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/idempotency"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/registry"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	_ "github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestWALIdempotencyAppend(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.EtcdCfg.RootPath.Key, fmt.Sprintf("idempotency-wal-%d", time.Now().UnixNano()))
	params.Save(params.StreamingCfg.WALWriteAheadBufferKeepalive.Key, "500ms")
	params.Save(params.StreamingCfg.WALWriteAheadBufferCapacity.Key, "10k")
	params.Save(params.StreamingCfg.IdempotencyEnabled.Key, "true")
	message.RegisterDefaultWALName(message.WALNameTest)
	defer func() {
		params.Reset(params.EtcdCfg.RootPath.Key)
		params.Reset(params.StreamingCfg.IdempotencyEnabled.Key)
	}()

	initIdempotencyResourceForTest(t)

	openerBuilder := registry.MustGetBuilder(
		message.WALNameTest,
		idempotency.NewInterceptorBuilder(),
		timetick.NewInterceptorBuilder(),
	)
	opener, err := openerBuilder.Build()
	require.NoError(t, err)
	defer opener.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	channel := types.PChannelInfo{
		Name: fmt.Sprintf("idempotency-wal-pchannel-%d", time.Now().UnixNano()),
		Term: 1,
	}
	rwWAL, err := opener.Open(ctx, &wal.OpenOption{
		Channel:        channel,
		DisableFlusher: true,
	})
	require.NoError(t, err)

	first, err := rwWAL.Append(ctx, newIdempotencyWALAppendMessage("key-1"))
	require.NoError(t, err)
	require.NotNil(t, first)
	require.NotNil(t, first.MessageID)
	require.NotZero(t, first.TimeTick)

	duplicate, err := rwWAL.Append(ctx, newIdempotencyWALAppendMessage("key-1"))
	require.NoError(t, err)
	require.NotNil(t, duplicate)
	require.True(t, first.MessageID.EQ(duplicate.MessageID))
	require.Equal(t, first.TimeTick, duplicate.TimeTick)
	// A duplicate response is identical to the first one, position included: the
	// summary record carries the original last-confirmed, so nothing is
	// substituted and the producer client decodes it the same way either time.
	require.True(t, first.LastConfirmedMessageID.EQ(duplicate.LastConfirmedMessageID))

	second, err := rwWAL.Append(ctx, newIdempotencyWALAppendMessage("key-2"))
	require.NoError(t, err)
	require.NotNil(t, second)
	require.False(t, first.MessageID.EQ(second.MessageID))
	require.Greater(t, second.TimeTick, first.TimeTick)

	// The summary observes the WAL through the recovery scanner, asynchronously
	// from the append that produced the message. Nothing is durable for the
	// idempotency view until it has, so the close below has to happen after the
	// scanner caught up or the reopened window would legitimately be empty.
	//
	// There is no hook to wait on, so this waits on the clock. If it ever
	// becomes flaky, the fix is a signal from the recovery storage, not a
	// longer sleep.
	time.Sleep(3 * time.Second)
	rwWAL.Close()

	recoveredWAL, err := opener.Open(ctx, &wal.OpenOption{
		Channel:        channel,
		DisableFlusher: true,
	})
	require.NoError(t, err)
	defer recoveredWAL.Close()

	recoveredDuplicate, err := recoveredWAL.Append(ctx, newIdempotencyWALAppendMessage("key-1"))
	require.NoError(t, err)
	require.True(t, first.MessageID.EQ(recoveredDuplicate.MessageID))
	require.Equal(t, first.TimeTick, recoveredDuplicate.TimeTick)

	recoveredSecondDuplicate, err := recoveredWAL.Append(ctx, newIdempotencyWALAppendMessage("key-2"))
	require.NoError(t, err)
	require.True(t, second.MessageID.EQ(recoveredSecondDuplicate.MessageID))
	require.Equal(t, second.TimeTick, recoveredSecondDuplicate.TimeTick)
}

// TestWALIdempotencyChunkReachesStorage covers the durable half of the write
// path through the real opener: a sealed chunk is written by an asynchronous
// task on the node scheduler, and nothing in the append path waits for it.
//
// It is worth its own test because the failure mode is silent. The summary is
// happy to stage forever, and recovery reads the staged and sealed tails as well
// as the chunks, so a summary that never writes anything still passes a
// close-and-reopen dedup test -- right up until the process actually dies.
func TestWALIdempotencyChunkReachesStorage(t *testing.T) {
	paramtable.Init()
	params := paramtable.Get()
	params.Save(params.EtcdCfg.RootPath.Key, fmt.Sprintf("idempotency-chunk-%d", time.Now().UnixNano()))
	params.Save(params.StreamingCfg.IdempotencyEnabled.Key, "true")
	// Seal on the first record rather than at the 16MiB default.
	params.Save(params.StreamingCfg.IdempotencyChunkMaxBytes.Key, "1")
	message.RegisterDefaultWALName(message.WALNameTest)
	defer func() {
		params.Reset(params.EtcdCfg.RootPath.Key)
		params.Reset(params.StreamingCfg.IdempotencyEnabled.Key)
		params.Reset(params.StreamingCfg.IdempotencyChunkMaxBytes.Key)
	}()

	chunkManager := initIdempotencyResourceForTest(t)

	openerBuilder := registry.MustGetBuilder(
		message.WALNameTest,
		idempotency.NewInterceptorBuilder(),
		timetick.NewInterceptorBuilder(),
	)
	opener, err := openerBuilder.Build()
	require.NoError(t, err)
	defer opener.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	channel := types.PChannelInfo{
		Name: fmt.Sprintf("idempotency-chunk-pchannel-%d", time.Now().UnixNano()),
		Term: 1,
	}
	rwWAL, err := opener.Open(ctx, &wal.OpenOption{Channel: channel, DisableFlusher: true})
	require.NoError(t, err)

	_, err = rwWAL.Append(ctx, newIdempotencyWALAppendMessage("chunk-key-1"))
	require.NoError(t, err)
	_, err = rwWAL.Append(ctx, newIdempotencyWALAppendMessage("chunk-key-2"))
	require.NoError(t, err)
	rwWAL.Close()

	// The summary observes through the recovery stream, so the reopen is what
	// feeds it: replay stages both records, the 1-byte threshold seals on the
	// first, and the write task carries it to storage.
	recoveredWAL, err := opener.Open(ctx, &wal.OpenOption{Channel: channel, DisableFlusher: true})
	require.NoError(t, err)
	defer recoveredWAL.Close()

	prefix := path.Join(chunkManager.RootPath(), "walsummary") + "/"
	require.Eventually(t, func() bool {
		keys, _, err := storage.ListAllChunkWithPrefix(ctx, chunkManager, prefix, true)
		if err != nil {
			return false
		}
		var chunks, manifests int
		for _, key := range keys {
			switch {
			case strings.Contains(key, "/chunks/"):
				chunks++
			case strings.Contains(key, "/manifest/"):
				manifests++
			}
		}
		return chunks > 0 && manifests > 0
	}, 20*time.Second, 200*time.Millisecond,
		"a sealed chunk and its manifest must reach object storage")
}

func initIdempotencyResourceForTest(t *testing.T) storage.ChunkManager {
	var consumeCheckpoint *streamingpb.WALCheckpoint
	segmentAssignments := make(map[int64]*streamingpb.SegmentAssignmentMeta)
	vchannels := make(map[string]*streamingpb.VChannelMeta)

	rc := mocks.NewMockMixCoordClient(t)
	tso := atomic.Uint64{}
	tso.Store(1000)
	rc.EXPECT().AllocTimestamp(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *rootcoordpb.AllocTimestampRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
		start := tso.Add(uint64(req.Count)) - uint64(req.Count)
		return &rootcoordpb.AllocTimestampResponse{
			Status:    merr.Success(),
			Timestamp: start,
			Count:     req.Count,
		}, nil
	}).Maybe()
	rc.EXPECT().GetPChannelInfo(mock.Anything, mock.Anything).Return(&rootcoordpb.GetPChannelInfoResponse{
		Status: merr.Success(),
		Collections: []*rootcoordpb.CollectionInfoOnPChannel{
			{
				CollectionId: 1,
				Partitions: []*rootcoordpb.PartitionInfoOnPChannel{
					{PartitionId: 1},
				},
				Vchannel: "v1",
				State:    etcdpb.CollectionState_CollectionCreated,
			},
		},
	}, nil).Maybe()
	rc.EXPECT().DescribeCollectionInternal(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, req *milvuspb.DescribeCollectionRequest, opts ...grpc.CallOption) (*milvuspb.DescribeCollectionResponse, error) {
		return &milvuspb.DescribeCollectionResponse{
			Status:       merr.Success(),
			CollectionID: req.CollectionID,
			Schema:       &schemapb.CollectionSchema{Name: "idempotency_test_collection"},
		}, nil
	}).Maybe()

	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().GetConsumeCheckpoint(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string) (*streamingpb.WALCheckpoint, error) {
		if consumeCheckpoint == nil {
			return nil, nil
		}
		return proto.Clone(consumeCheckpoint).(*streamingpb.WALCheckpoint), nil
	})
	catalog.EXPECT().ListSegmentAssignment(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string) ([]*streamingpb.SegmentAssignmentMeta, error) {
		values := make([]*streamingpb.SegmentAssignmentMeta, 0, len(segmentAssignments))
		for _, meta := range segmentAssignments {
			values = append(values, proto.Clone(meta).(*streamingpb.SegmentAssignmentMeta))
		}
		return values, nil
	}).Maybe()
	catalog.EXPECT().ListVChannel(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string) ([]*streamingpb.VChannelMeta, error) {
		values := make([]*streamingpb.VChannelMeta, 0, len(vchannels))
		for _, meta := range vchannels {
			values = append(values, proto.Clone(meta).(*streamingpb.VChannelMeta))
		}
		return values, nil
	}).Maybe()
	catalog.EXPECT().GetSalvageCheckpoint(mock.Anything, mock.Anything).Return(nil, nil).Maybe()
	catalog.EXPECT().SaveRecoverySnapshot(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, pchannel string, snapshot *metastore.WALRecoverySnapshot) error {
		if snapshot == nil {
			return nil
		}
		// The consume checkpoint is the commit point of the snapshot now, not a
		// separate catalog write.
		if snapshot.ConsumeCheckpoint != nil {
			consumeCheckpoint = proto.Clone(snapshot.ConsumeCheckpoint).(*streamingpb.WALCheckpoint)
		}
		for _, meta := range snapshot.SegmentAssignments {
			segmentID := meta.GetSegmentId()
			if meta.GetState() == streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED {
				delete(segmentAssignments, segmentID)
				continue
			}
			segmentAssignments[segmentID] = proto.Clone(meta).(*streamingpb.SegmentAssignmentMeta)
		}
		for key, meta := range snapshot.VChannels {
			vchannelName := key
			if meta.GetVchannel() != "" {
				vchannelName = meta.GetVchannel()
			}
			if meta.GetState() == streamingpb.VChannelState_VCHANNEL_STATE_DROPPED {
				delete(vchannels, vchannelName)
				continue
			}
			vchannels[vchannelName] = proto.Clone(meta).(*streamingpb.VChannelMeta)
		}
		if snapshot.ConsumeCheckpoint != nil {
			consumeCheckpoint = proto.Clone(snapshot.ConsumeCheckpoint).(*streamingpb.WALCheckpoint)
		}
		return nil
	}).Maybe()

	fMixCoordClient := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fMixCoordClient.Set(rc)
	// The root must be the raw t.TempDir(): summary chunk keys are built from the
	// chunk manager's own root and LocalChunkManager writes a key verbatim, so a
	// relative root would drop the chunk files into the package directory.
	chunkManager := storage.NewLocalChunkManager(objectstorage.RootPath(t.TempDir()))
	resource.InitForTest(
		t,
		resource.OptMixCoordClient(fMixCoordClient),
		resource.OptStreamingNodeCatalog(catalog),
		resource.OptChunkManager(chunkManager),
	)
	return chunkManager
}

func newIdempotencyWALAppendMessage(key string) message.MutableMessage {
	return message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&message.InsertMessageHeader{CollectionId: 1}).
		WithBody(&msgpb.InsertRequest{}).
		WithIdempotencyKey(key).
		MustBuildMutable()
}
