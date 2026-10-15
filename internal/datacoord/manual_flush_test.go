package datacoord

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestManualFlushSyncCompletion(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "broadcast_failure"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			coll := &collectionInfo{ID: 100, DatabaseName: "db", Schema: &schemapb.CollectionSchema{Name: "coll"}, VChannelNames: []string{"v1", "v2"}}
			bc := &embeddedBroadcastAPI{}
			server := &Server{handler: &embeddedHandler{}, meta: &meta{}}
			// No allocator is installed: streaming Flush must not allocate a separate TSO.
			server.stateCode.Store(commonpb.StateCode_Healthy)
			patch := mockey.Mock(streamingutil.IsStreamingServiceEnabled).Return(true).Build()
			defer patch.UnPatch()
			patchLock := mockey.Mock((*Server).startBroadcastWithCollectionID).To(func(_ *Server, _ context.Context, id int64) (broadcaster.BroadcastAPI, error) {
				require.Equal(t, coll.ID, id)
				return bc, nil
			}).Build()
			defer patchLock.UnPatch()
			patchClose := mockey.Mock((*embeddedBroadcastAPI).Close).Return().Build()
			defer patchClose.UnPatch()
			patchColl := mockey.Mock((*embeddedHandler).GetCollection).Return(coll, nil).Build()
			defer patchColl.UnPatch()
			// A stale or missing checkpoint cannot block the synchronous broadcast.
			patchCP := mockey.Mock((*meta).GetChannelCheckpoint).To(func(_ *meta, channel string) *msgpb.MsgPosition {
				if channel == "v1" {
					return &msgpb.MsgPosition{Timestamp: 1}
				}
				return nil
			}).Build()
			defer patchCP.UnPatch()
			reached := make(chan message.BroadcastMutableMessage, 1)
			ack := make(chan struct{})
			broadcastErr := merr.WrapErrServiceUnavailable("broadcast failed")
			patchBroadcast := mockey.Mock((*embeddedBroadcastAPI).Broadcast).To(func(_ *embeddedBroadcastAPI, ctx context.Context, msg message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
				reached <- msg
				select {
				case <-ack:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				if fail {
					return nil, broadcastErr
				}
				// Broadcast results do not carry RawAppend Extra payloads.
				return &types.BroadcastAppendResult{AppendResults: map[string]*types.AppendResult{"v1": {TimeTick: 10}, "v2": {TimeTick: 20}}}, nil
			}).Build()
			defer patchBroadcast.UnPatch()
			patchSegments := mockey.Mock((*meta).GetSegmentsOfCollection).To(func(_ *meta, _ context.Context, _ int64) []*SegmentInfo {
				select {
				case <-ack:
				default:
					t.Error("segment metadata read before broadcast completion")
				}
				return []*SegmentInfo{
					{SegmentInfo: &datapb.SegmentInfo{ID: 1, State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1}},
					{SegmentInfo: &datapb.SegmentInfo{ID: 2, State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L0}},
					{SegmentInfo: &datapb.SegmentInfo{ID: 3, State: commonpb.SegmentState_Growing, Level: datapb.SegmentLevel_L1}},
				}
			}).Build()
			defer patchSegments.UnPatch()
			done := make(chan *datapb.FlushResponse, 1)
			go func() {
				resp, err := server.Flush(ctx, &datapb.FlushRequest{CollectionID: coll.ID})
				if err != nil {
					t.Error(err)
				}
				done <- resp
			}()
			var msg message.BroadcastMutableMessage
			select {
			case msg = <-reached:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			require.Equal(t, message.MessageTypeManualFlush, msg.MessageType())
			require.True(t, msg.BroadcastHeader().AckSyncUp)
			require.ElementsMatch(t, coll.VChannelNames, msg.BroadcastHeader().VChannels)
			require.Zero(t, msg.BarrierTimeTick())
			require.Zero(t, message.MustAsBroadcastManualFlushMessageV2(msg).Header().GetFlushTs())
			select {
			case <-done:
				t.Fatal("Flush returned before consuming-side completion")
			default:
			}
			close(ack)
			var resp *datapb.FlushResponse
			select {
			case resp = <-done:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			if fail {
				require.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceUnavailable)
				return
			}
			require.NoError(t, merr.Error(resp.GetStatus()))
			require.Empty(t, resp.GetSegmentIDs())
			require.Equal(t, []int64{1}, resp.GetFlushSegmentIDs())
			require.Zero(t, resp.GetFlushTs())
			require.Positive(t, resp.GetTimeOfSeal())
			require.EqualValues(t, 1, resp.GetChannelCps()["v1"].GetTimestamp())
			require.Contains(t, resp.GetChannelCps(), "v2")
			require.Nil(t, resp.GetChannelCps()["v2"])
		})
	}
}

func TestGetFlushStateZeroTimestamp(t *testing.T) {
	server := &Server{meta: &meta{}}
	server.stateCode.Store(commonpb.StateCode_Healthy)
	patch := mockey.Mock((*meta).GetHealthySegment).To(func(_ *meta, _ context.Context, id int64) *SegmentInfo {
		if id == 1 {
			return &SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: id, State: commonpb.SegmentState_Growing}}
		}
		return nil
	}).Build()
	defer patch.UnPatch()
	for _, tc := range []struct {
		name     string
		segments []int64
		flushed  bool
	}{
		{"no_segments_or_checkpoint", nil, true},
		{"unflushed_segment", []int64{1}, false},
		{"retired_segment", []int64{2}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := server.GetFlushState(context.Background(), &datapb.GetFlushStateRequest{CollectionID: 100, SegmentIDs: tc.segments})
			require.NoError(t, err)
			require.NoError(t, merr.Error(resp.GetStatus()))
			require.Equal(t, tc.flushed, resp.GetFlushed())
		})
	}
}
