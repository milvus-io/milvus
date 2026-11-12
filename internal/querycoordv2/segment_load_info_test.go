package querycoordv2

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type segmentMetadataTestCoord struct{ types.MixCoord }

func (*segmentMetadataTestCoord) GetQueryViewSegmentLoadInfos(context.Context, int64, []int64) ([]*querypb.QueryViewSegmentLoadInfoResult, []*indexpb.IndexInfo, error) {
	panic("patch with mockey")
}

func TestGetQueryViewSegmentLoadInfoRPC(t *testing.T) {
	mockey.PatchConvey("unary errors are isolated and complete snapshots survive protobuf transport", t, func() {
		mockey.Mock((*segmentMetadataTestCoord).GetQueryViewSegmentLoadInfos).To(func(_ *segmentMetadataTestCoord, ctx context.Context, collection int64, ids []int64) ([]*querypb.QueryViewSegmentLoadInfoResult, []*indexpb.IndexInfo, error) {
			if len(ids) == 1 && ids[0] == 3 {
				<-ctx.Done()
				return nil, nil, ctx.Err()
			}
			results := make([]*querypb.QueryViewSegmentLoadInfoResult, 0, len(ids))
			for _, id := range ids {
				result := &querypb.QueryViewSegmentLoadInfoResult{SegmentID: id, Status: merr.Success()}
				if id == 2 {
					result.Status = merr.Status(merr.WrapErrSegmentNotFound(id))
				} else {
					result.LoadInfo = &querypb.SegmentLoadInfo{CollectionID: collection, SegmentID: id, ManifestPath: "base/_metadata/manifest-2.avro", TransformStartAfterTimetick: 100, Stats: &datapb.Statistics{InsertBinlogSize: 1024}}
				}
				results = append(results, result)
			}
			return results, []*indexpb.IndexInfo{{IndexID: 5}}, nil
		}).Build()
		coord := &Server{ctx: t.Context(), mixCoord: &segmentMetadataTestCoord{}}
		coord.UpdateStateCode(commonpb.StateCode_Healthy)
		listener := bufconn.Listen(1 << 20)
		server := grpc.NewServer()
		querypb.RegisterQueryCoordServer(server, coord)
		go func() { _ = server.Serve(listener) }()
		t.Cleanup(server.Stop)
		t.Cleanup(func() { _ = listener.Close() })
		conn, err := grpc.NewClient("passthrough:///segment-metadata", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		client := querypb.NewQueryCoordClient(conn)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		resp, err := client.GetQueryViewSegmentLoadInfo(ctx, &querypb.GetQueryViewSegmentLoadInfoRequest{CollectionID: 1, SegmentIDs: []int64{2, 1}})
		require.NoError(t, err)
		require.ErrorIs(t, merr.Error(resp.Results[0].GetStatus()), merr.ErrSegmentNotFound)
		require.NoError(t, merr.Error(resp.GetStatus()))
		require.EqualValues(t, 1, resp.Results[1].LoadInfo.SegmentID)
		short, stop := context.WithTimeout(ctx, 30*time.Millisecond)
		defer stop()
		_, err = client.GetQueryViewSegmentLoadInfo(short, &querypb.GetQueryViewSegmentLoadInfoRequest{CollectionID: 1, SegmentIDs: []int64{3}})
		require.Error(t, err)
		resp, err = client.GetQueryViewSegmentLoadInfo(ctx, &querypb.GetQueryViewSegmentLoadInfoRequest{CollectionID: 1, SegmentIDs: []int64{1}})
		require.NoError(t, merr.CheckRPCCall(resp, err))
		require.EqualValues(t, 1, resp.Results[0].LoadInfo.CollectionID)
		require.EqualValues(t, 100, resp.Results[0].LoadInfo.TransformStartAfterTimetick)
		require.EqualValues(t, 1024, resp.Results[0].LoadInfo.Stats.InsertBinlogSize)
		require.EqualValues(t, 5, resp.IndexInfoList[0].IndexID)
		coord.UpdateStateCode(commonpb.StateCode_Abnormal)
		resp, err = client.GetQueryViewSegmentLoadInfo(ctx, &querypb.GetQueryViewSegmentLoadInfoRequest{CollectionID: 1, SegmentIDs: []int64{1}})
		require.Error(t, merr.CheckRPCCall(resp, err))
	})
}
