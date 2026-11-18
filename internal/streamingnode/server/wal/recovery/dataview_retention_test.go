package recovery

import (
	"context"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	mixcoordclient "github.com/milvus-io/milvus/internal/distributed/mixcoord/client"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	streamingtypes "github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestFetchDataViewRetention(t *testing.T) {
	for _, tc := range []struct {
		name     string
		response *datapb.GetDataViewRetentionResponse
		err      error
	}{
		{"success", &datapb.GetDataViewRetentionResponse{Status: merr.Success(), CollectionIds: []int64{1, 1, 2}, VchannelIndices: []uint32{0, 3, 0}, TransformStartAfterTimeticks: []uint64{100, 200, 0}}, nil},
		{"empty", &datapb.GetDataViewRetentionResponse{Status: merr.Success()}, nil},
		{"missing indices", &datapb.GetDataViewRetentionResponse{Status: merr.Success(), CollectionIds: []int64{1}, TransformStartAfterTimeticks: []uint64{100}}, nil},
		{"missing ticks", &datapb.GetDataViewRetentionResponse{Status: merr.Success(), CollectionIds: []int64{1}, VchannelIndices: []uint32{0}}, nil},
		{"extra ticks", &datapb.GetDataViewRetentionResponse{Status: merr.Success(), TransformStartAfterTimeticks: []uint64{100}}, nil},
		{"recovering", &datapb.GetDataViewRetentionResponse{Status: merr.Status(merr.WrapErrServiceNotReadyMsg("recovering"))}, nil},
		{"old coord", nil, merr.ErrServiceUnimplemented},
		{"transport failure", nil, context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			future := syncutil.NewFuture[types.MixCoordClient]()
			future.Set(&mixcoordclient.Client{})
			resource.InitForTest(t, resource.OptMixCoordClient(future))
			patch := mockey.Mock((*mixcoordclient.Client).GetDataViewRetention).To(func(_ *mixcoordclient.Client, ctx context.Context, req *datapb.GetDataViewRetentionRequest, _ ...grpc.CallOption) (*datapb.GetDataViewRetentionResponse, error) {
				require.Equal(t, "p", req.Pchannel)
				deadline, ok := ctx.Deadline()
				require.True(t, ok)
				require.LessOrEqual(t, time.Until(deadline), 5*time.Second)
				if tc.response == nil {
					return nil, tc.err
				}
				wire, err := proto.Marshal(tc.response)
				require.NoError(t, err)
				response := &datapb.GetDataViewRetentionResponse{}
				require.NoError(t, proto.Unmarshal(wire, response))
				return response, tc.err
			}).Build()
			defer patch.UnPatch()
			rs := &recoveryStorageImpl{channel: streamingtypes.PChannelInfo{Name: "p"}}
			floor, err := rs.fetchDataViewRetention(t.Context())
			switch tc.name {
			case "success":
				require.NoError(t, err)
				require.Equal(t, map[string]uint64{"p_1v0": 100, "p_1v3": 200, "p_2v0": 0}, floor)
			case "empty":
				require.NoError(t, err)
				require.Empty(t, floor)
			default:
				require.Error(t, err)
				require.Zero(t, floor)
			}
		})
	}
	future := syncutil.NewFuture[types.MixCoordClient]()
	resource.InitForTest(t, resource.OptMixCoordClient(future))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := (&recoveryStorageImpl{}).fetchDataViewRetention(ctx)
	require.ErrorIs(t, err, context.Canceled)
}
