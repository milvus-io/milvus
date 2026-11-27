package ddl

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/channelmgr"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

type flushMixCoordClient struct{ types.MixCoordClient }

func TestFlushTaskDataCoordCompletion(t *testing.T) {
	for _, fail := range []bool{false, true} {
		name := "success"
		if fail {
			name = "rpc_failure"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			cache := NewMockCache(t)
			cache.EXPECT().GetCollectionID(mock.Anything, "db", mock.Anything).Return(int64(100), nil).Maybe()
			channels := channelmgr.NewMockChannelsMgr(t)
			channels.EXPECT().GetVChannels(int64(100)).Return([]string{"v1"}, nil).Maybe()
			task := &FlushTask{
				baseTask: baseTask{MetaCache: cache},
				ctx:      ctx,
				mixCoord: &flushMixCoordClient{},
				chMgr:    channels,
				FlushRequest: &milvuspb.FlushRequest{
					Base:            commonpbutil.NewMsgBase(),
					DbName:          "db",
					CollectionNames: []string{"a", "b"},
				},
			}
			// The streaming-node flush path appends a manual-flush message to
			// the WAL; stub it out so the test exercises the DataCoord RPC and
			// result assembly without a running WAL.
			patchWAL := mockey.Mock(sendManualFlushToWAL).Return(nil, nil).Build()
			defer patchWAL.UnPatch()
			calls := 0
			patchFlush := mockey.Mock((*flushMixCoordClient).Flush).To(func(_ *flushMixCoordClient, _ context.Context, req *datapb.FlushRequest, _ ...grpc.CallOption) (*datapb.FlushResponse, error) {
				calls++
				require.EqualValues(t, 100, req.GetCollectionID())
				if fail {
					return nil, merr.WrapErrServiceUnavailable("flush failed")
				}
				return &datapb.FlushResponse{Status: merr.Success(), FlushSegmentIDs: []int64{200}, TimeOfSeal: 123}, nil
			}).Build()
			defer patchFlush.UnPatch()
			err := task.Execute(ctx)
			if fail {
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
				require.Equal(t, 1, calls)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 2, calls)
			// The streaming flush derives the seal time from the flush TS
			// rather than the DataCoord response.
			timeOfSeal, _ := tsoutil.ParseTS(task.BeginTs())
			for _, collection := range task.CollectionNames {
				require.Contains(t, task.result.CollSegIDs, collection)
				require.Empty(t, task.result.CollSegIDs[collection].GetData())
				require.Equal(t, []int64{200}, task.result.FlushCollSegIDs[collection].GetData())
				require.Contains(t, task.result.CollFlushTs, collection)
				require.Zero(t, task.result.CollFlushTs[collection])
				require.EqualValues(t, timeOfSeal.Unix(), task.result.CollSealTimes[collection])
			}
		})
	}
}
