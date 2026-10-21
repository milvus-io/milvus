package proxy

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type localFlushWALAccesser struct{ streaming.WALAccesser }

func TestSendManualFlushToWALLocalContract(t *testing.T) {
	for _, appendErr := range []error{nil, context.DeadlineExceeded} {
		t.Run(map[bool]string{true: "success", false: "append failure"}[appendErr == nil], func(t *testing.T) {
			accesser := &localFlushWALAccesser{}
			walPatch := mockey.Mock(streaming.WAL).Return(accesser).Build()
			defer walPatch.UnPatch()
			extra, err := anypb.New(&message.ManualFlushExtraResponse{SegmentIds: []int64{11, 12}})
			require.NoError(t, err)
			calls := 0
			appendPatch := mockey.Mock((*localFlushWALAccesser).RawAppend).To(func(_ *localFlushWALAccesser, _ context.Context, msg message.MutableMessage, opts ...streaming.AppendOption) (*types.AppendResult, error) {
				calls++
				require.True(t, msg.IsUnreplicable())
				require.Nil(t, msg.ReplicateHeader())
				require.Nil(t, msg.BroadcastHeader())
				require.Equal(t, "p0_1v0", msg.VChannel())
				header := message.MustAsMutableManualFlushMessageV2(msg).Header()
				require.EqualValues(t, 1, header.CollectionId)
				require.EqualValues(t, 100, header.FlushTs)
				require.Len(t, opts, 1)
				require.EqualValues(t, 100, opts[0].BarrierTimeTick)
				return &types.AppendResult{TimeTick: 101, Extra: extra}, appendErr
			}).Build()
			defer appendPatch.UnPatch()
			ids, err := sendManualFlushToWAL(context.Background(), 1, "p0_1v0", 100)
			require.ErrorIs(t, err, appendErr)
			require.Equal(t, 1, calls)
			if appendErr == nil {
				require.Equal(t, []int64{11, 12}, ids)
			} else {
				require.Nil(t, ids)
			}
		})
	}
}
