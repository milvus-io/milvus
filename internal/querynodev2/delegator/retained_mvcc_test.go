package delegator

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/timetick/mvcc"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

func TestRetainedMVCCConsistencyFallbacks(t *testing.T) {
	mockey.PatchConvey("only ordinary local strong reads use retained targets", t, func() {
		cm := mvcc.NewMVCCManager(100)
		cm.ApplyRecoveryBarrier("v1", 100)
		cm.UpdateMVCC(message.NewInsertMessageBuilderV1().WithVChannel("v1").WithHeader(&message.InsertMessageHeader{}).WithBody(&message.InsertRequest{}).MustBuildMutable().WithTimeTick(120))
		cm.UpdateMVCC(message.NewTimeTickMessageBuilderV1().WithAllVChannel().WithHeader(&message.TimeTickMessageHeader{}).WithBody(&message.TimeTickMsg{}).MustBuildMutable().WithTimeTick(300))
		oldWAL := streaming.WAL()
		streaming.SetupNoopWALForTest()
		defer streaming.SetWALForTest(oldWAL)
		var unavailable bool
		calls := 0
		mockey.Mock(mockey.GetMethod(streaming.WAL().Local(), "GetLatestMVCCTimestampIfLocal")).To(func(_ context.Context, vc string) (uint64, error) {
			calls++
			if unavailable {
				return 0, context.Canceled
			}
			return cm.GetMVCCOfVChannel(vc).Timetick, nil
		}).Build()
		for _, tc := range []struct {
			name                  string
			consistency           commonpb.ConsistencyLevel
			snapshot              uint64
			iterator, unavailable bool
			target                uint64
			calls                 int
		}{
			{name: "ordinary strong", consistency: commonpb.ConsistencyLevel_Strong, target: 120, calls: 1},
			{name: "explicit snapshot", consistency: commonpb.ConsistencyLevel_Strong, snapshot: 150, target: 1000},
			{name: "iterator", consistency: commonpb.ConsistencyLevel_Strong, iterator: true, target: 1000},
			{name: "bounded", consistency: commonpb.ConsistencyLevel_Bounded, target: 1000},
			{name: "eventual", consistency: commonpb.ConsistencyLevel_Eventually, target: 1000},
			{name: "unavailable local RW WAL", consistency: commonpb.ConsistencyLevel_Strong, unavailable: true, target: 1000, calls: 1},
		} {
			t.Run(tc.name, func(t *testing.T) {
				calls = 0
				unavailable = tc.unavailable
				sd := &shardDelegator{vchannelName: "v1", latestRequiredMVCCTimeTick: atomic.NewUint64(90)}
				require.Equal(t, tc.target, sd.speedupGuranteeTS(context.Background(), tc.consistency, 1000, tc.snapshot, tc.iterator))
				require.Equal(t, uint64(1000), sd.latestRequiredMVCCTimeTick.Load(), "periodic fallback must still see the original guarantee")
				require.Equal(t, tc.calls, calls)
			})
		}
	})
}
