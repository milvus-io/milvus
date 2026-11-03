package delegator

import (
	"context"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func TestDeleteQueryStreamStrongSnapshot(t *testing.T) {
	paramtable.Init()
	oldWAL := streaming.WAL()
	streaming.SetupNoopWALForTest()
	defer streaming.SetWALForTest(oldWAL)
	for _, tc := range []struct {
		name        string
		mvcc, tsafe uint64
		unavailable bool
		wait        bool
	}{
		{name: "consumed writes need no new heartbeat", tsafe: 120},
		{name: "explicit snapshot still waits", mvcc: 1000, tsafe: 120, wait: true},
		{name: "unconsumed completed write still waits", tsafe: 110, wait: true},
		{name: "unavailable local WAL falls back", tsafe: 120, unavailable: true, wait: true},
		{name: "fallback succeeds after original guarantee", tsafe: 1000, unavailable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			patch := mockey.Mock(mockey.GetMethod(streaming.WAL().Local(), "GetLatestMVCCTimestampIfLocal")).To(func(context.Context, string) (uint64, error) {
				if tc.unavailable {
					return 0, context.Canceled
				}
				return 120, nil // Includes writes completed before this request.
			}).Build()
			defer patch.UnPatch()
			sd, channel := newExternalReadTestDelegator(t)
			sd.skipStreamingForExternalTable = false
			sd.latestTsafe = atomic.NewUint64(tc.tsafe)
			sd.latestRequiredMVCCTimeTick = atomic.NewUint64(0)
			sd.tsCond = syncutil.NewContextCond(&sync.Mutex{})
			req := &querypb.QueryRequest{
				Req: &internalpb.RetrieveRequest{
					ConsistencyLevel:   commonpb.ConsistencyLevel_Strong,
					GuaranteeTimestamp: 1000,
					MvccTimestamp:      tc.mvcc,
				},
				DmlChannels: []string{channel},
			}
			// A canceled context makes an unmet tSafe fail deterministically.
			// A satisfied tSafe proceeds to snapshot selection and then fails
			// at the empty distribution, without relying on timing assertions.
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := sd.QueryStream(ctx, req, nil)
			require.Error(t, err)
			if tc.wait {
				require.ErrorIs(t, err, context.Canceled)
				require.Equal(t, tc.mvcc, req.GetReq().GetMvccTimestamp())
			} else {
				require.NotErrorIs(t, err, context.Canceled)
				require.Equal(t, tc.tsafe, req.GetReq().GetMvccTimestamp())
			}
			require.Equal(t, uint64(1000), sd.latestRequiredMVCCTimeTick.Load())
		})
	}
}
