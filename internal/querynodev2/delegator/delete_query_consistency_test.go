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

func TestDeleteQueryStreamSnapshot(t *testing.T) {
	paramtable.Init()
	oldWAL := streaming.WAL()
	streaming.SetupNoopWALForTest()
	defer streaming.SetWALForTest(oldWAL)
	for _, tc := range []struct {
		name                   string
		consistency            commonpb.ConsistencyLevel
		guarantee, mvcc, tsafe uint64
		unavailable            bool
		wait                   bool
	}{
		{name: "consumed writes need no new heartbeat", guarantee: 1000, tsafe: 120},
		{name: "explicit snapshot still waits", guarantee: 1000, mvcc: 1000, tsafe: 120, wait: true},
		{name: "unconsumed completed write still waits", guarantee: 1000, tsafe: 110, wait: true},
		{name: "unavailable local WAL falls back", guarantee: 1000, tsafe: 120, unavailable: true, wait: true},
		{name: "fallback succeeds after original guarantee", guarantee: 1000, tsafe: 1000, unavailable: true},
		{name: "bounded uses consumed snapshot", consistency: commonpb.ConsistencyLevel_Bounded, guarantee: 100, tsafe: 110},
		{name: "bounded still waits for guarantee", consistency: commonpb.ConsistencyLevel_Bounded, guarantee: 100, tsafe: 90, wait: true},
		{name: "eventual uses consumed snapshot", consistency: commonpb.ConsistencyLevel_Eventually, guarantee: 1, tsafe: 110},
		{name: "session does not lower guarantee", consistency: commonpb.ConsistencyLevel_Session, guarantee: 1000, tsafe: 120, wait: true},
		{name: "session can use later snapshot", consistency: commonpb.ConsistencyLevel_Session, guarantee: 1000, tsafe: 1100},
		{name: "customized does not lower guarantee", consistency: commonpb.ConsistencyLevel_Customized, guarantee: 1000, tsafe: 120, wait: true},
		{name: "customized uses consumed snapshot", consistency: commonpb.ConsistencyLevel_Customized, guarantee: 1000, tsafe: 1000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			walCalls := 0
			patch := mockey.Mock(mockey.GetMethod(streaming.WAL().Local(), "GetLatestMVCCTimestampIfLocal")).To(func(context.Context, string) (uint64, error) {
				walCalls++
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
					ConsistencyLevel:   tc.consistency,
					GuaranteeTimestamp: tc.guarantee,
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
			require.Equal(t, tc.guarantee, sd.latestRequiredMVCCTimeTick.Load())
			if tc.consistency != commonpb.ConsistencyLevel_Strong {
				require.Zero(t, walCalls, "non-Strong reads must retain their guarantee")
				require.Equal(t, tc.guarantee, req.GetReq().GetGuaranteeTimestamp())
			}
		})
	}
}
