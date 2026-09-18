//go:build test

package recovery

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/mocks/mock_storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// TestFenceConsumeCheckpointStampsOwnTerm proves the takeover claim writes the
// checkpoint back carrying this term and nothing else: the position must be
// preserved, because the claim is a fence, not an advancement.
func TestFenceConsumeCheckpointStampsOwnTerm(t *testing.T) {
	rs := newTestRecoveryStorage(t)
	rs.checkpoint.TimeTick = 42

	var received *streamingpb.WALCheckpoint
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().SaveRecoverySnapshot(mock.Anything, "test_channel",
		mock.MatchedBy(func(snapshot *metastore.WALRecoverySnapshot) bool {
			require.NotNil(t, snapshot)
			require.NotNil(t, snapshot.ConsumeCheckpoint)
			received = snapshot.ConsumeCheckpoint
			return true
		})).Return(nil)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog))

	require.NoError(t, rs.fenceConsumeCheckpoint(context.Background(), 3))
	require.NotNil(t, received)
	assert.Equal(t, int64(3), received.GetTerm(), "the claim stamps this term")
	assert.Equal(t, uint64(42), received.GetTimeTick(), "the claim leaves the position alone")
	// Later advancements carry the term, so the compare-and-swap keeps
	// accepting this publisher.
	assert.Equal(t, int64(3), rs.checkpoint.Term)
}

// TestFenceConsumeCheckpointSkipsWhenAlreadyClaimed proves a reopen that does
// not change ownership writes nothing: the recorded term already matches.
func TestFenceConsumeCheckpointSkipsWhenAlreadyClaimed(t *testing.T) {
	rs := newTestRecoveryStorage(t)
	rs.checkpoint.Term = 7
	// No catalog expectation: any catalog call fails the test.
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(mock_metastore.NewMockStreamingNodeCataLog(t)))

	require.NoError(t, rs.fenceConsumeCheckpoint(context.Background(), 7))
}

// TestFenceConsumeCheckpointPrecedesSummaryRecovery pins the ordering the
// fence depends on: the claim must land BEFORE the summary store is read.
//
// Claiming after the probe leaves a window in which a superseded publisher's
// chunk is neither adopted by the probe nor blocked by the compare-and-swap,
// so it can still advance the checkpoint and truncate the WAL past records
// that then exist nowhere a recovery will look. Moving the claim later is the
// regression this test exists to catch.
func TestFenceConsumeCheckpointPrecedesSummaryRecovery(t *testing.T) {
	paramtable.Init()
	paramtable.Get().Save(paramtable.Get().StreamingCfg.IdempotencyEnabled.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().StreamingCfg.IdempotencyEnabled.Key)

	var order []string
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().SaveRecoverySnapshot(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(context.Context, string, *metastore.WALRecoverySnapshot) error {
			order = append(order, "claim")
			return nil
		}).Maybe()
	readStore := func() {
		// Record only the first read: the point is which side ran first.
		for _, step := range order {
			if step == "read-summary" {
				return
			}
		}
		order = append(order, "read-summary")
	}
	cm := mock_storage.NewMockChunkManager(t)
	cm.EXPECT().RootPath().Return("/root").Maybe()
	cm.EXPECT().Exist(mock.Anything, mock.Anything).
		RunAndReturn(func(context.Context, string) (bool, error) {
			readStore()
			return false, nil
		}).Maybe()
	cm.EXPECT().WalkWithPrefix(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(context.Context, string, bool, storage.ChunkObjectWalkFunc) error {
			readStore()
			return nil
		}).Maybe()
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptChunkManager(cm))

	rs := newRecoveryStorage(types.PChannelInfo{Name: "test_channel", Term: 5}, &WALCheckpoint{
		MessageID: rmq.NewRmqID(0),
		TimeTick:  0,
	})
	rs.segments = make(map[int64]*segmentRecoveryInfo)
	rs.vchannels = make(map[string]*vchannelRecoveryInfo)
	rs.SetLogger(resource.Resource().Logger())

	// Drive the real entry point, so this pins the production order rather
	// than the order this test happens to call things in.
	catalog.EXPECT().ListVChannel(mock.Anything, mock.Anything).Return(nil, nil)
	catalog.EXPECT().ListSegmentAssignment(mock.Anything, mock.Anything).Return(nil, nil)
	require.NoError(t, rs.recoverRecoveryInfoFromMeta(
		context.Background(), types.PChannelInfo{Name: "test_channel", Term: 5}, nil))

	require.NotEmpty(t, order)
	assert.Equal(t, "claim", order[0], "the consume checkpoint must be claimed before the summary store is read")
	assert.Contains(t, order, "read-summary", "the summary recovery must have run")
}
