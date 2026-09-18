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
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

// TestFenceConsumeCheckpointStampsOwnTerm proves the takeover claim writes the
// checkpoint back carrying this term and nothing else: the position must be
// preserved, because the claim is a fence, not an advancement.
func TestFenceConsumeCheckpointStampsOwnTerm(t *testing.T) {
	rs := newTestRecoveryStorage(t, &WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1)})
	rs.checkpoint.TimeTick = 42

	var received *streamingpb.WALCheckpoint
	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().SaveRecoverySnapshot(mock.Anything, "test-pchannel",
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
	rs := newTestRecoveryStorage(t, &WALCheckpoint{MessageID: walimplstest.NewTestMessageID(1)})
	rs.checkpoint.Term = 7
	// No catalog expectation: any catalog call fails the test.
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(mock_metastore.NewMockStreamingNodeCataLog(t)))

	require.NoError(t, rs.fenceConsumeCheckpoint(context.Background(), 7))
}
