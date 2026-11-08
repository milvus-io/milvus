package coordview

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

func TestSchemaHandoffRetainsPublishedUp(t *testing.T) {
	catalog, syncer := newMockCatalog(), newMockSyncer()
	mgr := newTestManager(t, catalog, syncer)
	up := func(dataVersion int64, schema int32) qviews.QueryViewVersion {
		require.NoError(t, mgr.AddPreparing(t.Context(), testBuilder(dataVersion, 1, 1).SetSchemaVersion(schema)))
		version := testVersion(dataVersion, 1, 1)
		simulateNodeResponse(t, syncer, testQN1, version, qviews.QueryViewStateReady)
		simulateNodeResponse(t, syncer, testSN, version, qviews.QueryViewStateReady)
		simulateNodeResponse(t, syncer, testSN, version, qviews.QueryViewStateUp)
		return version
	}
	old := up(1, 0)
	mgr.PublishSchema(0)
	current := up(2, 1)
	require.Equal(t, qviews.QueryViewStateUp, mgr.views[old].State())
	require.Equal(t, qviews.QueryViewStateUp, mgr.views[current].State())
	require.Len(t, mgr.Stats().UpBySchema, 2)

	// Same-schema replacement still retires its predecessor.
	replacement := up(3, 1)
	require.Equal(t, qviews.QueryViewStateDown, mgr.views[current].State())
	require.Equal(t, qviews.QueryViewStateUp, mgr.views[old].State())
	mgr.PublishSchema(1)
	mgr.waitFlush()
	require.Equal(t, qviews.QueryViewStateDown, mgr.views[old].State())
	require.Equal(t, qviews.QueryViewStateUp, mgr.views[replacement].State())
	mgr.PublishSchema(0) // replay cannot lower the publication fence
	require.EqualValues(t, 1, *mgr.publishedSchema)
	require.Len(t, mgr.Stats().UpBySchema, 1)
}

func TestSchemaHandoffRecoveryAndRelease(t *testing.T) {
	old := buildTestViewWithVersion(1, 1, 1, 1)
	next := buildTestViewWithVersion(1, 2, 1, 1)
	old.Meta.State = viewpb.QueryViewState_QueryViewStateUp
	next.Meta.State = viewpb.QueryViewState_QueryViewStateUp
	old.Meta.SchemaVersion = 0
	next.Meta.SchemaVersion = 1
	mgr := newTestManager(t, newMockCatalog(), newMockSyncer(), old, next)
	require.Len(t, mgr.Stats().UpBySchema, 2, "recovery must retain both until public metadata is recovered")
	require.NoError(t, mgr.RequestRelease(t.Context()))
	require.Empty(t, mgr.Stats().UpBySchema, "release drains every serving generation")
	require.Nil(t, mgr.upView)
}
