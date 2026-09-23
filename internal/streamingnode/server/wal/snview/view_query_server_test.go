package snview

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewquery"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func TestPChannelViewQueryServerDelegatesToWALTaskProvider(t *testing.T) {
	raw := &fakeViewQueryWAL{
		searchTasks: &fakeViewQuerySearchTasks{tasks: []viewquery.SearchSegmentTask{struct{}{}}},
	}
	manager := &fakeViewQueryWALManager{
		wal: raw,
	}
	scheduler := &fakeViewQueryScheduler{searchResult: &internalpb.SearchResults{}}
	server := NewPChannelViewQueryServer(manager, scheduler)
	req := &viewpb.SearchOnViewRequest{
		LegacyReq: &internalpb.SearchRequest{CollectionID: 10},
		ShardId:   (&viewpb.ShardID{ReplicaId: 1, Vchannel: "p0_100v0"}),
		Version:   testViewQueryVersion().IntoProto(),
		Mvcc:      &viewpb.QueryPlanMVCC{GrowingTimetick: 10, TransformingTimetick: 9},
	}

	resp, err := server.SearchOnView(newIncomingViewSyncContext(types.PChannelInfo{
		Name:       "p0",
		Term:       7,
		AccessMode: types.AccessModeRW,
	}), req)

	require.NoError(t, err)
	assert.Same(t, scheduler.searchResult, resp.GetLegacyResults())
	assert.Equal(t, types.PChannelInfo{Name: "p0", Term: 7, AccessMode: types.AccessModeRW}, manager.channel)
	assert.Equal(t, qviews.ShardID{ReplicaID: 1, VChannel: "p0_100v0"}, raw.searchShardID)
	assert.Equal(t, 1, raw.searchTasks.releaseCount)
}

func TestPChannelViewQueryServerDelegatesToWrappedWALTaskProvider(t *testing.T) {
	raw := &fakeViewQueryWAL{
		searchTasks: &fakeViewQuerySearchTasks{tasks: []viewquery.SearchSegmentTask{struct{}{}}},
	}
	manager := &fakeViewQueryWALManager{
		wal: wrappedTestWAL{WAL: raw, raw: raw},
	}
	scheduler := &fakeViewQueryScheduler{searchResult: &internalpb.SearchResults{}}
	server := NewPChannelViewQueryServer(manager, scheduler)
	req := &viewpb.SearchOnViewRequest{
		LegacyReq: &internalpb.SearchRequest{CollectionID: 10},
		ShardId:   (&viewpb.ShardID{ReplicaId: 1, Vchannel: "p0_100v0"}),
		Version:   testViewQueryVersion().IntoProto(),
		Mvcc:      &viewpb.QueryPlanMVCC{GrowingTimetick: 10, TransformingTimetick: 9},
	}

	resp, err := server.SearchOnView(newIncomingViewSyncContext(types.PChannelInfo{
		Name:       "p0",
		Term:       7,
		AccessMode: types.AccessModeRW,
	}), req)

	require.NoError(t, err)
	assert.Same(t, scheduler.searchResult, resp.GetLegacyResults())
	assert.Equal(t, types.PChannelInfo{Name: "p0", Term: 7, AccessMode: types.AccessModeRW}, manager.channel)
	assert.Equal(t, qviews.ShardID{ReplicaID: 1, VChannel: "p0_100v0"}, raw.searchShardID)
	assert.Equal(t, 1, raw.searchTasks.releaseCount)
}

func TestPChannelViewQueryServerRejectsMismatchedPChannelMetadata(t *testing.T) {
	manager := &fakeViewQueryWALManager{wal: &fakeViewQueryWAL{}}
	server := NewPChannelViewQueryServer(manager, &fakeViewQueryScheduler{})

	_, err := server.SearchOnView(newIncomingViewSyncContext(types.PChannelInfo{
		Name:       "other",
		Term:       7,
		AccessMode: types.AccessModeRW,
	}), &viewpb.SearchOnViewRequest{
		LegacyReq: &internalpb.SearchRequest{CollectionID: 10},
		ShardId:   (&viewpb.ShardID{ReplicaId: 1, Vchannel: "p0_100v0"}),
		Version:   testViewQueryVersion().IntoProto(),
		Mvcc:      &viewpb.QueryPlanMVCC{GrowingTimetick: 10, TransformingTimetick: 9},
	})

	require.Error(t, err)
	assert.Equal(t, codes.Unknown, status.Code(err))
}

type fakeViewQueryWALManager struct {
	channel types.PChannelInfo
	wal     wal.WAL
	err     error
}

func testViewQueryVersion() qviews.QueryViewVersion {
	return qviews.QueryViewVersion{
		DataVersion:  qviews.DataVersion{StreamingVersion: 1, CompactVersion: 1},
		QueryVersion: 1,
	}
}

func (m *fakeViewQueryWALManager) GetAvailableWAL(channel types.PChannelInfo) (wal.WAL, error) {
	m.channel = channel
	return m.wal, m.err
}

type fakeViewQueryWAL struct {
	wal.WAL
	searchTasks   *fakeViewQuerySearchTasks
	searchShardID qviews.ShardID
	queryTasks    *fakeViewQueryQueryTasks
	queryShardID  qviews.ShardID
}

func (w *fakeViewQueryWAL) AcquireSearchSegmentTasks(
	_ context.Context,
	shardID qviews.ShardID,
	_ qviews.QueryViewVersion,
	_ *viewpb.QueryPlanMVCC,
	_ *internalpb.SearchRequest,
) (viewquery.SearchSegmentTasks, error) {
	w.searchShardID = shardID
	return w.searchTasks, nil
}

func (w *fakeViewQueryWAL) AcquireQuerySegmentTasks(
	_ context.Context,
	shardID qviews.ShardID,
	_ qviews.QueryViewVersion,
	_ *viewpb.QueryPlanMVCC,
	_ *internalpb.RetrieveRequest,
) (viewquery.QuerySegmentTasks, error) {
	w.queryShardID = shardID
	return w.queryTasks, nil
}

type fakeViewQuerySearchTasks struct {
	tasks        []viewquery.SearchSegmentTask
	releaseCount int
}

func (t *fakeViewQuerySearchTasks) Tasks() []viewquery.SearchSegmentTask {
	return t.tasks
}

func (t *fakeViewQuerySearchTasks) Release() {
	t.releaseCount++
}

type fakeViewQueryQueryTasks struct {
	tasks        []viewquery.QuerySegmentTask
	releaseCount int
}

func (t *fakeViewQueryQueryTasks) Tasks() []viewquery.QuerySegmentTask {
	return t.tasks
}

func (t *fakeViewQueryQueryTasks) Release() {
	t.releaseCount++
}

type fakeViewQueryScheduler struct {
	searchResult *internalpb.SearchResults
	queryResult  *internalpb.RetrieveResults
}

func (s *fakeViewQueryScheduler) Search(context.Context, viewquery.SearchSegmentTasks) (*internalpb.SearchResults, error) {
	return s.searchResult, nil
}

func (s *fakeViewQueryScheduler) Query(context.Context, viewquery.QuerySegmentTasks) (*internalpb.RetrieveResults, error) {
	return s.queryResult, nil
}
