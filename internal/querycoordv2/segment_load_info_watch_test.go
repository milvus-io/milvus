package querycoordv2

import (
	"context"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	componenttypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestCalculateQueryViewSegmentLoadInfoRevisionIsStable(t *testing.T) {
	loadInfo := &querypb.SegmentLoadInfo{
		CollectionID:       100,
		SegmentID:          1000,
		BinlogPaths:        testRevisionFieldBinlogs("binlog"),
		Statslogs:          testRevisionFieldBinlogs("stats"),
		Deltalogs:          testRevisionFieldBinlogs("delta"),
		Bm25Logs:           testRevisionFieldBinlogs("bm25"),
		CompactionFrom:     []int64{1002, 1001},
		ChildManifestPaths: []string{"manifest/child/2", "manifest/child/1"},
		TextStatsLogs: map[int64]*datapb.TextIndexStats{
			102: {FieldID: 102, BuildID: 42, Files: []string{"text/102/2", "text/102/1"}},
			101: {FieldID: 101, BuildID: 41, Files: []string{"text/101/2", "text/101/1"}},
		},
		JsonKeyStatsLogs: map[int64]*datapb.JsonKeyStats{
			102: {FieldID: 102, BuildID: 32, Files: []string{"json/102/2", "json/102/1"}},
			101: {FieldID: 101, BuildID: 31, Files: []string{"json/101/2", "json/101/1"}},
		},
		IndexInfos: []*querypb.FieldIndexInfo{
			{
				FieldID:        102,
				IndexID:        12,
				BuildID:        22,
				IndexParams:    []*commonpb.KeyValuePair{{Key: "metric_type", Value: "IP"}, {Key: "index_type", Value: "HNSW"}},
				IndexFilePaths: []string{"index/12/2", "index/12/1"},
			},
			{
				FieldID:        101,
				IndexID:        11,
				BuildID:        21,
				IndexParams:    []*commonpb.KeyValuePair{{Key: "metric_type", Value: "COSINE"}, {Key: "index_type", Value: "HNSW"}},
				IndexFilePaths: []string{"index/11/2", "index/11/1"},
			},
		},
	}
	indexes := []*indexpb.IndexInfo{
		{
			CollectionID:    100,
			FieldID:         102,
			IndexID:         12,
			TypeParams:      []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}, {Key: "metric_type", Value: "IP"}},
			IndexParams:     []*commonpb.KeyValuePair{{Key: "M", Value: "16"}, {Key: "efConstruction", Value: "200"}},
			UserIndexParams: []*commonpb.KeyValuePair{{Key: "mmap.enabled", Value: "false"}, {Key: "index_type", Value: "HNSW"}},
		},
		{
			CollectionID:    100,
			FieldID:         101,
			IndexID:         11,
			TypeParams:      []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}, {Key: "metric_type", Value: "COSINE"}},
			IndexParams:     []*commonpb.KeyValuePair{{Key: "M", Value: "8"}, {Key: "efConstruction", Value: "100"}},
			UserIndexParams: []*commonpb.KeyValuePair{{Key: "mmap.enabled", Value: "true"}, {Key: "index_type", Value: "HNSW"}},
		},
	}

	reorderedLoadInfo := proto.Clone(loadInfo).(*querypb.SegmentLoadInfo)
	reverseRevisionFieldBinlogs(reorderedLoadInfo.BinlogPaths)
	reverseRevisionFieldBinlogs(reorderedLoadInfo.Statslogs)
	reverseRevisionFieldBinlogs(reorderedLoadInfo.Deltalogs)
	reverseRevisionFieldBinlogs(reorderedLoadInfo.Bm25Logs)
	slices.Reverse(reorderedLoadInfo.CompactionFrom)
	slices.Reverse(reorderedLoadInfo.ChildManifestPaths)
	slices.Reverse(reorderedLoadInfo.IndexInfos)
	for _, info := range reorderedLoadInfo.IndexInfos {
		slices.Reverse(info.IndexParams)
		slices.Reverse(info.IndexFilePaths)
	}
	reorderedLoadInfo.JsonKeyStatsLogs = map[int64]*datapb.JsonKeyStats{
		101: proto.Clone(loadInfo.JsonKeyStatsLogs[101]).(*datapb.JsonKeyStats),
		102: proto.Clone(loadInfo.JsonKeyStatsLogs[102]).(*datapb.JsonKeyStats),
	}
	for _, stats := range reorderedLoadInfo.JsonKeyStatsLogs {
		slices.Reverse(stats.Files)
	}
	for _, stats := range reorderedLoadInfo.TextStatsLogs {
		slices.Reverse(stats.Files)
	}
	reorderedIndexes := proto.Clone(&querypb.QueryViewSegmentLoadInfoSnapshot{IndexInfoList: indexes}).(*querypb.QueryViewSegmentLoadInfoSnapshot).IndexInfoList
	slices.Reverse(reorderedIndexes)
	for _, index := range reorderedIndexes {
		slices.Reverse(index.TypeParams)
		slices.Reverse(index.IndexParams)
		slices.Reverse(index.UserIndexParams)
	}

	expected := calculateQueryViewSegmentLoadInfoRevision(loadInfo, indexes)
	actual := calculateQueryViewSegmentLoadInfoRevision(reorderedLoadInfo, reorderedIndexes)
	assert.Equal(t, expected, actual)

	changedIndexes := proto.Clone(&querypb.QueryViewSegmentLoadInfoSnapshot{IndexInfoList: reorderedIndexes}).(*querypb.QueryViewSegmentLoadInfoSnapshot).IndexInfoList
	changedIndexes[0].IndexParams[0].Value = "changed"
	changed := calculateQueryViewSegmentLoadInfoRevision(reorderedLoadInfo, changedIndexes)
	assert.NotEqual(t, expected, changed)

	for name, mutate := range map[string]func(*querypb.SegmentLoadInfo, []*indexpb.IndexInfo){
		"manifest": func(loadInfo *querypb.SegmentLoadInfo, _ []*indexpb.IndexInfo) {
			loadInfo.ManifestPath = "manifest/v2"
		},
		"json stats": func(loadInfo *querypb.SegmentLoadInfo, _ []*indexpb.IndexInfo) {
			loadInfo.JsonKeyStatsLogs[101].BuildID++
		},
		"segment index build": func(loadInfo *querypb.SegmentLoadInfo, _ []*indexpb.IndexInfo) {
			loadInfo.IndexInfos[0].BuildID++
		},
		"segment index file": func(loadInfo *querypb.SegmentLoadInfo, _ []*indexpb.IndexInfo) {
			loadInfo.IndexInfos[0].IndexFilePaths[0] = "index/changed"
		},
		"collection index ID": func(_ *querypb.SegmentLoadInfo, indexes []*indexpb.IndexInfo) {
			indexes[0].IndexID++
		},
	} {
		t.Run(name, func(t *testing.T) {
			changedLoadInfo := proto.Clone(loadInfo).(*querypb.SegmentLoadInfo)
			changedIndexes := proto.Clone(&querypb.QueryViewSegmentLoadInfoSnapshot{IndexInfoList: indexes}).(*querypb.QueryViewSegmentLoadInfoSnapshot).IndexInfoList
			mutate(changedLoadInfo, changedIndexes)
			assert.NotEqual(t, expected, calculateQueryViewSegmentLoadInfoRevision(changedLoadInfo, changedIndexes))
		})
	}
}

func testRevisionFieldBinlogs(prefix string) []*datapb.FieldBinlog {
	return []*datapb.FieldBinlog{
		{
			FieldID:     102,
			ChildFields: []int64{202, 102},
			Format:      "parquet",
			Binlogs: []*datapb.Binlog{
				{LogID: 22, LogPath: prefix + "/102/2", TimestampFrom: 20, TimestampTo: 29},
				{LogID: 21, LogPath: prefix + "/102/1", TimestampFrom: 10, TimestampTo: 19},
			},
		},
		{
			FieldID:     101,
			ChildFields: []int64{201, 101},
			Format:      "vortex",
			Binlogs: []*datapb.Binlog{
				{LogID: 12, LogPath: prefix + "/101/2", TimestampFrom: 20, TimestampTo: 29},
				{LogID: 11, LogPath: prefix + "/101/1", TimestampFrom: 10, TimestampTo: 19},
			},
		},
	}
}

func reverseRevisionFieldBinlogs(fieldBinlogs []*datapb.FieldBinlog) {
	slices.Reverse(fieldBinlogs)
	for _, fieldBinlog := range fieldBinlogs {
		slices.Reverse(fieldBinlog.ChildFields)
		slices.Reverse(fieldBinlog.Binlogs)
	}
}

func TestCalculateQueryViewSegmentLoadInfoRevisionDoesNotMutateInputs(t *testing.T) {
	loadInfo := &querypb.SegmentLoadInfo{
		CollectionID: 100,
		SegmentID:    1000,
		IndexInfos: []*querypb.FieldIndexInfo{
			{
				FieldID:        102,
				IndexID:        12,
				BuildID:        22,
				IndexParams:    []*commonpb.KeyValuePair{{Key: "metric_type", Value: "IP"}, {Key: "index_type", Value: "HNSW"}},
				IndexFilePaths: []string{"index/12/2", "index/12/1"},
			},
			{
				FieldID:        101,
				IndexID:        11,
				BuildID:        21,
				IndexParams:    []*commonpb.KeyValuePair{{Key: "metric_type", Value: "COSINE"}, {Key: "index_type", Value: "HNSW"}},
				IndexFilePaths: []string{"index/11/2", "index/11/1"},
			},
		},
	}
	indexes := []*indexpb.IndexInfo{
		{
			CollectionID:    100,
			FieldID:         102,
			IndexID:         12,
			TypeParams:      []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}, {Key: "metric_type", Value: "IP"}},
			IndexParams:     []*commonpb.KeyValuePair{{Key: "M", Value: "16"}, {Key: "efConstruction", Value: "200"}},
			UserIndexParams: []*commonpb.KeyValuePair{{Key: "mmap.enabled", Value: "false"}, {Key: "index_type", Value: "HNSW"}},
		},
		{
			CollectionID:    100,
			FieldID:         101,
			IndexID:         11,
			TypeParams:      []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}, {Key: "metric_type", Value: "COSINE"}},
			IndexParams:     []*commonpb.KeyValuePair{{Key: "M", Value: "8"}, {Key: "efConstruction", Value: "100"}},
			UserIndexParams: []*commonpb.KeyValuePair{{Key: "mmap.enabled", Value: "true"}, {Key: "index_type", Value: "HNSW"}},
		},
	}
	originalLoadInfo := proto.Clone(loadInfo)
	originalIndexes := proto.Clone(&querypb.QueryViewSegmentLoadInfoSnapshot{IndexInfoList: indexes})

	calculateQueryViewSegmentLoadInfoRevision(loadInfo, indexes)

	assert.True(t, proto.Equal(originalLoadInfo, loadInfo))
	assert.True(t, proto.Equal(originalIndexes, &querypb.QueryViewSegmentLoadInfoSnapshot{IndexInfoList: indexes}))
}

func TestCalculateQueryViewSegmentLoadInfoRevisionIsStableForEqualSortKeys(t *testing.T) {
	loadInfo := &querypb.SegmentLoadInfo{
		CollectionID: 100,
		SegmentID:    1000,
		IndexInfos: []*querypb.FieldIndexInfo{
			{
				FieldID:     101,
				IndexID:     11,
				BuildID:     21,
				IndexName:   "idx",
				IndexParams: []*commonpb.KeyValuePair{{Key: "mmap.enabled", Value: "false"}},
			},
			{
				FieldID:     101,
				IndexID:     11,
				BuildID:     21,
				IndexName:   "idx",
				IndexParams: []*commonpb.KeyValuePair{{Key: "mmap.enabled", Value: "true"}},
			},
		},
	}
	indexes := []*indexpb.IndexInfo{
		{
			CollectionID:    100,
			FieldID:         101,
			IndexID:         11,
			IndexName:       "idx",
			UserIndexParams: []*commonpb.KeyValuePair{{Key: "mmap.enabled", Value: "false"}},
		},
		{
			CollectionID:    100,
			FieldID:         101,
			IndexID:         11,
			IndexName:       "idx",
			UserIndexParams: []*commonpb.KeyValuePair{{Key: "mmap.enabled", Value: "true"}},
		},
	}
	reorderedLoadInfo := proto.Clone(loadInfo).(*querypb.SegmentLoadInfo)
	slices.Reverse(reorderedLoadInfo.IndexInfos)
	reorderedIndexes := proto.Clone(&querypb.QueryViewSegmentLoadInfoSnapshot{IndexInfoList: indexes}).(*querypb.QueryViewSegmentLoadInfoSnapshot).IndexInfoList
	slices.Reverse(reorderedIndexes)

	assert.Equal(t,
		calculateQueryViewSegmentLoadInfoRevision(loadInfo, indexes),
		calculateQueryViewSegmentLoadInfoRevision(reorderedLoadInfo, reorderedIndexes),
	)

	t.Run("unknown fields", func(t *testing.T) {
		firstParam := &commonpb.KeyValuePair{Key: "same", Value: "same"}
		firstParam.ProtoReflect().SetUnknown([]byte{0x98, 0x06, 0x01})
		secondParam := &commonpb.KeyValuePair{Key: "same", Value: "same"}
		secondParam.ProtoReflect().SetUnknown([]byte{0x98, 0x06, 0x02})
		firstFieldBinlog := &datapb.FieldBinlog{FieldID: 101}
		firstFieldBinlog.ProtoReflect().SetUnknown([]byte{0x98, 0x06, 0x01})
		secondFieldBinlog := &datapb.FieldBinlog{FieldID: 101}
		secondFieldBinlog.ProtoReflect().SetUnknown([]byte{0x98, 0x06, 0x02})

		loadInfo := &querypb.SegmentLoadInfo{
			CollectionID: 100,
			SegmentID:    1000,
			BinlogPaths:  []*datapb.FieldBinlog{firstFieldBinlog, secondFieldBinlog},
		}
		indexes := []*indexpb.IndexInfo{{
			CollectionID:    100,
			FieldID:         101,
			IndexID:         11,
			UserIndexParams: []*commonpb.KeyValuePair{firstParam, secondParam},
		}}
		reorderedLoadInfo := proto.Clone(loadInfo).(*querypb.SegmentLoadInfo)
		slices.Reverse(reorderedLoadInfo.BinlogPaths)
		reorderedIndexes := proto.Clone(&querypb.QueryViewSegmentLoadInfoSnapshot{IndexInfoList: indexes}).(*querypb.QueryViewSegmentLoadInfoSnapshot).IndexInfoList
		slices.Reverse(reorderedIndexes[0].UserIndexParams)

		assert.Equal(t,
			calculateQueryViewSegmentLoadInfoRevision(loadInfo, indexes),
			calculateQueryViewSegmentLoadInfoRevision(reorderedLoadInfo, reorderedIndexes),
		)
	})
}

func TestQueryViewSegmentLoadInfoWatchSession_ClearsSubscriptionsOnStreamClose(t *testing.T) {
	watcher := newQueryViewSegmentLoadInfoWatcher()
	session := &queryViewSegmentLoadInfoWatchSession{
		stream:  &eofSegmentLoadInfoWatchServer{ctx: context.Background()},
		watcher: watcher,
		subscriptions: map[int64]queryViewSegmentLoadInfoSubscription{
			1000: {collectionID: 100, segmentID: 1000},
		},
		notifyCh: make(chan struct{}, 1),
		dirty:    make(map[int64]struct{}),
	}
	watcher.register(session)
	require.NotEmpty(t, watcher.sessions)

	err := session.run()
	require.NoError(t, err)
	assert.Empty(t, session.subscriptions)
	assert.Empty(t, watcher.sessions)
	assert.Empty(t, watcher.bySegment)
}

func TestQueryViewSegmentLoadInfoWatchSession_PushesSnapshotOnNotify(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	segmentID := int64(1000)
	collectionID := int64(100)
	loadInfo := &querypb.SegmentLoadInfo{
		CollectionID: collectionID,
		SegmentID:    segmentID,
		NumOfRows:    10,
	}
	mixCoord := &fakeSegmentLoadInfoWatchMixCoord{
		loadInfo: loadInfo,
		indexes:  []*indexpb.IndexInfo{{CollectionID: collectionID, IndexID: 10}},
	}
	server := &Server{
		ctx:                    ctx,
		mixCoord:               mixCoord,
		segmentLoadInfoWatcher: newQueryViewSegmentLoadInfoWatcher(),
	}
	server.UpdateStateCode(commonpb.StateCode_Healthy)
	stream := newChannelSegmentLoadInfoWatchServer(ctx)

	done := make(chan error, 1)
	go func() {
		done <- server.WatchQueryViewSegmentLoadInfo(stream)
	}()

	stream.recv <- &querypb.WatchQueryViewSegmentLoadInfoRequest{
		Subscribe: []*querypb.WatchQueryViewSegmentLoadInfoSubscription{{
			CollectionID: collectionID,
			SegmentID:    segmentID,
			Revision:     calculateQueryViewSegmentLoadInfoRevision(loadInfo, mixCoord.indexes),
		}},
	}
	assertNoWatchResponse(t, stream.send)

	mixCoord.setLoadInfo(&querypb.SegmentLoadInfo{
		CollectionID: collectionID,
		SegmentID:    segmentID,
		NumOfRows:    20,
	})
	server.NotifyQueryViewSegmentLoadInfoChanged(collectionID, segmentID)

	resp := requireWatchResponse(t, stream.send)
	require.True(t, merr.Ok(resp.GetStatus()))
	require.Len(t, resp.GetSnapshots(), 1)
	assert.Equal(t, segmentID, resp.GetSnapshots()[0].GetSegmentID())
	assert.Equal(t, int64(20), resp.GetSnapshots()[0].GetLoadInfo().GetNumOfRows())

	stream.closeRecv()
	require.NoError(t, <-done)

	mixCoord.setLoadInfo(&querypb.SegmentLoadInfo{
		CollectionID: collectionID,
		SegmentID:    segmentID,
		NumOfRows:    30,
	})
	server.NotifyQueryViewSegmentLoadInfoChanged(collectionID, segmentID)
	assertNoWatchResponse(t, stream.send)
}

type eofSegmentLoadInfoWatchServer struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *eofSegmentLoadInfoWatchServer) Send(*querypb.WatchQueryViewSegmentLoadInfoResponse) error {
	return nil
}

func (s *eofSegmentLoadInfoWatchServer) Recv() (*querypb.WatchQueryViewSegmentLoadInfoRequest, error) {
	return nil, io.EOF
}

func (s *eofSegmentLoadInfoWatchServer) Context() context.Context {
	return s.ctx
}

type channelSegmentLoadInfoWatchServer struct {
	grpc.ServerStream
	ctx    context.Context
	recv   chan *querypb.WatchQueryViewSegmentLoadInfoRequest
	send   chan *querypb.WatchQueryViewSegmentLoadInfoResponse
	closed chan struct{}
	once   sync.Once
}

func newChannelSegmentLoadInfoWatchServer(ctx context.Context) *channelSegmentLoadInfoWatchServer {
	return &channelSegmentLoadInfoWatchServer{
		ctx:    ctx,
		recv:   make(chan *querypb.WatchQueryViewSegmentLoadInfoRequest, 8),
		send:   make(chan *querypb.WatchQueryViewSegmentLoadInfoResponse, 8),
		closed: make(chan struct{}),
	}
}

func (s *channelSegmentLoadInfoWatchServer) Send(resp *querypb.WatchQueryViewSegmentLoadInfoResponse) error {
	select {
	case s.send <- resp:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *channelSegmentLoadInfoWatchServer) Recv() (*querypb.WatchQueryViewSegmentLoadInfoRequest, error) {
	select {
	case req := <-s.recv:
		return req, nil
	case <-s.closed:
		return nil, io.EOF
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

func (s *channelSegmentLoadInfoWatchServer) Context() context.Context {
	return s.ctx
}

func (s *channelSegmentLoadInfoWatchServer) closeRecv() {
	s.once.Do(func() {
		close(s.closed)
	})
}

func requireWatchResponse(t *testing.T, ch <-chan *querypb.WatchQueryViewSegmentLoadInfoResponse) *querypb.WatchQueryViewSegmentLoadInfoResponse {
	t.Helper()
	select {
	case resp := <-ch:
		return resp
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for watch response")
		return nil
	}
}

func assertNoWatchResponse(t *testing.T, ch <-chan *querypb.WatchQueryViewSegmentLoadInfoResponse) {
	t.Helper()
	select {
	case resp := <-ch:
		t.Fatalf("unexpected watch response: %v", resp)
	case <-time.After(100 * time.Millisecond):
	}
}

type fakeSegmentLoadInfoWatchMixCoord struct {
	componenttypes.MixCoord
	mu       sync.Mutex
	loadInfo *querypb.SegmentLoadInfo
	indexes  []*indexpb.IndexInfo
}

func (m *fakeSegmentLoadInfoWatchMixCoord) setLoadInfo(loadInfo *querypb.SegmentLoadInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.loadInfo = loadInfo
}

func (m *fakeSegmentLoadInfoWatchMixCoord) GetQueryViewSegmentLoadInfos(ctx context.Context, collectionID int64, segmentIDs []int64) ([]*querypb.SegmentLoadInfo, []*indexpb.IndexInfo, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return []*querypb.SegmentLoadInfo{m.loadInfo}, m.indexes, nil
}
