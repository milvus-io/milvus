package catalog_service

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	catalog "github.com/milvus-io/milvus-catalog"
	"github.com/milvus-io/milvus-catalog/catalogpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	metamocks "github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// ---------------------------------------------------------------------------
// trackingSvc — a hand-written stub for catalog.MilvusCatalogService that
// records which methods were called. Tests check that dispatch went to svc
// (ie. engine-facing methods) by inspecting this hit map.
// ---------------------------------------------------------------------------

type trackingSvc struct {
	mu     sync.Mutex
	called map[string]int
}

func newTrackingSvc() *trackingSvc {
	return &trackingSvc{called: map[string]int{}}
}

func (s *trackingSvc) hit(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.called[name]++
}

func (s *trackingSvc) count(name string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.called[name]
}

func (s *trackingSvc) any() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, v := range s.called {
		total += v
	}
	return total
}

// Compile-time check.
var _ catalog.MilvusCatalogService = (*trackingSvc)(nil)

// --- Database ---
func (s *trackingSvc) CreateDatabase(ctx context.Context, db *model.Database, ts uint64) error {
	s.hit("CreateDatabase")
	return nil
}
func (s *trackingSvc) ListDatabases(ctx context.Context, ts uint64) ([]*model.Database, error) {
	s.hit("ListDatabases")
	return nil, nil
}
func (s *trackingSvc) AlterDatabase(ctx context.Context, newDB *model.Database, ts uint64) error {
	s.hit("AlterDatabase")
	return nil
}
func (s *trackingSvc) DropDatabase(ctx context.Context, dbID int64, ts uint64) error {
	s.hit("DropDatabase")
	return nil
}

// --- Collection ---
func (s *trackingSvc) CreateCollection(ctx context.Context, coll *model.Collection, ts uint64) error {
	s.hit("CreateCollection")
	return nil
}
func (s *trackingSvc) GetCollectionByID(ctx context.Context, dbID int64, ts uint64, collectionID int64) (*model.Collection, error) {
	s.hit("GetCollectionByID")
	return nil, nil
}
func (s *trackingSvc) GetCollectionByName(ctx context.Context, dbID int64, dbName string, collectionName string, ts uint64) (*model.Collection, error) {
	s.hit("GetCollectionByName")
	return nil, nil
}
func (s *trackingSvc) ListCollections(ctx context.Context, dbID int64, ts uint64) ([]*model.Collection, error) {
	s.hit("ListCollections")
	return nil, nil
}
func (s *trackingSvc) CollectionExists(ctx context.Context, dbID int64, collectionID int64, ts uint64) bool {
	s.hit("CollectionExists")
	return false
}
func (s *trackingSvc) DropCollection(ctx context.Context, coll *model.Collection, ts uint64) error {
	s.hit("DropCollection")
	return nil
}
func (s *trackingSvc) AlterCollection(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, alterType catalog.AlterType, ts uint64, fieldModify bool) error {
	s.hit("AlterCollection")
	return nil
}
func (s *trackingSvc) AlterCollectionDB(ctx context.Context, oldColl *model.Collection, newColl *model.Collection, ts uint64) error {
	s.hit("AlterCollectionDB")
	return nil
}
func (s *trackingSvc) CreatePartition(ctx context.Context, dbID int64, partition *model.Partition, ts uint64) error {
	s.hit("CreatePartition")
	return nil
}
func (s *trackingSvc) DropPartition(ctx context.Context, dbID int64, collectionID int64, partitionID int64, ts uint64) error {
	s.hit("DropPartition")
	return nil
}
func (s *trackingSvc) AlterPartition(ctx context.Context, dbID int64, oldPart *model.Partition, newPart *model.Partition, alterType catalog.AlterType, ts uint64) error {
	s.hit("AlterPartition")
	return nil
}
func (s *trackingSvc) CreateAlias(ctx context.Context, alias *model.Alias, ts uint64) error {
	s.hit("CreateAlias")
	return nil
}
func (s *trackingSvc) DropAlias(ctx context.Context, dbID int64, alias string, ts uint64) error {
	s.hit("DropAlias")
	return nil
}
func (s *trackingSvc) AlterAlias(ctx context.Context, alias *model.Alias, ts uint64) error {
	s.hit("AlterAlias")
	return nil
}
func (s *trackingSvc) ListAliases(ctx context.Context, dbID int64, ts uint64) ([]*model.Alias, error) {
	s.hit("ListAliases")
	return nil, nil
}

// --- Segment ---
func (s *trackingSvc) ListSegments(ctx context.Context, collectionID int64) ([]*datapb.SegmentInfo, error) {
	s.hit("ListSegments")
	return nil, nil
}
func (s *trackingSvc) AddSegment(ctx context.Context, segment *datapb.SegmentInfo) error {
	s.hit("AddSegment")
	return nil
}
func (s *trackingSvc) AlterSegments(ctx context.Context, newSegments []*datapb.SegmentInfo, binlogs ...catalog.BinlogsIncrement) error {
	s.hit("AlterSegments")
	return nil
}
func (s *trackingSvc) SaveDroppedSegmentsInBatch(ctx context.Context, segments []*datapb.SegmentInfo) error {
	s.hit("SaveDroppedSegmentsInBatch")
	return nil
}
func (s *trackingSvc) DropSegment(ctx context.Context, segment *datapb.SegmentInfo) error {
	s.hit("DropSegment")
	return nil
}

// --- Index ---
func (s *trackingSvc) CreateIndex(ctx context.Context, index *model.Index) error {
	s.hit("CreateIndex")
	return nil
}
func (s *trackingSvc) ListIndexes(ctx context.Context) ([]*model.Index, error) {
	s.hit("ListIndexes")
	return nil, nil
}
func (s *trackingSvc) AlterIndexes(ctx context.Context, newIndexes []*model.Index) error {
	s.hit("AlterIndexes")
	return nil
}
func (s *trackingSvc) DropIndex(ctx context.Context, collID, dropIdxID int64) error {
	s.hit("DropIndex")
	return nil
}

// --- FileResource ---
func (s *trackingSvc) SaveFileResource(ctx context.Context, resource *internalpb.FileResourceInfo, version uint64) error {
	s.hit("SaveFileResource")
	return nil
}
func (s *trackingSvc) RemoveFileResource(ctx context.Context, resourceID int64, version uint64) error {
	s.hit("RemoveFileResource")
	return nil
}
func (s *trackingSvc) ListFileResource(ctx context.Context) ([]*internalpb.FileResourceInfo, uint64, error) {
	s.hit("ListFileResource")
	return nil, 0, nil
}

// --- Atomic composite API ---
func (s *trackingSvc) CommitCompaction(ctx context.Context, req *catalogpb.CommitCompactionRequest) (*catalogpb.CommitCompactionResponse, error) {
	s.hit("CommitCompaction")
	return &catalogpb.CommitCompactionResponse{Committed: true}, nil
}
func (s *trackingSvc) CreateCollectionWithIndex(ctx context.Context, req *catalogpb.CreateCollectionWithIndexRequest) (*catalogpb.CreateCollectionWithIndexResponse, error) {
	s.hit("CreateCollectionWithIndex")
	return &catalogpb.CreateCollectionWithIndexResponse{}, nil
}
func (s *trackingSvc) CommitFlushSegment(ctx context.Context, req *catalogpb.CommitFlushSegmentRequest) (*catalogpb.CommitFlushSegmentResponse, error) {
	s.hit("CommitFlushSegment")
	return &catalogpb.CommitFlushSegmentResponse{}, nil
}
func (s *trackingSvc) GetCommitMarker(ctx context.Context, req *catalogpb.GetCommitMarkerRequest) (*catalogpb.GetCommitMarkerResponse, error) {
	s.hit("GetCommitMarker")
	return &catalogpb.GetCommitMarkerResponse{Exists: false}, nil
}

// Close no-op for test.
func (s *trackingSvc) Close() { s.hit("Close") }

// ---------------------------------------------------------------------------
// RootCoordAdapter tests
// ---------------------------------------------------------------------------

// newRCTest returns a RootCoordAdapter wrapping a trackingSvc and a mockery
// mock for the local catalog. Unused local methods will panic on unexpected
// calls — which is exactly what we want to catch routing mistakes.
func newRCTest(t *testing.T) (*RootCoordAdapter, *trackingSvc, *metamocks.RootCoordCatalog) {
	t.Helper()
	svc := newTrackingSvc()
	local := metamocks.NewRootCoordCatalog(t)
	a := NewRootCoordAdapter(svc, local)
	return a, svc, local
}

func TestRootCoordAdapter_CreateDatabase_RoutesToSvc(t *testing.T) {
	a, svc, _ := newRCTest(t)
	// local mock has no expectations — any call to local will fail the test.
	err := a.CreateDatabase(context.Background(), &model.Database{ID: 1}, 100)
	require.NoError(t, err)
	assert.Equal(t, 1, svc.count("CreateDatabase"))
}

func TestRootCoordAdapter_DropDatabase_RoutesToSvc(t *testing.T) {
	a, svc, _ := newRCTest(t)
	err := a.DropDatabase(context.Background(), 1, 100)
	require.NoError(t, err)
	assert.Equal(t, 1, svc.count("DropDatabase"))
}

func TestRootCoordAdapter_ListDatabases_RoutesToSvc(t *testing.T) {
	a, svc, _ := newRCTest(t)
	_, err := a.ListDatabases(context.Background(), 100)
	require.NoError(t, err)
	assert.Equal(t, 1, svc.count("ListDatabases"))
}

func TestRootCoordAdapter_AlterDatabase_RoutesToSvc(t *testing.T) {
	a, svc, _ := newRCTest(t)
	err := a.AlterDatabase(context.Background(), &model.Database{ID: 1}, 100)
	require.NoError(t, err)
	assert.Equal(t, 1, svc.count("AlterDatabase"))
}

func TestRootCoordAdapter_CreateCollection_RoutesToSvc(t *testing.T) {
	a, svc, _ := newRCTest(t)
	err := a.CreateCollection(context.Background(), &model.Collection{CollectionID: 1}, 100)
	require.NoError(t, err)
	assert.Equal(t, 1, svc.count("CreateCollection"))
}

func TestRootCoordAdapter_DropCollection_RoutesToSvc(t *testing.T) {
	a, svc, _ := newRCTest(t)
	err := a.DropCollection(context.Background(), &model.Collection{CollectionID: 1}, 100)
	require.NoError(t, err)
	assert.Equal(t, 1, svc.count("DropCollection"))
}

func TestRootCoordAdapter_AlterCollectionDB_RoutesToLocal(t *testing.T) {
	a, svc, local := newRCTest(t)
	local.EXPECT().AlterCollectionDB(mock.Anything, mock.Anything, mock.Anything, uint64(100)).Return(nil).Once()
	err := a.AlterCollectionDB(context.Background(), &model.Collection{CollectionID: 1}, &model.Collection{CollectionID: 1}, 100)
	require.NoError(t, err)
	assert.Equal(t, 0, svc.any())
}

func TestRootCoordAdapter_CreateRole_RoutesToLocal(t *testing.T) {
	a, svc, local := newRCTest(t)
	local.EXPECT().CreateRole(mock.Anything, "t", mock.Anything).Return(nil).Once()
	err := a.CreateRole(context.Background(), "t", &milvuspb.RoleEntity{Name: "r"})
	require.NoError(t, err)
	assert.Equal(t, 0, svc.any())
}

func TestRootCoordAdapter_AlterGrant_RoutesToLocal(t *testing.T) {
	a, svc, local := newRCTest(t)
	local.EXPECT().AlterGrant(mock.Anything, "t", mock.Anything, mock.Anything).Return(nil).Once()
	err := a.AlterGrant(context.Background(), "t", &milvuspb.GrantEntity{}, milvuspb.OperatePrivilegeType_Grant)
	require.NoError(t, err)
	assert.Equal(t, 0, svc.any())
}

func TestRootCoordAdapter_BackupRBAC_RoutesToLocal(t *testing.T) {
	a, svc, local := newRCTest(t)
	local.EXPECT().BackupRBAC(mock.Anything, "t").Return(&milvuspb.RBACMeta{}, nil).Once()
	_, err := a.BackupRBAC(context.Background(), "t")
	require.NoError(t, err)
	assert.Equal(t, 0, svc.any())
}

// ---------------------------------------------------------------------------
// DataCoordAdapter tests
// ---------------------------------------------------------------------------

func newDCTest(t *testing.T) (*DataCoordAdapter, *trackingSvc, *metamocks.DataCoordCatalog) {
	t.Helper()
	svc := newTrackingSvc()
	local := metamocks.NewDataCoordCatalog(t)
	a := NewDataCoordAdapter(svc, local)
	return a, svc, local
}

func TestDataCoordAdapter_ListSegments_RoutesToSvc(t *testing.T) {
	a, svc, _ := newDCTest(t)
	_, err := a.ListSegments(context.Background(), 1)
	require.NoError(t, err)
	assert.Equal(t, 1, svc.count("ListSegments"))
}

func TestDataCoordAdapter_SaveChannelCheckpoint_RoutesToLocal(t *testing.T) {
	a, svc, local := newDCTest(t)
	local.EXPECT().SaveChannelCheckpoint(mock.Anything, "ch1", mock.Anything).Return(nil).Once()
	err := a.SaveChannelCheckpoint(context.Background(), "ch1", &msgpb.MsgPosition{})
	require.NoError(t, err)
	assert.Equal(t, 0, svc.any())
}

func TestDataCoordAdapter_SaveCompactionTask_RoutesToLocal(t *testing.T) {
	a, svc, local := newDCTest(t)
	local.EXPECT().SaveCompactionTask(mock.Anything, mock.Anything).Return(nil).Once()
	err := a.SaveCompactionTask(context.Background(), &datapb.CompactionTask{})
	require.NoError(t, err)
	assert.Equal(t, 0, svc.any())
}

func TestDataCoordAdapter_SaveImportJob_RoutesToLocal(t *testing.T) {
	a, svc, local := newDCTest(t)
	local.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil).Once()
	err := a.SaveImportJob(context.Background(), &datapb.ImportJob{})
	require.NoError(t, err)
	assert.Equal(t, 0, svc.any())
}

func TestDataCoordAdapter_CreateSegmentIndex_RoutesToLocal(t *testing.T) {
	a, svc, local := newDCTest(t)
	local.EXPECT().CreateSegmentIndex(mock.Anything, mock.Anything).Return(nil).Once()
	err := a.CreateSegmentIndex(context.Background(), &model.SegmentIndex{})
	require.NoError(t, err)
	assert.Equal(t, 0, svc.any())
}

func TestDataCoordAdapter_ListSegmentIndexes_RoutesToLocal(t *testing.T) {
	a, svc, local := newDCTest(t)
	local.EXPECT().ListSegmentIndexes(mock.Anything, int64(1)).Return(nil, nil).Once()
	_, err := a.ListSegmentIndexes(context.Background(), 1)
	require.NoError(t, err)
	assert.Equal(t, 0, svc.any())
}

func TestDataCoordAdapter_CreateIndex_RoutesToSvc(t *testing.T) {
	a, svc, _ := newDCTest(t)
	err := a.CreateIndex(context.Background(), &model.Index{CollectionID: 1, IndexID: 2})
	require.NoError(t, err)
	assert.Equal(t, 1, svc.count("CreateIndex"))
}

func TestDataCoordAdapter_GcConfirm_ComposedViaListSegments(t *testing.T) {
	a, svc, _ := newDCTest(t)
	// Regardless of partition, GcConfirm must compose via svc.ListSegments
	// and never touch local.
	ok := a.GcConfirm(context.Background(), 1, common.AllPartitionsID)
	assert.True(t, ok, "empty collection should be considered GC'd")
	assert.Equal(t, 1, svc.count("ListSegments"))
}

func TestDataCoordAdapter_SaveSnapshot_RoutesToLocal(t *testing.T) {
	a, svc, local := newDCTest(t)
	local.EXPECT().SaveSnapshot(mock.Anything, mock.Anything).Return(nil).Once()
	err := a.SaveSnapshot(context.Background(), &datapb.SnapshotInfo{})
	require.NoError(t, err)
	assert.Equal(t, 0, svc.any())
}

// ---------------------------------------------------------------------------
// Atomic composite API via extension interface
// ---------------------------------------------------------------------------

func TestDataCoordAdapter_ImplementsAtomicCapable(t *testing.T) {
	a, _, _ := newDCTest(t)
	_, ok := interface{}(a).(AtomicCapableCatalog)
	assert.True(t, ok, "DataCoordAdapter must implement AtomicCapableCatalog")
}

func TestDataCoordAdapter_CommitCompaction_RoutesToSvc(t *testing.T) {
	a, svc, _ := newDCTest(t)
	_, err := a.CommitCompaction(context.Background(), &catalogpb.CommitCompactionRequest{TaskId: 1})
	require.NoError(t, err)
	assert.Equal(t, 1, svc.count("CommitCompaction"))
}

func TestDataCoordAdapter_CommitFlushSegment_RoutesToSvc(t *testing.T) {
	a, svc, _ := newDCTest(t)
	_, err := a.CommitFlushSegment(context.Background(), &catalogpb.CommitFlushSegmentRequest{})
	require.NoError(t, err)
	assert.Equal(t, 1, svc.count("CommitFlushSegment"))
}
