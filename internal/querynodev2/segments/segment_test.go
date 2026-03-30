package segments

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	storage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type SegmentSuite struct {
	suite.Suite
	rootPath     string
	chunkManager storage.ChunkManager

	// Data
	manager      *Manager
	collectionID int64
	partitionID  int64
	segmentID    int64
	collection   *Collection
	sealed       Segment
	growing      Segment
}

func (suite *SegmentSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *SegmentSuite) SetupTest() {
	var err error
	ctx := context.Background()
	msgLength := 100

	suite.rootPath = suite.T().Name()
	chunkManagerFactory := storage.NewTestChunkManagerFactory(paramtable.Get(), suite.rootPath)
	suite.chunkManager, _ = chunkManagerFactory.NewPersistentStorageChunkManager(ctx)
	initcore.InitRemoteChunkManager(paramtable.Get())
	localDataRootPath := filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), typeutil.QueryNodeRole)
	initcore.InitLocalChunkManager(localDataRootPath)
	initcore.InitMmapManager(paramtable.Get(), 1)
	initcore.InitTieredStorage(paramtable.Get())

	suite.collectionID = 100
	suite.partitionID = 10
	suite.segmentID = 1

	suite.manager = NewManager()
	schema := mock_segcore.GenTestCollectionSchema("test-reduce", schemapb.DataType_Int64, true)
	indexMeta := mock_segcore.GenTestIndexMeta(suite.collectionID, schema)
	suite.manager.Collection.PutOrRef(suite.collectionID,
		schema,
		indexMeta,
		&querypb.LoadMetaInfo{
			LoadType:     querypb.LoadType_LoadCollection,
			CollectionID: suite.collectionID,
			PartitionIDs: []int64{suite.partitionID},
		},
	)
	suite.collection = suite.manager.Collection.Get(suite.collectionID)

	suite.sealed, err = NewSegment(ctx,
		suite.collection,
		suite.manager.Segment,
		SegmentTypeSealed,
		0,
		&querypb.SegmentLoadInfo{
			CollectionID:  suite.collectionID,
			SegmentID:     suite.segmentID,
			PartitionID:   suite.partitionID,
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
			Level:         datapb.SegmentLevel_Legacy,
			NumOfRows:     int64(msgLength),
			BinlogPaths: []*datapb.FieldBinlog{
				{
					FieldID: 101,
					Binlogs: []*datapb.Binlog{
						{
							LogSize:    10086,
							MemorySize: 10086,
						},
					},
				},
			},
		},
	)
	suite.Require().NoError(err)

	binlogs, _, err := mock_segcore.SaveBinLog(ctx,
		suite.collectionID,
		suite.partitionID,
		suite.segmentID,
		msgLength,
		schema,
		suite.chunkManager,
	)
	suite.Require().NoError(err)
	g, err := suite.sealed.(*LocalSegment).StartLoadData()
	suite.Require().NoError(err)
	for _, binlog := range binlogs {
		err = suite.sealed.(*LocalSegment).LoadFieldData(ctx, binlog.FieldID, int64(msgLength), binlog)
		suite.Require().NoError(err)
	}
	g.Done(nil)

	suite.growing, err = NewSegment(ctx,
		suite.collection,
		suite.manager.Segment,
		SegmentTypeGrowing,
		0,
		&querypb.SegmentLoadInfo{
			SegmentID:     suite.segmentID + 1,
			CollectionID:  suite.collectionID,
			PartitionID:   suite.partitionID,
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
			Level:         datapb.SegmentLevel_Legacy,
		},
	)
	suite.Require().NoError(err)

	insertMsg, err := mock_segcore.GenInsertMsg(suite.collection.GetCCollection(), suite.partitionID, suite.growing.ID(), msgLength)
	suite.Require().NoError(err)
	insertRecord, err := storage.TransferInsertMsgToInsertRecord(suite.collection.Schema(), insertMsg)
	suite.Require().NoError(err)
	err = suite.growing.Insert(ctx, insertMsg.RowIDs, insertMsg.Timestamps, insertRecord)
	suite.Require().NoError(err)

	suite.manager.Segment.Put(context.Background(), SegmentTypeSealed, suite.sealed)
	suite.manager.Segment.Put(context.Background(), SegmentTypeGrowing, suite.growing)
}

func (suite *SegmentSuite) TearDownTest() {
	ctx := context.Background()
	suite.sealed.Release(context.Background())
	suite.growing.Release(context.Background())
	DeleteCollection(suite.collection)
	suite.chunkManager.RemoveWithPrefix(ctx, suite.rootPath)
}

func (suite *SegmentSuite) TestLoadInfo() {
	// sealed segment has load info
	suite.NotNil(suite.sealed.LoadInfo())
	// growing segment has no load info
	suite.NotNil(suite.growing.LoadInfo())
}

func (suite *SegmentSuite) TestResourceUsageEstimate() {
	// growing segment has resource usage
	// growing segment can not estimate resource usage
	usage := suite.growing.ResourceUsageEstimate()
	suite.Zero(usage.MemorySize)
	suite.Zero(usage.DiskSize)
	// sealed segment has resource usage
	usage = suite.sealed.ResourceUsageEstimate()
	// mmap is on
	suite.NotZero(usage.MemorySize)
	suite.Zero(usage.DiskSize)
	suite.Zero(usage.MmapFieldCount)
}

func (suite *SegmentSuite) TestDelete() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pks := storage.NewInt64PrimaryKeys(2)
	pks.AppendRaw(0, 1)

	// Test for sealed
	rowNum := suite.sealed.RowNum()
	err := suite.sealed.Delete(ctx, pks, []uint64{1000, 1000})
	suite.NoError(err)

	suite.Equal(rowNum-int64(pks.Len()), suite.sealed.RowNum())
	suite.Equal(rowNum, suite.sealed.InsertCount())

	// Test for growing
	rowNum = suite.growing.RowNum()
	err = suite.growing.Delete(ctx, pks, []uint64{1000, 1000})
	suite.NoError(err)

	suite.Equal(rowNum-int64(pks.Len()), suite.growing.RowNum())
	suite.Equal(rowNum, suite.growing.InsertCount())
}

func (suite *SegmentSuite) TestHasRawData() {
	has := suite.growing.HasRawData(mock_segcore.SimpleFloatVecField.ID)
	suite.True(has)
	has = suite.sealed.HasRawData(mock_segcore.SimpleFloatVecField.ID)
	suite.True(has)
}

func (suite *SegmentSuite) TestCASVersion() {
	segment := suite.sealed

	curVersion := segment.Version()
	suite.False(segment.CASVersion(curVersion-1, curVersion+1))
	suite.NotEqual(curVersion+1, segment.Version())

	suite.True(segment.CASVersion(curVersion, curVersion+1))
	suite.Equal(curVersion+1, segment.Version())
}

func (suite *SegmentSuite) TestSegmentRemoveUnusedFieldFiles() {
}

// TestDeleteSameTimestampAcrossBatches reproduces the DumpSnapshot Assert failure
// caused by proxy splitting a large DELETE operation into multiple messages that
// cross timetick boundaries. All split messages share the same TSO timestamp,
// so consecutive StreamPush calls can insert entries with the same timestamp but
// smaller row_ids — landing BEFORE the DumpSnapshot cursor in the sorted skip list.
//
// Scenario:
//  1. Batch 1: Delete PKs [50,60,70,80,90] at ts=1000 → DumpSnapshot creates cursor
//  2. Batch 2: Delete PKs [10,20,30] at ts=[1000,1000,1001] → entries (1000,10),(1000,20)
//     are sorted BEFORE the cursor, but accessor.size() counts them.
//     Iterator from cursor can't reach them → old code Assert, new code rebuilds.
func (suite *SegmentSuite) TestDeleteSameTimestampAcrossBatches() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set DELETE_DUMP_BATCH_SIZE to a small value so DumpSnapshot triggers easily
	initcore.UpdateDefaultDeleteDumpBatchSize(3)
	defer initcore.UpdateDefaultDeleteDumpBatchSize(10000)

	// Batch 1: delete PKs with higher row_ids at timestamp 1000
	// This triggers DumpSnapshot which sets cursor after processing 3 entries.
	// Skip list: (1000,50),(1000,60),(1000,70),(1000,80),(1000,90)
	// DumpSnapshot processes first 3 → cursor at (1000,80)
	pks1 := storage.NewInt64PrimaryKeys(5)
	pks1.AppendRaw(50, 60, 70, 80, 90)
	err := suite.sealed.Delete(ctx, pks1, []uint64{1000, 1000, 1000, 1000, 1000})
	suite.NoError(err)

	// Batch 2: delete PKs with LOWER row_ids at the SAME timestamp 1000,
	// plus one entry at ts=1001 to bypass lastDeltaTimestamp check (1000 >= 1001 is false).
	//
	// InternalPush inserts: (1000,10),(1000,20) → BEFORE cursor (1000,80)
	//                       (1001,30)           → AFTER cursor
	//
	// DumpSnapshot sees: total=8, dumped=3, remaining=5 > BATCH_SIZE(3)
	// But only 3 entries exist from cursor to end: (1000,80),(1000,90),(1001,30)
	// For loop consumes all 3 → iterator reaches end() → triggers rebuild (or Assert in old code)
	pks2 := storage.NewInt64PrimaryKeys(3)
	pks2.AppendRaw(10, 20, 30)
	err = suite.sealed.Delete(ctx, pks2, []uint64{1000, 1000, 1001})
	suite.NoError(err)

	// If we reach here without crash/panic, the rebuild fix works correctly.
	// Verify all 8 deletes were applied.
	suite.Equal(int64(100-8), suite.sealed.RowNum())
	suite.Equal(int64(100), suite.sealed.InsertCount())
}

func (suite *SegmentSuite) TestSegmentReleased() {
	suite.sealed.Release(context.Background())

	sealed := suite.sealed.(*LocalSegment)

	suite.False(sealed.ptrLock.PinIfNotReleased())
	suite.EqualValues(0, sealed.RowNum())
	suite.EqualValues(0, sealed.MemSize())
	suite.False(sealed.HasRawData(101))
}

func TestSegment(t *testing.T) {
	suite.Run(t, new(SegmentSuite))
}

// newTestBaseSegment creates a baseSegment for testing without requiring segcore Collection.
func newTestBaseSegment(segmentID, partitionID int64) baseSegment {
	return baseSegment{
		loadInfo: atomic.NewPointer(&querypb.SegmentLoadInfo{
			SegmentID:   segmentID,
			PartitionID: partitionID,
		}),
		version:            atomic.NewInt64(0),
		bm25Stats:          make(map[int64]*storage.BM25Stats),
		resourceUsageCache: atomic.NewPointer[ResourceUsage](nil),
		needUpdatedVersion: atomic.NewInt64(0),
	}
}

// TestBaseSegment_PkCandidateExternalCandidate tests pkCandidate wrapper methods
// with an ExternalSegmentCandidate (used for external/virtual-PK collections).
func TestBaseSegment_PkCandidateExternalCandidate(t *testing.T) {
	paramtable.Init()

	segmentID := int64(12345)
	partitionID := int64(10)
	candidate := pkoracle.NewExternalSegmentCandidate(segmentID, partitionID, SegmentTypeSealed)

	bs := newTestBaseSegment(segmentID, partitionID)
	bs.SetPKCandidate(candidate)

	// PkCandidateExist: ExternalSegmentCandidate always returns true
	assert.True(t, bs.PkCandidateExist())

	// Stats: ExternalSegmentCandidate returns nil
	assert.Nil(t, bs.Stats())

	// GetMinPk / GetMaxPk: nil stats → nil
	assert.Nil(t, bs.GetMinPk())
	assert.Nil(t, bs.GetMaxPk())

	// Charge / Refund: no-op, should not panic
	bs.Charge()
	bs.Refund()

	// UpdatePkCandidate: no-op for external candidate
	bs.UpdatePkCandidate([]storage.PrimaryKey{storage.NewInt64PrimaryKey(1)})

	// MayPkExist with a virtual PK belonging to this segment
	virtualPK := GetVirtualPK(segmentID, 42)
	lc := storage.NewLocationsCache(storage.NewInt64PrimaryKey(virtualPK))
	assert.True(t, bs.MayPkExist(lc))

	// MayPkExist with a virtual PK from a different segment
	otherPK := GetVirtualPK(segmentID+1, 42)
	lc2 := storage.NewLocationsCache(storage.NewInt64PrimaryKey(otherPK))
	assert.False(t, bs.MayPkExist(lc2))

	// BatchPkExist
	pks := []storage.PrimaryKey{
		storage.NewInt64PrimaryKey(GetVirtualPK(segmentID, 0)),
		storage.NewInt64PrimaryKey(GetVirtualPK(segmentID+1, 0)),
		storage.NewInt64PrimaryKey(GetVirtualPK(segmentID, 99)),
	}
	blc := storage.NewBatchLocationsCache(pks)
	results := bs.BatchPkExist(blc)
	assert.Equal(t, []bool{true, false, true}, results)
}

// TestBaseSegment_GetMinMaxPkWithStats tests GetMinPk/GetMaxPk when Stats() returns non-nil.
func TestBaseSegment_GetMinMaxPkWithStats(t *testing.T) {
	paramtable.Init()

	segmentID := int64(100)
	partitionID := int64(10)
	bfs := pkoracle.NewBloomFilterSet(segmentID, partitionID, SegmentTypeSealed)
	// Feed PKs so Stats() returns non-nil with min/max
	bfs.UpdatePkCandidate([]storage.PrimaryKey{
		storage.NewInt64PrimaryKey(10),
		storage.NewInt64PrimaryKey(50),
		storage.NewInt64PrimaryKey(100),
	})

	bs := newTestBaseSegment(segmentID, partitionID)
	bs.SetPKCandidate(bfs)

	minPk := bs.GetMinPk()
	assert.NotNil(t, minPk)
	maxPk := bs.GetMaxPk()
	assert.NotNil(t, maxPk)
}

// TestBaseSegment_PkCandidateNil tests pkCandidate wrapper methods when candidate is nil.
func TestBaseSegment_PkCandidateNil(t *testing.T) {
	paramtable.Init()

	bs := newTestBaseSegment(1, 0)
	// pkCandidate is nil by default from newTestBaseSegment

	// PkCandidateExist: nil → false
	assert.False(t, bs.PkCandidateExist())

	// Stats: nil candidate → nil
	assert.Nil(t, bs.Stats())

	// GetMinPk / GetMaxPk: nil candidate → nil
	assert.Nil(t, bs.GetMinPk())
	assert.Nil(t, bs.GetMaxPk())

	// Charge / Refund: nil candidate → no-op, should not panic
	bs.Charge()
	bs.Refund()

	// UpdatePkCandidate: nil candidate → no-op
	bs.UpdatePkCandidate([]storage.PrimaryKey{storage.NewInt64PrimaryKey(1)})

	// MayPkExist: nil candidate → returns true (assume PK might exist)
	lc := storage.NewLocationsCache(storage.NewInt64PrimaryKey(42))
	assert.True(t, bs.MayPkExist(lc))

	// BatchPkExist: nil candidate → all true (consistent with MayPkExist)
	pks := []storage.PrimaryKey{
		storage.NewInt64PrimaryKey(1),
		storage.NewInt64PrimaryKey(2),
	}
	blc := storage.NewBatchLocationsCache(pks)
	results := bs.BatchPkExist(blc)
	assert.Equal(t, []bool{true, true}, results)
}

// TestBaseSegment_SkipGrowingBF tests that skipGrowingBF bypasses PK candidate checks.
func TestBaseSegment_SkipGrowingBF(t *testing.T) {
	paramtable.Init()

	segmentID := int64(100)
	candidate := pkoracle.NewExternalSegmentCandidate(segmentID, 10, SegmentTypeGrowing)

	bs := newTestBaseSegment(segmentID, 10)
	bs.skipGrowingBF = true
	bs.SetPKCandidate(candidate)

	// MayPkExist: skipGrowingBF → always true regardless of candidate
	otherPK := GetVirtualPK(segmentID+999, 42)
	lc := storage.NewLocationsCache(storage.NewInt64PrimaryKey(otherPK))
	assert.True(t, bs.MayPkExist(lc))

	// UpdatePkCandidate: skipGrowingBF → skips update
	bs.UpdatePkCandidate([]storage.PrimaryKey{storage.NewInt64PrimaryKey(1)})

	// BatchPkExist: skipGrowingBF → all true
	pks := []storage.PrimaryKey{
		storage.NewInt64PrimaryKey(1),
		storage.NewInt64PrimaryKey(2),
		storage.NewInt64PrimaryKey(3),
	}
	blc := storage.NewBatchLocationsCache(pks)
	results := bs.BatchPkExist(blc)
	assert.Equal(t, []bool{true, true, true}, results)
}
