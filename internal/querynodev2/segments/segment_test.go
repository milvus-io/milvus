package segments

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
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

// TestLoadDeltaData_LowerTsNotSkipped guards against the bug where L0-forwarded
// deletes with ts lower than the already-applied manifest-delta watermark
// were silently dropped, causing snapshot restore to retain deleted rows.
//
// Reproduction: apply a delete at a high ts (simulating segment's own _delta/
// loaded from manifest), then apply a delete for a DIFFERENT PK at a lower ts
// (simulating L0 segment delete forwarded by delegator). Both must take effect.
// Before the fix, the second call was skipped entirely via:
//
//	if s.lastDeltaTimestamp.Load() >= tss[len(tss)-1] { return nil }
func (suite *SegmentSuite) TestLoadDeltaData_LowerTsNotSkipped() {
	ctx := context.Background()

	// Phase 1: apply delete for PK=80 at ts=2000 (simulates manifest _delta/).
	pksHigh := storage.NewInt64PrimaryKeys(1)
	pksHigh.AppendRaw(80)
	ddHigh, err := storage.NewDeltaDataWithData(pksHigh, []uint64{2000})
	suite.Require().NoError(err)
	suite.Require().NoError(suite.sealed.(*LocalSegment).LoadDeltaData(ctx, ddHigh))
	suite.EqualValues(2000, suite.sealed.(*LocalSegment).LastDeltaTimestamp())

	// Phase 2: L0-forwarded delete for PK=10 at ts=1000 (LOWER than watermark).
	// Must be applied — before the fix this was silently dropped.
	pksLow := storage.NewInt64PrimaryKeys(1)
	pksLow.AppendRaw(10)
	ddLow, err := storage.NewDeltaDataWithData(pksLow, []uint64{1000})
	suite.Require().NoError(err)
	suite.Require().NoError(suite.sealed.(*LocalSegment).LoadDeltaData(ctx, ddLow))

	// Both PKs deleted => RowNum = 100 - 2 = 98.
	suite.EqualValues(98, suite.sealed.RowNum())
	// Watermark stays at max, not regresses to 1000.
	suite.EqualValues(2000, suite.sealed.(*LocalSegment).LastDeltaTimestamp())
}

// TestLoadDeltaData_UnsortedBatchAllApplied guards against the BufferForwarder
// interaction: rangeHitL0Deletions iterates L0 segments in unsorted order, so
// tss[last] is whichever L0 segment was visited last, NOT the batch max.
// A batch like tss=[1500, 500] would previously be compared as tss[1]=500
// against watermark and (if watermark >= 500) get entirely dropped, including
// the PK at ts=1500 that was ABOVE the watermark.
func (suite *SegmentSuite) TestLoadDeltaData_UnsortedBatchAllApplied() {
	ctx := context.Background()

	// Establish watermark at 1000.
	pksInit := storage.NewInt64PrimaryKeys(1)
	pksInit.AppendRaw(99)
	ddInit, err := storage.NewDeltaDataWithData(pksInit, []uint64{1000})
	suite.Require().NoError(err)
	suite.Require().NoError(suite.sealed.(*LocalSegment).LoadDeltaData(ctx, ddInit))

	// Unsorted batch: first ts=1500 (above watermark), last ts=500 (below).
	// Must apply both. Before fix: tss[last]=500 <= 1000 => entire batch dropped.
	pksMixed := storage.NewInt64PrimaryKeys(2)
	pksMixed.AppendRaw(20, 30)
	ddMixed, err := storage.NewDeltaDataWithData(pksMixed, []uint64{1500, 500})
	suite.Require().NoError(err)
	suite.Require().NoError(suite.sealed.(*LocalSegment).LoadDeltaData(ctx, ddMixed))

	suite.EqualValues(97, suite.sealed.RowNum()) // 100 - 3 deletes
	// Watermark advances to batch max, not last.
	suite.EqualValues(1500, suite.sealed.(*LocalSegment).LastDeltaTimestamp())
}

// TestAdvanceLastDeltaTimestamp_NeverRegresses verifies the watermark is
// monotonic: a lower-max batch after a higher-max batch must not regress it.
func (suite *SegmentSuite) TestAdvanceLastDeltaTimestamp_NeverRegresses() {
	ctx := context.Background()

	pksA := storage.NewInt64PrimaryKeys(1)
	pksA.AppendRaw(11)
	ddA, _ := storage.NewDeltaDataWithData(pksA, []uint64{5000})
	suite.Require().NoError(suite.sealed.(*LocalSegment).LoadDeltaData(ctx, ddA))
	suite.EqualValues(5000, suite.sealed.(*LocalSegment).LastDeltaTimestamp())

	pksB := storage.NewInt64PrimaryKeys(1)
	pksB.AppendRaw(12)
	ddB, _ := storage.NewDeltaDataWithData(pksB, []uint64{2000})
	suite.Require().NoError(suite.sealed.(*LocalSegment).LoadDeltaData(ctx, ddB))
	// Watermark stays at 5000, not regresses to 2000.
	suite.EqualValues(5000, suite.sealed.(*LocalSegment).LastDeltaTimestamp())
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
