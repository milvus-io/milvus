package segments

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

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
	initcore.InitMmapManager(paramtable.Get())

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
		nil,
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
		SegmentTypeGrowing,
		0,
		&querypb.SegmentLoadInfo{
			SegmentID:     suite.segmentID + 1,
			CollectionID:  suite.collectionID,
			PartitionID:   suite.partitionID,
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
			Level:         datapb.SegmentLevel_Legacy,
		},
		nil,
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
	// growing segment has no resource usage
	usage = suite.sealed.ResourceUsageEstimate()
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

// TestDeleteSameTimestampAcrossBatches reproduces the DumpSnapshot rebuild path
// caused by proxy splitting a large DELETE operation into multiple messages that
// share the same TSO timestamp. Consecutive StreamPush calls can insert entries
// with the same timestamp but smaller row_ids — landing BEFORE the DumpSnapshot
// cursor in the sorted skip list.
//
// DUMP_BATCH_SIZE in C++ (segcore/DeletedRecord.h) is 10000, so we create a
// segment with enough rows to trigger the snapshot + rebuild naturally.
//
// Scenario (DUMP_BATCH_SIZE = 10000):
//  1. Batch 1: Delete 10100 PKs with high row_ids at ts=T
//     → DumpSnapshot dumps first 10000, cursor at (T, row_20100)
//  2. Batch 2: Delete 10100 PKs with LOW row_ids at the SAME ts=T
//     → all 10100 entries sort BEFORE cursor; total_undumped = 10200 > 10000
//     → DumpSnapshot iterates from cursor, but only 100 entries remain
//     → iterator exhausts → old code Assert, new code rebuilds from scratch.
func (suite *SegmentSuite) TestDeleteSameTimestampAcrossBatches() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rowCount := 20200

	growing, err := NewSegment(ctx,
		suite.collection,
		SegmentTypeGrowing,
		0,
		&querypb.SegmentLoadInfo{
			SegmentID:     suite.segmentID + 10,
			CollectionID:  suite.collectionID,
			PartitionID:   suite.partitionID,
			InsertChannel: fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", suite.collectionID),
			Level:         datapb.SegmentLevel_Legacy,
		},
		nil,
	)
	suite.Require().NoError(err)
	defer growing.Release(context.Background())

	insertMsg, err := mock_segcore.GenInsertMsg(suite.collection.GetCCollection(), suite.partitionID, growing.ID(), rowCount)
	suite.Require().NoError(err)
	insertRecord, err := storage.TransferInsertMsgToInsertRecord(suite.collection.Schema(), insertMsg)
	suite.Require().NoError(err)
	err = growing.Insert(ctx, insertMsg.RowIDs, insertMsg.Timestamps, insertRecord)
	suite.Require().NoError(err)
	suite.Equal(int64(rowCount), growing.RowNum())

	// Use a timestamp well above any insert timestamp to avoid the
	// "delete_ts == insert_ts" skip in InternalPush.
	deleteTS := uint64(rowCount + 1000)

	// Batch 1: delete PKs [10100..20199] (10100 entries) at deleteTS.
	// Skip list entries: (deleteTS,10100), ..., (deleteTS,20199).
	// DumpSnapshot: 10100 > 10000 → dumps first 10000, cursor at (deleteTS,20100).
	batch1Size := 10100
	pks1 := storage.NewInt64PrimaryKeys(int64(batch1Size))
	ts1 := make([]uint64, batch1Size)
	for i := 0; i < batch1Size; i++ {
		pks1.AppendRaw(int64(10100 + i))
		ts1[i] = deleteTS
	}
	err = growing.Delete(ctx, pks1, ts1)
	suite.NoError(err)

	// Batch 2: delete PKs [0..10099] (10100 entries) at the SAME deleteTS.
	// All entries (deleteTS,0)..(deleteTS,10099) sort BEFORE cursor (deleteTS,20100).
	// Total=20200, dumped=10000 → undumped=10200 > 10000, DumpSnapshot triggers.
	// From cursor only 100 entries remain → iterator exhausts → rebuild
	// (or Assert in old code).
	batch2Size := 10100
	pks2 := storage.NewInt64PrimaryKeys(int64(batch2Size))
	ts2 := make([]uint64, batch2Size)
	for i := 0; i < batch2Size; i++ {
		pks2.AppendRaw(int64(i))
		ts2[i] = deleteTS
	}
	err = growing.Delete(ctx, pks2, ts2)
	suite.NoError(err)

	// If we reach here without crash/panic, the rebuild fix works correctly.
	totalDeleted := batch1Size + batch2Size
	suite.Equal(int64(rowCount-totalDeleted), growing.RowNum())
	suite.Equal(int64(rowCount), growing.InsertCount())
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

func TestWarmupDispatcher(t *testing.T) {
	d := NewWarmupDispatcher()
	ctx := context.Background()
	go d.Run(ctx)

	completed := atomic.NewInt64(0)
	taskCnt := 10000
	for i := 0; i < taskCnt; i++ {
		d.AddTask(func() (any, error) {
			completed.Inc()
			return nil, nil
		})
	}

	assert.Eventually(t, func() bool {
		return completed.Load() == int64(taskCnt)
	}, 10*time.Second, time.Second)
}
