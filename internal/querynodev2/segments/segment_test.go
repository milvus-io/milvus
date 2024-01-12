package segments

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	storage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
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
	chunkManagerFactory := NewTestChunkManagerFactory(paramtable.Get(), suite.rootPath)
	suite.chunkManager, _ = chunkManagerFactory.NewPersistentStorageChunkManager(ctx)
	initcore.InitRemoteChunkManager(paramtable.Get())

	suite.collectionID = 100
	suite.partitionID = 10
	suite.segmentID = 1

	suite.manager = NewManager()
	schema := GenTestCollectionSchema("test-reduce", schemapb.DataType_Int64)
	indexMeta := GenTestIndexMeta(suite.collectionID, schema)
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
		suite.segmentID,
		suite.partitionID,
		suite.collectionID,
		"dml",
		SegmentTypeSealed,
		0,
		nil,
		nil,
		datapb.SegmentLevel_Legacy,
	)
	suite.Require().NoError(err)

	binlogs, _, err := SaveBinLog(ctx,
		suite.collectionID,
		suite.partitionID,
		suite.segmentID,
		msgLength,
		schema,
		suite.chunkManager,
	)
	suite.Require().NoError(err)
	for _, binlog := range binlogs {
		err = suite.sealed.(*LocalSegment).LoadFieldData(ctx, binlog.FieldID, int64(msgLength), binlog, false)
		suite.Require().NoError(err)
	}

	suite.growing, err = NewSegment(ctx,
		suite.collection,
		suite.segmentID+1,
		suite.partitionID,
		suite.collectionID,
		"dml",
		SegmentTypeGrowing,
		0,
		nil,
		nil,
		datapb.SegmentLevel_Legacy,
	)
	suite.Require().NoError(err)

	insertMsg, err := genInsertMsg(suite.collection, suite.partitionID, suite.growing.ID(), msgLength)
	suite.Require().NoError(err)
	insertRecord, err := storage.TransferInsertMsgToInsertRecord(suite.collection.Schema(), insertMsg)
	suite.Require().NoError(err)
	err = suite.growing.Insert(ctx, insertMsg.RowIDs, insertMsg.Timestamps, insertRecord)
	suite.Require().NoError(err)

	suite.manager.Segment.Put(SegmentTypeSealed, suite.sealed)
	suite.manager.Segment.Put(SegmentTypeGrowing, suite.growing)
}

func (suite *SegmentSuite) TearDownTest() {
	ctx := context.Background()
	suite.sealed.Release()
	suite.growing.Release()
	DeleteCollection(suite.collection)
	suite.chunkManager.RemoveWithPrefix(ctx, suite.rootPath)
}

func (suite *SegmentSuite) TestDelete() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pks, err := storage.GenInt64PrimaryKeys(0, 1)
	suite.NoError(err)

	// Test for sealed
	rowNum := suite.sealed.RowNum()
	err = suite.sealed.Delete(ctx, pks, []uint64{1000, 1000})
	suite.NoError(err)

	suite.Equal(rowNum-int64(len(pks)), suite.sealed.RowNum())
	suite.Equal(rowNum, suite.sealed.InsertCount())

	// Test for growing
	rowNum = suite.growing.RowNum()
	err = suite.growing.Delete(ctx, pks, []uint64{1000, 1000})
	suite.NoError(err)

	suite.Equal(rowNum-int64(len(pks)), suite.growing.RowNum())
	suite.Equal(rowNum, suite.growing.InsertCount())
}

func (suite *SegmentSuite) TestHasRawData() {
	has := suite.growing.HasRawData(simpleFloatVecField.id)
	suite.True(has)
	has = suite.sealed.HasRawData(simpleFloatVecField.id)
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

func (suite *SegmentSuite) TestSegmentReleased() {
	suite.sealed.Release()

	sealed := suite.sealed.(*LocalSegment)

	sealed.ptrLock.RLock()
	suite.False(sealed.isValid())
	sealed.ptrLock.RUnlock()
	suite.EqualValues(0, sealed.RowNum())
	suite.EqualValues(0, sealed.MemSize())
	suite.False(sealed.HasRawData(101))
}

func TestSegment(t *testing.T) {
	suite.Run(t, new(SegmentSuite))
}
