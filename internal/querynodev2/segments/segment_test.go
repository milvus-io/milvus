package segments

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	storage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type SegmentSuite struct {
	suite.Suite
	chunkManager storage.ChunkManager

	// Data
	manager      *Manager
	collectionID int64
	partitionID  int64
	segmentID    int64
	collection   *Collection
	sealed       *LocalSegment
	growing      *LocalSegment
}

func (suite *SegmentSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *SegmentSuite) SetupTest() {
	var err error
	ctx := context.Background()
	msgLength := 100

	chunkManagerFactory := storage.NewChunkManagerFactoryWithParam(paramtable.Get())
	suite.chunkManager, _ = chunkManagerFactory.NewPersistentStorageChunkManager(ctx)
	initcore.InitRemoteChunkManager(paramtable.Get())

	suite.collectionID = 100
	suite.partitionID = 10
	suite.segmentID = 1

	suite.manager = NewManager()
	schema := GenTestCollectionSchema("test-reduce", schemapb.DataType_Int64)
	indexMeta := GenTestIndexMeta(suite.collectionID, schema)
	suite.manager.Collection.Put(suite.collectionID,
		schema,
		indexMeta,
		&querypb.LoadMetaInfo{
			LoadType:     querypb.LoadType_LoadCollection,
			CollectionID: suite.collectionID,
			PartitionIDs: []int64{suite.partitionID},
		},
	)
	suite.collection = suite.manager.Collection.Get(suite.collectionID)

	suite.sealed, err = NewSegment(suite.collection,
		suite.segmentID,
		suite.partitionID,
		suite.collectionID,
		"dml",
		SegmentTypeSealed,
		0,
		nil,
		nil,
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
		err = suite.sealed.LoadFieldData(binlog.FieldID, int64(msgLength), binlog)
		suite.Require().NoError(err)
	}

	suite.growing, err = NewSegment(suite.collection,
		suite.segmentID+1,
		suite.partitionID,
		suite.collectionID,
		"dml",
		SegmentTypeGrowing,
		0,
		nil,
		nil,
	)
	suite.Require().NoError(err)

	insertMsg, err := genInsertMsg(suite.collection, suite.partitionID, suite.growing.segmentID, msgLength)
	suite.Require().NoError(err)
	insertRecord, err := storage.TransferInsertMsgToInsertRecord(suite.collection.Schema(), insertMsg)
	suite.Require().NoError(err)
	err = suite.growing.Insert(insertMsg.RowIDs, insertMsg.Timestamps, insertRecord)
	suite.Require().NoError(err)

	suite.manager.Segment.Put(SegmentTypeSealed, suite.sealed)
	suite.manager.Segment.Put(SegmentTypeGrowing, suite.growing)
}

func (suite *SegmentSuite) TearDownTest() {
	ctx := context.Background()
	DeleteSegment(suite.sealed)
	DeleteSegment(suite.growing)
	DeleteCollection(suite.collection)
	suite.chunkManager.RemoveWithPrefix(ctx, paramtable.Get().MinioCfg.RootPath.GetValue())
}

func (suite *SegmentSuite) TestDelete() {
	pks, err := storage.GenInt64PrimaryKeys(0, 1)
	suite.NoError(err)

	// Test for sealed
	rowNum := suite.sealed.RowNum()
	err = suite.sealed.Delete(pks, []uint64{1000, 1000})
	suite.NoError(err)

	suite.Equal(rowNum-int64(len(pks)), suite.sealed.RowNum())
	suite.Equal(rowNum, suite.sealed.InsertCount())

	// Test for growing
	rowNum = suite.growing.RowNum()
	err = suite.growing.Delete(pks, []uint64{1000, 1000})
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

func (suite *SegmentSuite) TestValidateIndexedFieldsData() {
	result := &segcorepb.RetrieveResults{
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{5, 4, 3, 2, 9, 8, 7, 6},
				}},
		},
		Offset: []int64{5, 4, 3, 2, 9, 8, 7, 6},
		FieldsData: []*schemapb.FieldData{
			genFieldData("int64 field", 100, schemapb.DataType_Int64,
				[]int64{5, 4, 3, 2, 9, 8, 7, 6}, 1),
			genFieldData("float vector field", 101, schemapb.DataType_FloatVector,
				[]float32{5, 4, 3, 2, 9, 8, 7, 6}, 1),
		},
	}

	// no index
	err := suite.growing.ValidateIndexedFieldsData(context.Background(), result)
	suite.NoError(err)
	err = suite.sealed.ValidateIndexedFieldsData(context.Background(), result)
	suite.NoError(err)

	// with index and has raw data
	suite.sealed.AddIndex(101, &IndexedFieldInfo{
		IndexInfo: &querypb.FieldIndexInfo{
			FieldID:     101,
			EnableIndex: true,
		},
	})
	suite.True(suite.sealed.ExistIndex(101))
	err = suite.sealed.ValidateIndexedFieldsData(context.Background(), result)
	suite.NoError(err)

	// index doesn't have index type
	DeleteSegment(suite.sealed)
	suite.True(suite.sealed.ExistIndex(101))
	err = suite.sealed.ValidateIndexedFieldsData(context.Background(), result)
	suite.Error(err)

	// with index but doesn't have raw data
	index := suite.sealed.GetIndex(101)
	_, indexParams := genIndexParams(IndexHNSW, L2)
	index.IndexInfo.IndexParams = funcutil.Map2KeyValuePair(indexParams)
	DeleteSegment(suite.sealed)
	suite.True(suite.sealed.ExistIndex(101))
	err = suite.sealed.ValidateIndexedFieldsData(context.Background(), result)
	suite.Error(err)
}

func (suite *SegmentSuite) TestSegmentReleased() {
	DeleteSegment(suite.sealed)

	suite.sealed.mut.RLock()
	suite.False(suite.sealed.isValid())
	suite.sealed.mut.RUnlock()
	suite.EqualValues(0, suite.sealed.InsertCount())
	suite.EqualValues(0, suite.sealed.RowNum())
	suite.EqualValues(0, suite.sealed.MemSize())
	suite.False(suite.sealed.HasRawData(101))
}

func TestSegment(t *testing.T) {
	suite.Run(t, new(SegmentSuite))
}
