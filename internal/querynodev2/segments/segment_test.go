package segments

import (
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	storage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/stretchr/testify/suite"
)

type SegmentSuite struct {
	suite.Suite

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
	suite.collectionID = 100
	suite.partitionID = 10
	suite.segmentID = 1

	suite.manager = NewManager()
	suite.manager.Collection.Put(suite.collectionID,
		GenTestCollectionSchema("test-reduce", schemapb.DataType_Int64),
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

	insertData, err := genInsertData(100, suite.collection.Schema())
	suite.Require().NoError(err)
	insertRecord, err := storage.TransferInsertDataToInsertRecord(insertData)
	suite.Require().NoError(err)
	numRows := insertRecord.NumRows
	for _, fieldData := range insertRecord.FieldsData {
		err = suite.sealed.LoadField(numRows, fieldData)
		suite.Require().NoError(err)
	}

	insertMsg, err := genInsertMsg(suite.collection, suite.partitionID, suite.growing.segmentID, 100)
	suite.Require().NoError(err)
	insertRecord, err = storage.TransferInsertMsgToInsertRecord(suite.collection.Schema(), insertMsg)
	suite.Require().NoError(err)
	suite.growing.Insert(insertMsg.RowIDs, insertMsg.Timestamps, insertRecord)

	suite.manager.Segment.Put(SegmentTypeSealed, suite.sealed)
	suite.manager.Segment.Put(SegmentTypeGrowing, suite.growing)
}

func (suite *SegmentSuite) TearDownTest() {
	DeleteSegment(suite.sealed)
	DeleteSegment(suite.growing)
	DeleteCollection(suite.collection)
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

func TestSegment(t *testing.T) {
	suite.Run(t, new(SegmentSuite))
}
