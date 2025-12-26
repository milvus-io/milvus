package segments

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// TextSegmentSuite tests TEXT field with mixed storage (inline + LOB)
type TextSegmentSuite struct {
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

	// TEXT field ID
	textFieldID int64

	// LOB threshold for testing (64KB)
	lobThreshold int
}

func (suite *TextSegmentSuite) SetupSuite() {
	paramtable.Init()
}

func (suite *TextSegmentSuite) SetupTest() {
	var err error
	ctx := context.Background()

	suite.rootPath = suite.T().Name()
	chunkManagerFactory := storage.NewTestChunkManagerFactory(paramtable.Get(), suite.rootPath)
	suite.chunkManager, _ = chunkManagerFactory.NewPersistentStorageChunkManager(ctx)
	initcore.InitRemoteChunkManager(paramtable.Get())
	initcore.InitLocalChunkManager(suite.rootPath)
	initcore.InitMmapManager(paramtable.Get(), 1)
	initcore.InitTieredStorage(paramtable.Get())

	suite.collectionID = 100
	suite.partitionID = 10
	suite.segmentID = 1
	suite.textFieldID = 103
	suite.lobThreshold = 65536 // 64KB

	suite.manager = NewManager()

	// create schema with TEXT field
	schema := suite.genTextFieldSchema()
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

	msgLength := 10

	// create sealed segment
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
		},
	)
	suite.Require().NoError(err)

	// generate and save binlog with TEXT data
	binlogs, _, err := suite.SaveTextBinLog(ctx,
		suite.collectionID,
		suite.partitionID,
		suite.segmentID,
		msgLength,
		schema,
		suite.chunkManager,
	)
	suite.Require().NoError(err)

	// load data to sealed segment
	g, err := suite.sealed.(*LocalSegment).StartLoadData()
	suite.Require().NoError(err)
	for _, binlog := range binlogs {
		err = suite.sealed.(*LocalSegment).LoadFieldData(ctx, binlog.FieldID, int64(msgLength), binlog)
		suite.Require().NoError(err)
	}
	g.Done(nil)

	suite.manager.Segment.Put(context.Background(), SegmentTypeSealed, suite.sealed)
}

func (suite *TextSegmentSuite) TearDownTest() {
	ctx := context.Background()
	if suite.sealed != nil {
		suite.sealed.Release(context.Background())
	}
	if suite.collection != nil {
		DeleteCollection(suite.collection)
	}
	if suite.chunkManager != nil {
		suite.chunkManager.RemoveWithPrefix(ctx, suite.rootPath)
	}
}

// genTextFieldSchema generates a collection schema with TEXT field
func (suite *TextSegmentSuite) genTextFieldSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:        "test_text_collection",
		Description: "test collection with TEXT field",
		Fields: []*schemapb.FieldSchema{
			// PK field
			{
				FieldID:      100,
				Name:         "pk",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			// Vector field
			{
				FieldID:  101,
				Name:     "vec",
				DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "8"},
				},
			},
			// TEXT field - this will test mixed storage
			{
				FieldID:  suite.textFieldID,
				Name:     "text_field",
				DataType: schemapb.DataType_Text,
			},
		},
	}
}

// SaveTextBinLog saves binlog with TEXT field data (mixed sizes)
func (suite *TextSegmentSuite) SaveTextBinLog(ctx context.Context,
	collectionID, partitionID, segmentID int64,
	msgLength int,
	schema *schemapb.CollectionSchema,
	chunkManager storage.ChunkManager) ([]*datapb.FieldBinlog, []*datapb.FieldBinlog, error) {

	// Prepare all field data
	// PK field (int64)
	pkData := &storage.Int64FieldData{
		Data: make([]int64, msgLength),
	}
	for i := 0; i < msgLength; i++ {
		pkData.Data[i] = int64(i)
	}

	// Vector field
	vecData := &storage.FloatVectorFieldData{
		Data: make([]float32, msgLength*8),
		Dim:  8,
	}
	for i := 0; i < msgLength*8; i++ {
		vecData.Data[i] = float32(i)
	}

	// TEXT field with mixed sizes
	textData := &storage.StringFieldData{
		Data: make([]string, msgLength),
	}
	for i := 0; i < msgLength; i++ {
		// generate texts with mixed sizes
		if i%3 == 0 {
			// small text (< 64KB) - inline storage
			textData.Data[i] = fmt.Sprintf("small_text_%d", i)
		} else if i%3 == 1 {
			// medium text (< 64KB) - inline storage
			textData.Data[i] = strings.Repeat(fmt.Sprintf("medium_%d_", i), 1000) // ~10KB
		} else {
			// large text (> 64KB) - LOB storage
			textData.Data[i] = strings.Repeat(fmt.Sprintf("large_%d_", i), 10000) // ~100KB
		}
	}

	// RowID and Timestamp fields
	rowIDs := make([]int64, msgLength)
	timestamps := make([]int64, msgLength)
	for i := 0; i < msgLength; i++ {
		rowIDs[i] = int64(i)
		timestamps[i] = int64(i + 1)
	}

	// Create InsertData with all fields
	collMeta := &etcdpb.CollectionMeta{
		ID:     collectionID,
		Schema: schema,
	}
	insertCodec := storage.NewInsertCodecWithSchema(collMeta)

	insertData := &storage.InsertData{
		Data: map[int64]storage.FieldData{
			0:                 &storage.Int64FieldData{Data: rowIDs},     // RowIDField
			1:                 &storage.Int64FieldData{Data: timestamps}, // TimestampField
			100:               pkData,                                    // PK field
			101:               vecData,                                   // Vector field
			suite.textFieldID: textData,                                  // TEXT field
		},
	}

	// Serialize all fields together
	blobs, err := insertCodec.Serialize(partitionID, segmentID, insertData)
	if err != nil {
		return nil, nil, err
	}

	// Blobs are returned in schema field order (user-defined fields only, NOT system fields)
	// Schema fields: PK(100), Vector(101), TEXT(103)
	// So blobs[0]=PK, blobs[1]=Vector, blobs[2]=TEXT
	binlogs := make([]*datapb.FieldBinlog, 0)

	// Map blob index to field ID
	blobsToLoad := []struct {
		blobIndex int
		fieldID   int64
	}{
		{0, 100},               // PK
		{1, 101},               // Vector
		{2, suite.textFieldID}, // TEXT
	}

	for _, item := range blobsToLoad {
		if item.blobIndex >= len(blobs) {
			return nil, nil, fmt.Errorf("blob index %d out of range", item.blobIndex)
		}
		blob := blobs[item.blobIndex]
		fieldID := item.fieldID

		logPath := fmt.Sprintf("%s/%d/%d/%d/%d/0",
			suite.rootPath,
			collectionID,
			partitionID,
			segmentID,
			fieldID,
		)

		err = chunkManager.Write(ctx, logPath, blob.GetValue())
		if err != nil {
			return nil, nil, err
		}

		fieldBinlog := &datapb.FieldBinlog{
			FieldID: fieldID,
			Binlogs: []*datapb.Binlog{
				{
					LogID:         0,
					LogPath:       logPath,
					LogSize:       int64(len(blob.GetValue())),
					MemorySize:    int64(len(blob.GetValue())),
					TimestampFrom: 1,
					TimestampTo:   uint64(msgLength),
					EntriesNum:    int64(msgLength),
				},
			},
		}

		binlogs = append(binlogs, fieldBinlog)
	}

	return binlogs, nil, nil
}

// TestRetrieveTextMixedStorage tests retrieving TEXT field with mixed storage
func (suite *TextSegmentSuite) TestRetrieveTextMixedStorage() {
	ctx := context.Background()

	// create retrieve plan
	plan, err := mock_segcore.GenSimpleRetrievePlan(suite.collection.GetCCollection())
	suite.NoError(err)

	// retrieve all rows by offsets
	offsets := make([]int64, 10)
	for i := 0; i < 10; i++ {
		offsets[i] = int64(i)
	}

	result, err := suite.sealed.RetrieveByOffsets(ctx, &segcore.RetrievePlanWithOffsets{
		RetrievePlan: plan,
		Offsets:      offsets,
	})
	suite.NoError(err)
	suite.NotNil(result)

	// verify TEXT field data
	suite.Require().NotNil(result.GetFieldsData())

	var textField *schemapb.FieldData
	for _, field := range result.GetFieldsData() {
		if field.GetFieldId() == suite.textFieldID {
			textField = field
			break
		}
	}

	suite.Require().NotNil(textField, "TEXT field not found in result")
	suite.Require().NotNil(textField.GetScalars())
	suite.Require().NotNil(textField.GetScalars().GetStringData())

	texts := textField.GetScalars().GetStringData().GetData()
	suite.Require().Equal(10, len(texts), "should retrieve 10 rows")

	// verify each row's text
	for i := 0; i < 10; i++ {
		retrievedText := texts[i]

		if i%3 == 0 {
			// small text
			expected := fmt.Sprintf("small_text_%d", i)
			suite.Equal(expected, retrievedText, "small text mismatch at row %d", i)
		} else if i%3 == 1 {
			// medium text
			suite.True(strings.Contains(retrievedText, fmt.Sprintf("medium_%d_", i)),
				"medium text should contain pattern at row %d", i)
			suite.True(len(retrievedText) < suite.lobThreshold,
				"medium text should be below LOB threshold at row %d", i)
		} else {
			// large text - should be retrieved correctly from LOB
			suite.True(strings.Contains(retrievedText, fmt.Sprintf("large_%d_", i)),
				"large text should contain pattern at row %d", i)
			suite.True(len(retrievedText) > suite.lobThreshold,
				"large text should be above LOB threshold at row %d", i)
		}
	}
}

// TestRetrieveTextPartialRows tests retrieving partial rows
func (suite *TextSegmentSuite) TestRetrieveTextPartialRows() {
	ctx := context.Background()

	plan, err := mock_segcore.GenSimpleRetrievePlan(suite.collection.GetCCollection())
	suite.NoError(err)

	// retrieve only rows 0, 2, 5, 8 (mix of small and large texts)
	offsets := []int64{0, 2, 5, 8}

	result, err := suite.sealed.RetrieveByOffsets(ctx, &segcore.RetrievePlanWithOffsets{
		RetrievePlan: plan,
		Offsets:      offsets,
	})
	suite.NoError(err)
	suite.NotNil(result)

	var textField *schemapb.FieldData
	for _, field := range result.GetFieldsData() {
		if field.GetFieldId() == suite.textFieldID {
			textField = field
			break
		}
	}

	suite.Require().NotNil(textField)
	texts := textField.GetScalars().GetStringData().GetData()
	suite.Require().Equal(4, len(texts))

	// verify specific rows
	// Row 0: small text
	suite.Equal("small_text_0", texts[0])

	// Row 2: large text (> 64KB)
	suite.True(strings.Contains(texts[1], "large_2_"))
	suite.True(len(texts[1]) > suite.lobThreshold)

	// Row 5: large text
	suite.True(strings.Contains(texts[2], "large_5_"))

	// Row 8: large text
	suite.True(strings.Contains(texts[3], "large_8_"))
}

// TestTextFieldRowNum tests row count
func (suite *TextSegmentSuite) TestTextFieldRowNum() {
	rowNum := suite.sealed.RowNum()
	suite.Equal(int64(10), rowNum, "should have 10 rows")
}

func TestTextSegment(t *testing.T) {
	suite.Run(t, new(TextSegmentSuite))
}
