package segcore_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/util/mock_segcore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/v2/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestGrowingSegment(t *testing.T) {
	paramtable.Init()
	localDataRootPath := filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), typeutil.QueryNodeRole)
	initcore.InitLocalChunkManager(localDataRootPath)
	err := initcore.InitMmapManager(paramtable.Get())
	assert.NoError(t, err)

	collectionID := int64(100)
	segmentID := int64(100)

	schema := mock_segcore.GenTestCollectionSchema("test-reduce", schemapb.DataType_Int64, true)
	collection, err := segcore.CreateCCollection(&segcore.CreateCCollectionRequest{
		CollectionID: collectionID,
		Schema:       schema,
		IndexMeta:    mock_segcore.GenTestIndexMeta(collectionID, schema),
	})
	assert.NoError(t, err)
	assert.NotNil(t, collection)
	defer collection.Release()

	segment, err := segcore.CreateCSegment(&segcore.CreateCSegmentRequest{
		Collection:  collection,
		SegmentID:   segmentID,
		SegmentType: segcore.SegmentTypeGrowing,
		IsSorted:    false,
	})

	assert.NoError(t, err)
	assert.NotNil(t, segment)
	defer segment.Release()

	assert.Equal(t, segmentID, segment.ID())
	assert.Equal(t, int64(0), segment.RowNum())
	assert.Zero(t, segment.MemSize())
	assert.True(t, segment.HasRawData(0))
	assertEqualCount(t, collection, segment, 0)

	insertMsg, err := mock_segcore.GenInsertMsg(collection, 1, segmentID, 100)
	assert.NoError(t, err)
	insertResult, err := segment.Insert(context.Background(), &segcore.InsertRequest{
		RowIDs:     insertMsg.RowIDs,
		Timestamps: insertMsg.Timestamps,
		Record: &segcorepb.InsertRecord{
			FieldsData: insertMsg.FieldsData,
			NumRows:    int64(len(insertMsg.RowIDs)),
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, insertResult)
	assert.Equal(t, int64(100), insertResult.InsertedRows)

	assert.Equal(t, int64(100), segment.RowNum())
	assertEqualCount(t, collection, segment, 100)

	pk := storage.NewInt64PrimaryKeys(1)
	pk.Append(storage.NewInt64PrimaryKey(10))
	deleteResult, err := segment.Delete(context.Background(), &segcore.DeleteRequest{
		PrimaryKeys: pk,
		Timestamps: []typeutil.Timestamp{
			1000,
		},
	})
	assert.NoError(t, err)
	assert.NotNil(t, deleteResult)

	assert.Equal(t, int64(99), segment.RowNum())
}

func assertEqualCount(
	t *testing.T,
	collection *segcore.CCollection,
	segment segcore.CSegment,
	count int64,
) {
	plan := planpb.PlanNode{
		Node: &planpb.PlanNode_Query{
			Query: &planpb.QueryPlanNode{
				IsCount: true,
			},
		},
	}
	expr, err := proto.Marshal(&plan)
	assert.NoError(t, err)
	retrievePlan, err := segcore.NewRetrievePlan(
		collection,
		expr,
		typeutil.MaxTimestamp,
		100)
	defer retrievePlan.Delete()

	assert.True(t, retrievePlan.ShouldIgnoreNonPk())
	assert.False(t, retrievePlan.IsIgnoreNonPk())
	retrievePlan.SetIgnoreNonPk(true)
	assert.True(t, retrievePlan.IsIgnoreNonPk())
	assert.NotZero(t, retrievePlan.MsgID())

	assert.NotNil(t, retrievePlan)
	assert.NoError(t, err)

	retrieveResult, err := segment.Retrieve(context.Background(), retrievePlan)
	assert.NotNil(t, retrieveResult)
	assert.NoError(t, err)
	result, err := retrieveResult.GetResult()
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, count, result.AllRetrieveCount)
	retrieveResult.Release()

	retrieveResult2, err := segment.RetrieveByOffsets(context.Background(), &segcore.RetrievePlanWithOffsets{
		RetrievePlan: retrievePlan,
		Offsets:      []int64{0, 1, 2, 3, 4},
	})
	assert.NoError(t, err)
	assert.NotNil(t, retrieveResult2)
	retrieveResult2.Release()
}
