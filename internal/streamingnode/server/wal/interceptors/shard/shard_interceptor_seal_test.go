package shard

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/interceptors/shard/mock_shards"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors/shard/shards"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// newSealTestInterceptor builds an interceptor whose shard manager reports the
// given per-row main-index size and variable-size-vector flag for the seal-size
// fast path.
func newSealTestInterceptor(t *testing.T, perRecord int, hasVariable bool, schemaErr error) *shardInterceptor {
	t.Helper()
	paramtable.Init()
	shardManager := mock_shards.NewMockShardManager(t)
	shardManager.EXPECT().GetMainIndexSizeInfo(mock.Anything, mock.Anything).Return(perRecord, hasVariable, schemaErr).Maybe()
	return &shardInterceptor{shardManager: shardManager}
}

func setSizeMetricForTest(t *testing.T, metric string) {
	t.Helper()
	paramtable.Get().Save(paramtable.Get().DataCoordCfg.SizeMetric.Key, metric)
	t.Cleanup(func() { paramtable.Get().Reset(paramtable.Get().DataCoordCfg.SizeMetric.Key) })
}

func buildInsertMsgForTest(t *testing.T, rows uint64, fieldsData []*schemapb.FieldData) message.MutableInsertMessageV1 {
	t.Helper()
	msg := message.NewInsertMessageBuilderV1().
		WithVChannel("v1").
		WithHeader(&messagespb.InsertMessageHeader{
			CollectionId: 1,
			Partitions: []*messagespb.PartitionSegmentAssignment{
				{PartitionId: 1, Rows: rows},
			},
		}).
		WithBody(&msgpb.InsertRequest{NumRows: rows, FieldsData: fieldsData}).
		MustBuildMutable()
	return message.MustAsMutableInsertMessageV1(msg)
}

func TestEstimateSealSizeWholeRowMetric(t *testing.T) {
	impl := newSealTestInterceptor(t, 0, false, nil)
	insertMsg := buildInsertMsgForTest(t, 4, []*schemapb.FieldData{
		{
			FieldId: 10,
			Type:    schemapb.DataType_FloatVector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  8,
				Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, 32)}},
			}},
		},
	})
	assert.Equal(t, uint64(0), impl.estimateSealSize(context.Background(), insertMsg, 1, 1))
}

func TestEstimateSealSizeDenseFastPath(t *testing.T) {
	setSizeMetricForTest(t, typeutil.SizeMetricMainIndex)
	impl := newSealTestInterceptor(t, 8*4, false, nil)
	insertMsg := buildInsertMsgForTest(t, 4, []*schemapb.FieldData{
		{
			FieldId: 10,
			Type:    schemapb.DataType_FloatVector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  8,
				Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, 32)}},
			}},
		},
	})
	// O(1) fast path: rows × dim × element_size, no body parsing.
	assert.Equal(t, uint64(4*8*4), impl.estimateSealSize(context.Background(), insertMsg, 1, 1))
}

func TestEstimateSealSizeMeasuredPicksLargestVectorColumn(t *testing.T) {
	setSizeMetricForTest(t, typeutil.SizeMetricMainIndex)
	// hasVariable forces the measured path despite a per-record estimate.
	impl := newSealTestInterceptor(t, 8*4, true, nil)
	insertMsg := buildInsertMsgForTest(t, 2, []*schemapb.FieldData{
		{
			FieldId: 10,
			Type:    schemapb.DataType_FloatVector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  8,
				Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, 16)}},
			}},
		},
		{
			FieldId: 11,
			Type:    schemapb.DataType_SparseFloatVector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Data: &schemapb.VectorField_SparseFloatVector{SparseFloatVector: &schemapb.SparseFloatArray{Contents: [][]byte{{1, 2, 3, 4, 5}, {6, 7, 8}}}},
			}},
		},
	})
	// Sparse present forces the measured path; the largest vector column wins
	// (dense = 16 floats × 4 = 64 bytes > sparse contents = 8 bytes).
	assert.Equal(t, uint64(64), impl.estimateSealSize(context.Background(), insertMsg, 1, 1))
}

func TestEstimateSealSizeMeasuredWithoutSchema(t *testing.T) {
	setSizeMetricForTest(t, typeutil.SizeMetricMainIndex)
	impl := newSealTestInterceptor(t, 0, false, shards.ErrCollectionSchemaNotFound)
	insertMsg := buildInsertMsgForTest(t, 4, []*schemapb.FieldData{
		{
			FieldId: 10,
			Type:    schemapb.DataType_FloatVector,
			Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
				Dim:  8,
				Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: make([]float32, 32)}},
			}},
		},
	})
	// Schema unavailable: the measured path still sizes the dominant vector
	// column from the insert body.
	assert.Equal(t, uint64(32*4), impl.estimateSealSize(context.Background(), insertMsg, 1, 1))
}

func TestEstimateSealSizeWithoutBodyData(t *testing.T) {
	setSizeMetricForTest(t, typeutil.SizeMetricMainIndex)
	// No body fields data but the schema has a dense vector → schema-derived
	// fast path rows × perRecord.
	impl := newSealTestInterceptor(t, 8*4, false, nil)
	insertMsg := buildInsertMsgForTest(t, 3, nil)
	assert.Equal(t, uint64(3*8*4), impl.estimateSealSize(context.Background(), insertMsg, 1, 1))
}
