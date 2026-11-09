package segments

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestQueryViewRequiresMaterializedFunctionOutputs(t *testing.T) {
	for _, kind := range []schemapb.DataType{schemapb.DataType_SparseFloatVector, schemapb.DataType_BinaryVector} {
		t.Run(kind.String(), func(t *testing.T) {
			collection := NewCollectionWithoutSegcoreForTest(1, &schemapb.CollectionSchema{Version: 2, Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
				{FieldID: 101, Name: "derived", DataType: kind, IsFunctionOutput: true},
				{FieldID: 102, Name: "nullable", DataType: schemapb.DataType_Int64, Nullable: true},
			}})
			collection.loadFields = typeutil.NewSet[int64](100, 101, 102)
			physical := &LocalSegment{baseSegment: baseSegment{loadInfo: atomic.NewPointer(&querypb.SegmentLoadInfo{SegmentID: 10})}}
			materialized, indexed := false, false
			fields := mockey.Mock((*LocalSegment).HasFieldData).To(func(_ *LocalSegment, id int64) bool {
				require.EqualValues(t, 101, id)
				return materialized
			}).Build()
			defer fields.UnPatch()
			indexes := mockey.Mock((*LocalSegment).ExistIndex).To(func(_ *LocalSegment, id int64) bool {
				require.EqualValues(t, 101, id)
				return indexed
			}).Build()
			defer indexes.UnPatch()
			require.ErrorIs(t, physical.ValidateQueryViewFields(collection), merr.ErrServiceNotReady)
			materialized = true
			require.NoError(t, physical.ValidateQueryViewFields(collection))
			materialized, indexed = false, true
			require.NoError(t, physical.ValidateQueryViewFields(collection), "an index can serve without a separate raw column")
			indexed = false
			collection.loadFields.Remove(101)
			require.NoError(t, physical.ValidateQueryViewFields(collection), "partial-load views require only their selected fields")
		})
	}
}
