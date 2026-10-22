package materialize

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Every Arrow type a carried field arrives as is one the writer and the
// reader handle.
func TestCarriedTypesHaveALayout(t *testing.T) {
	for dt, id := range carried {
		var at arrow.DataType
		switch id {
		case arrow.FIXED_SIZE_BINARY:
			at = &arrow.FixedSizeBinaryType{ByteWidth: 8}
		case arrow.BOOL:
			at = arrow.FixedWidthTypes.Boolean
		case arrow.INT32:
			at = arrow.PrimitiveTypes.Int32
		case arrow.INT64:
			at = arrow.PrimitiveTypes.Int64
		case arrow.FLOAT32:
			at = arrow.PrimitiveTypes.Float32
		case arrow.FLOAT64:
			at = arrow.PrimitiveTypes.Float64
		case arrow.STRING:
			at = arrow.BinaryTypes.String
		case arrow.BINARY:
			at = arrow.BinaryTypes.Binary
		}
		require.NotNil(t, at, "%s arrives as %s", dt, id)
		_, _, ok := colLayoutOf(at)
		require.True(t, ok, "%s arrives as %s", dt, id)
	}
}

func TestCarries(t *testing.T) {
	cases := []struct {
		dt   schemapb.DataType
		want bool
	}{
		{schemapb.DataType_Int64, true},
		{schemapb.DataType_VarChar, true},
		{schemapb.DataType_JSON, true},
		{schemapb.DataType_FloatVector, true},
		{schemapb.DataType_Text, false},
		{schemapb.DataType_Array, false},
		{schemapb.DataType_SparseFloatVector, false},
		{schemapb.DataType_ArrayOfVector, false},
	}
	for _, c := range cases {
		require.Equal(t, c.want, Carries(c.dt), c.dt.String())
	}
}

// fieldsOf is coll's SchemaHelper, which Gather resolves field ids with.
func fieldsOf(t testing.TB, coll *schemapb.CollectionSchema) *typeutil.SchemaHelper {
	t.Helper()
	h, err := typeutil.CreateSchemaHelper(coll)
	require.NoError(t, err)
	return h
}

// RowSize charges what queryutil's rowSizeCalculator charges on the Arrow
// path: 8 per system column; a fixed width, a nullable scalar included; a
// vector only when not null; 1 per bool; a var-len value's length, 0 when
// null.
func TestBatchRowSize(t *testing.T) {
	b := &Batch{Rows: 2, Sys: [][]int64{{1, 2}}, cols: []column{
		{typ: arrow.INT64, width: 8, rows: 2},
		{typ: arrow.BOOL, rows: 2},
		{typ: arrow.STRING, rows: 2, offsets: []int32{0, 3, 3}, valid: []byte{0b01}, nulls: 1},
		{typ: arrow.FIXED_SIZE_BINARY, width: 16, rows: 2, valid: []byte{0b10}, nulls: 1},
		{typ: arrow.INT32, width: 4, rows: 2, valid: []byte{0b01}, nulls: 1},
	}}
	cases := []struct {
		row  int
		want int64
	}{
		{0, 8 + 8 + 1 + 3 + 0 + 4},
		{1, 8 + 8 + 1 + 0 + 16 + 4},
	}
	for _, c := range cases {
		require.Equal(t, c.want, b.RowSize(c.row), "row %d", c.row)
	}
}
