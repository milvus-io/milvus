package materialize

import (
	"github.com/apache/arrow/go/v17/arrow"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// carried maps each field type Materialize carries to the Arrow type the
// retrieve export sends it as (WorthCarryingAsArrow in segment_c.cpp, design
// doc 20261019 §3). The proxy routes a request to Materialize only when it
// outputs these types (Carries), and the gather reads each column as this
// Arrow type.
var carried = map[schemapb.DataType]arrow.Type{
	schemapb.DataType_Bool:           arrow.BOOL,
	schemapb.DataType_Int8:           arrow.INT32,
	schemapb.DataType_Int16:          arrow.INT32,
	schemapb.DataType_Int32:          arrow.INT32,
	schemapb.DataType_Int64:          arrow.INT64,
	schemapb.DataType_Timestamptz:    arrow.INT64,
	schemapb.DataType_Float:          arrow.FLOAT32,
	schemapb.DataType_Double:         arrow.FLOAT64,
	schemapb.DataType_VarChar:        arrow.STRING,
	schemapb.DataType_String:         arrow.STRING,
	schemapb.DataType_JSON:           arrow.BINARY,
	schemapb.DataType_Geometry:       arrow.BINARY,
	schemapb.DataType_FloatVector:    arrow.FIXED_SIZE_BINARY,
	schemapb.DataType_BinaryVector:   arrow.FIXED_SIZE_BINARY,
	schemapb.DataType_Float16Vector:  arrow.FIXED_SIZE_BINARY,
	schemapb.DataType_BFloat16Vector: arrow.FIXED_SIZE_BINARY,
	schemapb.DataType_Int8Vector:     arrow.FIXED_SIZE_BINARY,
}

// Carries reports whether Materialize carries a field of type dt.
func Carries(dt schemapb.DataType) bool {
	_, ok := carried[dt]
	return ok
}

// colKind is a column's physical layout in a batch.
type colKind uint8

const (
	kindFixed  colKind = iota // validity, values of a fixed byte width
	kindBool                  // validity, bit-packed values
	kindVarLen                // validity, int32 offsets, values
)

// colLayoutOf is the one table of the Arrow column types a batch holds,
// shared by the fused writer and the reader: the pk (int64 or utf8), the
// int64 system columns and the types the retrieve export produces (carried),
// plus the int8/int16 widths. width is the bytes per value of a kindFixed
// type. ok is false for any other type.
func colLayoutOf(dt arrow.DataType) (kind colKind, width int, ok bool) {
	switch dt.ID() {
	case arrow.BOOL:
		return kindBool, 0, true
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.FLOAT32, arrow.FLOAT64:
		return kindFixed, dt.(arrow.FixedWidthDataType).BitWidth() / 8, true
	case arrow.FIXED_SIZE_BINARY:
		w := dt.(*arrow.FixedSizeBinaryType).ByteWidth
		return kindFixed, w, w > 0
	case arrow.STRING, arrow.BINARY:
		return kindVarLen, 0, true
	}
	return 0, 0, false
}

// bodyBuffers counts the IPC buffers of a column of kind: validity and
// values, plus offsets for the var-len ones.
func bodyBuffers(kind colKind) int {
	if kind == kindVarLen {
		return 3
	}
	return 2
}

// The pad math the writer and the reader share; the bit math is arrow's
// bitutil.

type integer interface{ ~int | ~int64 }

// pad64 rounds n up to a multiple of 64, the stock IPC writer's padding of
// a buffer (ipc.kArrowAlignment).
func pad64[T integer](n T) T { return (n + 63) &^ 63 }

// The size Query's maxOutputSize guard charges rows on the Arrow path
// (queryutil's rowSizeCalculator), the one rule Batch.RowSize, the
// two-step writer's resultSize and the fused writer's sizing apply.

// sysResultSize is what rows rows of nsys system columns are charged: 8
// bytes per value, as the guard charges an int64 FieldsData value.
func sysResultSize(nsys, rows int) int64 { return int64(8 * nsys * rows) }

// colResultSize is what rows rows of one user column are charged, nulls of
// them null and varBytes the non-null var-len values' bytes: 1 per bool,
// the values' bytes of a utf8 or binary column, width per fixed-width
// value -- a vector (fixed_size_binary) only when not null, any other
// value, null or not.
func colResultSize(id arrow.Type, width, rows, nulls, varBytes int) int64 {
	switch id {
	case arrow.BOOL:
		return int64(rows)
	case arrow.STRING, arrow.BINARY:
		return int64(varBytes)
	case arrow.FIXED_SIZE_BINARY:
		return int64((rows - nulls) * width)
	default:
		return int64(rows * width)
	}
}
