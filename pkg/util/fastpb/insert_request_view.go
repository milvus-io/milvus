package fastpb

import (
	"encoding/binary"
	"math"
	"reflect"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// InsertRequestViewEncoder serializes a row selection from Source directly into
// the protobuf wire representation of a msgpb.InsertRequest. It deliberately
// does not materialize destination RowIDs, Timestamps, or FieldData columns.
//
// Template supplies the output message metadata (base, collection, partition,
// segment, shard, version, and namespace). Its row-bearing fields are ignored.
// Source supplies RowIDs, Timestamps, and FieldsData. Rows is borrowed rather
// than copied and must be strictly increasing.
//
// Template, Source, Rows, and every buffer reachable from them must remain
// immutable until MarshalTo returns. This is the same ownership rule as an
// Arrow array view: the encoder owns only the view, not the referenced data.
type InsertRequestViewEncoder struct {
	template              *msgpb.InsertRequest
	source                *msgpb.InsertRequest
	rows                  []int
	firstFieldDataIndices []int64
	size                  int
}

// NewInsertRequestViewEncoder creates a borrowed row-selection view over source.
// Invalid offsets and malformed source column shapes are Milvus-internal
// contract violations, so they intentionally use ErrServiceInternal rather
// than an input-error code.
func NewInsertRequestViewEncoder(template, source *msgpb.InsertRequest, rows []int) (*InsertRequestViewEncoder, error) {
	return newInsertRequestViewEncoder(template, source, rows, nil)
}

// NewInsertRequestViewEncoderWithFirstFieldIndices is the O(1)-prefix variant
// used by a sequential message splitter. firstFieldDataIndices must be the
// result of FieldDataIdxComputer.Compute(rows[0]) captured at the first row of
// this view. Like rows, the slice is borrowed and must remain immutable until
// MarshalTo returns; a splitter may reuse one scratch slice after the builder
// synchronously consumes the encoder. Supplying it avoids rescanning nullable-
// vector validity prefixes for every split message.
func NewInsertRequestViewEncoderWithFirstFieldIndices(template, source *msgpb.InsertRequest, rows []int, firstFieldDataIndices []int64) (*InsertRequestViewEncoder, error) {
	if len(rows) > 0 && len(firstFieldDataIndices) != len(source.GetFieldsData()) {
		return nil, insertViewInternal("first field-index count %d does not match FieldsData count %d", len(firstFieldDataIndices), len(source.GetFieldsData()))
	}
	return newInsertRequestViewEncoder(template, source, rows, firstFieldDataIndices)
}

func newInsertRequestViewEncoder(template, source *msgpb.InsertRequest, rows []int, firstFieldDataIndices []int64) (*InsertRequestViewEncoder, error) {
	if template == nil {
		return nil, insertViewInternal("nil output template")
	}
	if source == nil {
		return nil, insertViewInternal("nil source request")
	}
	if len(source.GetRowData()) != 0 {
		return nil, insertViewInternal("row-based InsertRequest is not supported")
	}
	if len(rows) == 0 {
		return nil, insertViewInternal("row selection is empty")
	}

	previous := -1
	for i, row := range rows {
		if row < 0 {
			return nil, insertViewInternal("row offset at selection index %d is negative: %d", i, row)
		}
		if i > 0 && row <= previous {
			return nil, insertViewInternal("row offsets must be strictly increasing: rows[%d]=%d after %d", i, row, previous)
		}
		if row >= len(source.GetRowIDs()) {
			return nil, insertViewInternal("row offset %d exceeds RowIDs length %d", row, len(source.GetRowIDs()))
		}
		if row >= len(source.GetTimestamps()) {
			return nil, insertViewInternal("row offset %d exceeds Timestamps length %d", row, len(source.GetTimestamps()))
		}
		if source.GetNumRows() != 0 && uint64(row) >= source.GetNumRows() {
			return nil, insertViewInternal("row offset %d exceeds source NumRows %d", row, source.GetNumRows())
		}
		previous = row
	}
	if len(rows) > 0 && firstFieldDataIndices == nil {
		var err error
		firstFieldDataIndices, err = calculateFirstFieldDataIndices(source.GetFieldsData(), rows[0])
		if err != nil {
			return nil, err
		}
	}
	for fieldIndex, dataIndex := range firstFieldDataIndices {
		if dataIndex < 0 || uint64(dataIndex) > uint64(math.MaxInt) {
			return nil, insertViewInternal("first data index %d for field %d is invalid", dataIndex, fieldIndex)
		}
	}

	encoder := &InsertRequestViewEncoder{
		template:              template,
		source:                source,
		rows:                  rows,
		firstFieldDataIndices: firstFieldDataIndices,
	}
	size, err := encoder.calculateSize()
	if err != nil {
		return nil, err
	}
	encoder.size = size
	return encoder, nil
}

func calculateFirstFieldDataIndices(fields []*schemapb.FieldData, firstRow int) ([]int64, error) {
	indices := make([]int64, len(fields))
	for fieldIndex, field := range fields {
		indices[fieldIndex] = int64(firstRow)
		if field == nil || !isCompactVectorType(field.GetType()) || len(field.GetValidData()) == 0 {
			continue
		}
		valid := field.GetValidData()
		if firstRow >= len(valid) {
			return nil, insertViewInternal("first row offset %d exceeds ValidData length %d for field %q (%d)", firstRow, len(valid), field.GetFieldName(), field.GetFieldId())
		}
		dataIndex := int64(0)
		for _, isValid := range valid[:firstRow] {
			if isValid {
				dataIndex++
			}
		}
		indices[fieldIndex] = dataIndex
	}
	return indices, nil
}

func isCompactVectorType(dataType schemapb.DataType) bool {
	switch dataType {
	case schemapb.DataType_BinaryVector,
		schemapb.DataType_FloatVector,
		schemapb.DataType_Float16Vector,
		schemapb.DataType_BFloat16Vector,
		schemapb.DataType_SparseFloatVector,
		schemapb.DataType_Int8Vector:
		return true
	default:
		return false
	}
}

// EncodedSize returns the exact number of bytes MarshalTo writes.
func (e *InsertRequestViewEncoder) EncodedSize() (int, error) {
	if e == nil {
		return 0, insertViewInternal("nil encoder")
	}
	return e.size, nil
}

// MarshalTo writes the selected request into dst without constructing target
// protobuf columns. dst may be larger than EncodedSize; only its prefix is used.
func (e *InsertRequestViewEncoder) MarshalTo(dst []byte) (int, error) {
	if e == nil {
		return 0, insertViewInternal("nil encoder")
	}
	if len(dst) < e.size {
		return 0, insertViewInternal("destination buffer has %d bytes, need %d", len(dst), e.size)
	}

	w := newInsertViewMarshalWriter(dst[:0:e.size])
	if err := e.appendRequest(w); err != nil {
		return 0, err
	}
	if w.err != nil {
		return 0, w.err
	}
	if w.n != e.size || len(w.out) != e.size {
		return 0, insertViewInternal("encoded size changed while borrowed data was in use: expected %d, wrote %d", e.size, w.n)
	}
	if e.size > 0 && &w.out[0] != &dst[0] {
		return 0, insertViewInternal("encoder exceeded the caller-provided destination buffer")
	}
	return e.size, nil
}

func (e *InsertRequestViewEncoder) calculateSize() (int, error) {
	w := newInsertViewSizeWriter()
	if err := e.appendRequest(w); err != nil {
		return 0, err
	}
	if w.err != nil {
		return 0, w.err
	}
	return w.n, nil
}

func (e *InsertRequestViewEncoder) appendRequest(w *insertViewWriter) error {
	t := e.template
	if t.GetBase() != nil {
		if err := w.protoMessage(1, t.GetBase(), "insert base"); err != nil {
			return err
		}
	}
	if err := w.proto3String(2, t.GetShardName(), "shard name"); err != nil {
		return err
	}
	if err := w.proto3String(3, t.GetDbName(), "database name"); err != nil {
		return err
	}
	if err := w.proto3String(4, t.GetCollectionName(), "collection name"); err != nil {
		return err
	}
	if err := w.proto3String(5, t.GetPartitionName(), "partition name"); err != nil {
		return err
	}
	w.proto3Varint(6, uint64(t.GetDbID()))
	w.proto3Varint(7, uint64(t.GetCollectionID()))
	w.proto3Varint(8, uint64(t.GetPartitionID()))
	w.proto3Varint(9, uint64(t.GetSegmentID()))

	if err := e.appendSelectedUint64(w, 10, e.source.GetTimestamps(), "timestamps"); err != nil {
		return err
	}
	if err := e.appendSelectedInt64(w, 11, e.source.GetRowIDs(), "row IDs"); err != nil {
		return err
	}

	for i, field := range e.source.GetFieldsData() {
		if field == nil {
			return insertViewInternal("source FieldsData[%d] is nil", i)
		}
		if field.GetStructArrays() != nil {
			return insertViewInternal("source field %q (%d) still contains StructArrays; proxy must flatten struct fields before repacking", field.GetFieldName(), field.GetFieldId())
		}
		fieldSize, err := e.sizeFieldData(field)
		if err != nil {
			return err
		}
		if err := w.message(13, fieldSize, func() error {
			return e.appendFieldData(w, field)
		}); err != nil {
			return err
		}
	}

	w.proto3Varint(14, uint64(len(e.rows)))
	w.proto3Varint(15, uint64(t.GetVersion()))
	if t.Namespace != nil {
		if err := w.string(16, *t.Namespace, true, "namespace"); err != nil {
			return err
		}
	}

	// A shallow-copy-and-overwrite implementation would retain template unknown
	// fields. Preserve them here as well so future metadata remains wire-safe.
	w.raw(t.ProtoReflect().GetUnknown())
	return w.err
}

func (e *InsertRequestViewEncoder) appendSelectedUint64(w *insertViewWriter, fieldNumber protowire.Number, values []uint64, label string) error {
	if len(e.rows) == 0 {
		return nil
	}
	payloadSize := 0
	for _, row := range e.rows {
		if row >= len(values) {
			return insertViewInternal("row offset %d exceeds %s length %d", row, label, len(values))
		}
		payloadSize += protowire.SizeVarint(values[row])
	}
	return w.message(fieldNumber, payloadSize, func() error {
		for _, row := range e.rows {
			w.varint(values[row])
		}
		return w.err
	})
}

func (e *InsertRequestViewEncoder) appendSelectedInt64(w *insertViewWriter, fieldNumber protowire.Number, values []int64, label string) error {
	if len(e.rows) == 0 {
		return nil
	}
	payloadSize := 0
	for _, row := range e.rows {
		if row >= len(values) {
			return insertViewInternal("row offset %d exceeds %s length %d", row, label, len(values))
		}
		payloadSize += protowire.SizeVarint(uint64(values[row]))
	}
	return w.message(fieldNumber, payloadSize, func() error {
		for _, row := range e.rows {
			w.varint(uint64(values[row]))
		}
		return w.err
	})
}

func (e *InsertRequestViewEncoder) sizeFieldData(field *schemapb.FieldData) (int, error) {
	w := newInsertViewSizeWriter()
	if err := e.appendFieldData(w, field); err != nil {
		return 0, err
	}
	if w.err != nil {
		return 0, w.err
	}
	return w.n, nil
}

func (e *InsertRequestViewEncoder) appendFieldData(w *insertViewWriter, field *schemapb.FieldData) error {
	w.proto3Varint(1, uint64(field.GetType()))
	if err := w.proto3String(2, field.GetFieldName(), "field name"); err != nil {
		return err
	}

	switch value := field.Field.(type) {
	case nil:
		// Match AppendFieldData: metadata survives even if the source has no
		// scalar/vector payload.
	case *schemapb.FieldData_Scalars:
		if value.Scalars == nil {
			return insertViewInternal("scalar field %q (%d) has a nil ScalarField", field.GetFieldName(), field.GetFieldId())
		}
		scalarSize, err := e.sizeScalarField(value.Scalars)
		if err != nil {
			return err
		}
		if err := w.message(3, scalarSize, func() error {
			return e.appendScalarField(w, value.Scalars)
		}); err != nil {
			return err
		}
	case *schemapb.FieldData_Vectors:
		if value.Vectors == nil {
			return insertViewInternal("vector field %q (%d) has a nil VectorField", field.GetFieldName(), field.GetFieldId())
		}
		vectorSize, err := e.sizeVectorField(field, value.Vectors)
		if err != nil {
			return err
		}
		if err := w.message(4, vectorSize, func() error {
			return e.appendVectorField(w, field, value.Vectors)
		}); err != nil {
			return err
		}
	case *schemapb.FieldData_StructArrays:
		return insertViewInternal("source field %q (%d) still contains StructArrays; proxy must flatten struct fields before repacking", field.GetFieldName(), field.GetFieldId())
	default:
		return insertViewInternal("source field %q (%d) has unsupported FieldData oneof %T", field.GetFieldName(), field.GetFieldId(), value)
	}

	w.proto3Varint(5, uint64(field.GetFieldId()))
	if field.GetIsDynamic() {
		w.varintField(6, 1)
	}
	if len(field.GetValidData()) > 0 && len(e.rows) > 0 {
		valid := field.GetValidData()
		for _, row := range e.rows {
			if row >= len(valid) {
				return insertViewInternal("row offset %d exceeds ValidData length %d for field %q (%d)", row, len(valid), field.GetFieldName(), field.GetFieldId())
			}
		}
		if err := w.message(7, len(e.rows), func() error {
			for _, row := range e.rows {
				if valid[row] {
					w.varint(1)
				} else {
					w.varint(0)
				}
			}
			return w.err
		}); err != nil {
			return err
		}
	}
	return w.err
}

func (e *InsertRequestViewEncoder) sizeScalarField(field *schemapb.ScalarField) (int, error) {
	w := newInsertViewSizeWriter()
	if err := e.appendScalarField(w, field); err != nil {
		return 0, err
	}
	if w.err != nil {
		return 0, w.err
	}
	return w.n, nil
}

func (e *InsertRequestViewEncoder) appendScalarField(w *insertViewWriter, field *schemapb.ScalarField) error {
	switch value := field.Data.(type) {
	case nil:
		return nil
	case *schemapb.ScalarField_BoolData:
		return e.appendBoolArray(w, 1, value.BoolData)
	case *schemapb.ScalarField_IntData:
		return e.appendInt32Array(w, 2, value.IntData, "int scalar")
	case *schemapb.ScalarField_LongData:
		return e.appendInt64Array(w, 3, value.LongData, "long scalar")
	case *schemapb.ScalarField_FloatData:
		return e.appendFloat32Array(w, 4, value.FloatData)
	case *schemapb.ScalarField_DoubleData:
		return e.appendFloat64Array(w, 5, value.DoubleData)
	case *schemapb.ScalarField_StringData:
		return e.appendStringArray(w, 6, value.StringData, "string scalar")
	// These cold oneofs are declared by the current schema but are not handled
	// by the old row-at-a-time AppendFieldData oracle. Encoding them here is a
	// forward-safe row selection; the differential test covers the old oracle's
	// real support set separately.
	case *schemapb.ScalarField_BytesData:
		return e.appendBytesArray(w, 7, value.BytesData, "bytes scalar")
	case *schemapb.ScalarField_ArrayData:
		return e.appendArrayArray(w, 8, value.ArrayData)
	case *schemapb.ScalarField_JsonData:
		return e.appendRawBytesRows(w, 9, value.JsonData.GetData(), "JSON scalar", value.JsonData == nil)
	case *schemapb.ScalarField_GeometryData:
		return e.appendRawBytesRows(w, 10, value.GeometryData.GetData(), "geometry scalar", value.GeometryData == nil)
	case *schemapb.ScalarField_TimestamptzData:
		return e.appendInt64Values(w, 11, value.TimestamptzData.GetData(), "timestamptz scalar", value.TimestamptzData == nil)
	case *schemapb.ScalarField_GeometryWktData:
		return e.appendStringValues(w, 12, value.GeometryWktData.GetData(), "geometry WKT scalar", value.GeometryWktData == nil)
	case *schemapb.ScalarField_MolData:
		return e.appendRawBytesRows(w, 13, value.MolData.GetData(), "molecular scalar", value.MolData == nil)
	case *schemapb.ScalarField_MolSmilesData:
		return e.appendStringValues(w, 14, value.MolSmilesData.GetData(), "molecular SMILES scalar", value.MolSmilesData == nil)
	case *schemapb.ScalarField_DateData:
		return e.appendInt32Values(w, 15, value.DateData.GetData(), "date scalar", value.DateData == nil)
	case *schemapb.ScalarField_TimeData:
		return e.appendInt64Values(w, 16, value.TimeData.GetData(), "time scalar", value.TimeData == nil)
	default:
		return insertViewInternal("unsupported ScalarField oneof %T", value)
	}
}

func (e *InsertRequestViewEncoder) appendBoolArray(w *insertViewWriter, fieldNumber protowire.Number, values *schemapb.BoolArray) error {
	if values == nil {
		return insertViewInternal("nil bool scalar array")
	}
	for _, row := range e.rows {
		if row >= len(values.GetData()) {
			return insertViewInternal("row offset %d exceeds bool scalar length %d", row, len(values.GetData()))
		}
	}
	arraySize := 0
	if len(e.rows) > 0 {
		arraySize = protowire.SizeTag(1) + protowire.SizeBytes(len(e.rows))
	}
	return w.message(fieldNumber, arraySize, func() error {
		if len(e.rows) == 0 {
			return nil
		}
		return w.message(1, len(e.rows), func() error {
			for _, row := range e.rows {
				if values.GetData()[row] {
					w.varint(1)
				} else {
					w.varint(0)
				}
			}
			return w.err
		})
	})
}

func (e *InsertRequestViewEncoder) appendInt32Array(w *insertViewWriter, fieldNumber protowire.Number, values *schemapb.IntArray, label string) error {
	if values == nil {
		return insertViewInternal("nil %s array", label)
	}
	return e.appendInt32Values(w, fieldNumber, values.GetData(), label, false)
}

func (e *InsertRequestViewEncoder) appendInt64Array(w *insertViewWriter, fieldNumber protowire.Number, values *schemapb.LongArray, label string) error {
	if values == nil {
		return insertViewInternal("nil %s array", label)
	}
	return e.appendInt64Values(w, fieldNumber, values.GetData(), label, false)
}

func (e *InsertRequestViewEncoder) appendInt32Values(w *insertViewWriter, fieldNumber protowire.Number, values []int32, label string, nilArray bool) error {
	if nilArray {
		return insertViewInternal("nil %s array", label)
	}
	payloadSize := 0
	for _, row := range e.rows {
		if row >= len(values) {
			return insertViewInternal("row offset %d exceeds %s length %d", row, label, len(values))
		}
		payloadSize += protowire.SizeVarint(uint64(values[row]))
	}
	arraySize := 0
	if len(e.rows) > 0 {
		arraySize = protowire.SizeTag(1) + protowire.SizeBytes(payloadSize)
	}
	return w.message(fieldNumber, arraySize, func() error {
		if len(e.rows) == 0 {
			return nil
		}
		return w.message(1, payloadSize, func() error {
			for _, row := range e.rows {
				w.varint(uint64(values[row]))
			}
			return w.err
		})
	})
}

func (e *InsertRequestViewEncoder) appendInt64Values(w *insertViewWriter, fieldNumber protowire.Number, values []int64, label string, nilArray bool) error {
	if nilArray {
		return insertViewInternal("nil %s array", label)
	}
	payloadSize := 0
	for _, row := range e.rows {
		if row >= len(values) {
			return insertViewInternal("row offset %d exceeds %s length %d", row, label, len(values))
		}
		payloadSize += protowire.SizeVarint(uint64(values[row]))
	}
	arraySize := 0
	if len(e.rows) > 0 {
		arraySize = protowire.SizeTag(1) + protowire.SizeBytes(payloadSize)
	}
	return w.message(fieldNumber, arraySize, func() error {
		if len(e.rows) == 0 {
			return nil
		}
		return w.message(1, payloadSize, func() error {
			for _, row := range e.rows {
				w.varint(uint64(values[row]))
			}
			return w.err
		})
	})
}

func (e *InsertRequestViewEncoder) appendFloat32Array(w *insertViewWriter, fieldNumber protowire.Number, values *schemapb.FloatArray) error {
	if values == nil {
		return insertViewInternal("nil float scalar array")
	}
	for _, row := range e.rows {
		if row >= len(values.GetData()) {
			return insertViewInternal("row offset %d exceeds float scalar length %d", row, len(values.GetData()))
		}
	}
	payloadSize, err := checkedProduct(len(e.rows), 4, "float scalar payload")
	if err != nil {
		return err
	}
	arraySize := 0
	if payloadSize > 0 {
		arraySize = protowire.SizeTag(1) + protowire.SizeBytes(payloadSize)
	}
	return w.message(fieldNumber, arraySize, func() error {
		if payloadSize == 0 {
			return nil
		}
		return w.message(1, payloadSize, func() error {
			for _, row := range e.rows {
				w.fixed32(math.Float32bits(values.GetData()[row]))
			}
			return w.err
		})
	})
}

func (e *InsertRequestViewEncoder) appendFloat64Array(w *insertViewWriter, fieldNumber protowire.Number, values *schemapb.DoubleArray) error {
	if values == nil {
		return insertViewInternal("nil double scalar array")
	}
	for _, row := range e.rows {
		if row >= len(values.GetData()) {
			return insertViewInternal("row offset %d exceeds double scalar length %d", row, len(values.GetData()))
		}
	}
	payloadSize, err := checkedProduct(len(e.rows), 8, "double scalar payload")
	if err != nil {
		return err
	}
	arraySize := 0
	if payloadSize > 0 {
		arraySize = protowire.SizeTag(1) + protowire.SizeBytes(payloadSize)
	}
	return w.message(fieldNumber, arraySize, func() error {
		if payloadSize == 0 {
			return nil
		}
		return w.message(1, payloadSize, func() error {
			for _, row := range e.rows {
				w.fixed64(math.Float64bits(values.GetData()[row]))
			}
			return w.err
		})
	})
}

func (e *InsertRequestViewEncoder) appendStringArray(w *insertViewWriter, fieldNumber protowire.Number, values *schemapb.StringArray, label string) error {
	if values == nil {
		return insertViewInternal("nil %s array", label)
	}
	return e.appendStringValues(w, fieldNumber, values.GetData(), label, false)
}

func (e *InsertRequestViewEncoder) appendStringValues(w *insertViewWriter, fieldNumber protowire.Number, values []string, label string, nilArray bool) error {
	if nilArray {
		return insertViewInternal("nil %s array", label)
	}
	arraySize := 0
	for _, row := range e.rows {
		if row >= len(values) {
			return insertViewInternal("row offset %d exceeds %s length %d", row, label, len(values))
		}
		value := values[row]
		if !utf8.ValidString(value) {
			return insertViewInternal("%s row %d contains invalid UTF-8", label, row)
		}
		arraySize += protowire.SizeTag(1) + protowire.SizeBytes(len(value))
	}
	return w.message(fieldNumber, arraySize, func() error {
		for _, row := range e.rows {
			w.stringBytes(1, values[row])
		}
		return w.err
	})
}

func (e *InsertRequestViewEncoder) appendBytesArray(w *insertViewWriter, fieldNumber protowire.Number, values *schemapb.BytesArray, label string) error {
	if values == nil {
		return insertViewInternal("nil %s array", label)
	}
	return e.appendRawBytesRows(w, fieldNumber, values.GetData(), label, false)
}

func (e *InsertRequestViewEncoder) appendRawBytesRows(w *insertViewWriter, fieldNumber protowire.Number, values [][]byte, label string, nilArray bool) error {
	if nilArray {
		return insertViewInternal("nil %s array", label)
	}
	arraySize := 0
	for _, row := range e.rows {
		if row >= len(values) {
			return insertViewInternal("row offset %d exceeds %s length %d", row, label, len(values))
		}
		arraySize += protowire.SizeTag(1) + protowire.SizeBytes(len(values[row]))
	}
	return w.message(fieldNumber, arraySize, func() error {
		for _, row := range e.rows {
			w.bytes(1, values[row])
		}
		return w.err
	})
}

func (e *InsertRequestViewEncoder) appendArrayArray(w *insertViewWriter, fieldNumber protowire.Number, values *schemapb.ArrayArray) error {
	if values == nil {
		return insertViewInternal("nil array scalar payload")
	}
	arraySize := 0
	for _, row := range e.rows {
		if row >= len(values.GetData()) {
			return insertViewInternal("row offset %d exceeds array scalar length %d", row, len(values.GetData()))
		}
		itemSize := nullableProtoSize(values.GetData()[row])
		arraySize += protowire.SizeTag(1) + protowire.SizeBytes(itemSize)
	}
	if values.GetElementType() != schemapb.DataType_None {
		arraySize += protowire.SizeTag(2) + protowire.SizeVarint(uint64(values.GetElementType()))
	}
	return w.message(fieldNumber, arraySize, func() error {
		for _, row := range e.rows {
			if err := w.protoMessage(1, values.GetData()[row], "array scalar row"); err != nil {
				return err
			}
		}
		w.proto3Varint(2, uint64(values.GetElementType()))
		return w.err
	})
}

func (e *InsertRequestViewEncoder) sizeVectorField(field *schemapb.FieldData, vector *schemapb.VectorField) (int, error) {
	w := newInsertViewSizeWriter()
	if err := e.appendVectorField(w, field, vector); err != nil {
		return 0, err
	}
	if w.err != nil {
		return 0, w.err
	}
	return w.n, nil
}

func (e *InsertRequestViewEncoder) appendVectorField(w *insertViewWriter, field *schemapb.FieldData, vector *schemapb.VectorField) error {
	switch value := vector.Data.(type) {
	case nil:
		w.proto3Varint(1, uint64(vector.GetDim()))
		return w.err
	case *schemapb.VectorField_BinaryVector:
		return e.appendDenseBytesVector(w, field, vector.GetDim(), 3, value.BinaryVector, 8, "binary vector")
	case *schemapb.VectorField_FloatVector:
		return e.appendFloatVector(w, field, vector.GetDim(), value.FloatVector)
	case *schemapb.VectorField_Float16Vector:
		return e.appendDenseBytesVector(w, field, vector.GetDim(), 4, value.Float16Vector, 2, "float16 vector")
	case *schemapb.VectorField_Bfloat16Vector:
		return e.appendDenseBytesVector(w, field, vector.GetDim(), 5, value.Bfloat16Vector, 2, "bfloat16 vector")
	case *schemapb.VectorField_SparseFloatVector:
		return e.appendSparseVector(w, field, vector, value.SparseFloatVector)
	case *schemapb.VectorField_Int8Vector:
		return e.appendDenseBytesVector(w, field, vector.GetDim(), 7, value.Int8Vector, 1, "int8 vector")
	case *schemapb.VectorField_VectorArray:
		return e.appendVectorArray(w, vector, value.VectorArray)
	default:
		return insertViewInternal("field %q (%d) has unsupported VectorField oneof %T", field.GetFieldName(), field.GetFieldId(), value)
	}
}

// appendDenseBytesVector handles byte-backed dense vectors. unit denotes bytes
// per dimension, except binary vectors where unit=8 means dimensions per byte.
func (e *InsertRequestViewEncoder) appendDenseBytesVector(w *insertViewWriter, field *schemapb.FieldData, dim int64, fieldNumber protowire.Number, data []byte, unit int, label string) error {
	stride, err := denseVectorStride(dim, unit, label)
	if err != nil {
		return err
	}
	selected, err := e.countSelectedVectorRows(field, func(dataIndex int) error {
		_, _, err := vectorBounds(dataIndex, stride, len(data), field, label)
		return err
	})
	if err != nil {
		return err
	}
	w.proto3Varint(1, uint64(dim))
	if selected == 0 {
		return w.err
	}
	payloadSize, err := checkedProduct(selected, stride, label+" payload")
	if err != nil {
		return err
	}
	return w.message(fieldNumber, payloadSize, func() error {
		_, err := e.countSelectedVectorRows(field, func(dataIndex int) error {
			start, end, err := vectorBounds(dataIndex, stride, len(data), field, label)
			if err != nil {
				return err
			}
			w.raw(data[start:end])
			return w.err
		})
		return err
	})
}

func (e *InsertRequestViewEncoder) appendFloatVector(w *insertViewWriter, field *schemapb.FieldData, dim int64, values *schemapb.FloatArray) error {
	if values == nil {
		return insertViewInternal("field %q (%d) has a nil float vector payload", field.GetFieldName(), field.GetFieldId())
	}
	stride, err := nonNegativeDim(dim, "float vector")
	if err != nil {
		return err
	}
	selected, err := e.countSelectedVectorRows(field, func(dataIndex int) error {
		_, _, err := vectorBounds(dataIndex, stride, len(values.GetData()), field, "float vector")
		return err
	})
	if err != nil {
		return err
	}
	w.proto3Varint(1, uint64(dim))
	if selected == 0 {
		return w.err
	}
	elementCount, err := checkedProduct(selected, stride, "float vector element count")
	if err != nil {
		return err
	}
	payloadSize, err := checkedProduct(elementCount, 4, "float vector payload")
	if err != nil {
		return err
	}
	floatArraySize := 0
	if payloadSize > 0 {
		floatArraySize = protowire.SizeTag(1) + protowire.SizeBytes(payloadSize)
	}
	return w.message(2, floatArraySize, func() error {
		if payloadSize == 0 {
			return nil
		}
		return w.message(1, payloadSize, func() error {
			_, err := e.countSelectedVectorRows(field, func(dataIndex int) error {
				start, end, err := vectorBounds(dataIndex, stride, len(values.GetData()), field, "float vector")
				if err != nil {
					return err
				}
				w.raw(f32ReadOnlyBytes(values.GetData()[start:end]))
				return w.err
			})
			return err
		})
	})
}

func (e *InsertRequestViewEncoder) appendSparseVector(w *insertViewWriter, field *schemapb.FieldData, vector *schemapb.VectorField, values *schemapb.SparseFloatArray) error {
	if values == nil {
		return insertViewInternal("field %q (%d) has a nil sparse vector payload", field.GetFieldName(), field.GetFieldId())
	}
	selected := 0
	sparseSize := 0
	selectedDim := int64(0)
	_, err := e.countSelectedVectorRows(field, func(dataIndex int) error {
		if dataIndex < 0 || dataIndex >= len(values.GetContents()) {
			return insertViewInternal("compact sparse vector index %d exceeds payload rows %d for field %q (%d)", dataIndex, len(values.GetContents()), field.GetFieldName(), field.GetFieldId())
		}
		row := values.GetContents()[dataIndex]
		rowDim, err := sparseRowDim(row)
		if err != nil {
			return merr.Wrapf(err, "field %q (%d) has malformed sparse row %d", field.GetFieldName(), field.GetFieldId(), dataIndex)
		}
		if rowDim > selectedDim {
			selectedDim = rowDim
		}
		selected++
		sparseSize += protowire.SizeTag(1) + protowire.SizeBytes(len(row))
		return nil
	})
	if err != nil {
		return err
	}

	outerDim := vector.GetDim()
	if selected > 0 {
		// This intentionally matches AppendFieldData's sparse-vector behavior:
		// once data is present, VectorField.Dim comes from SparseFloatArray.Dim.
		outerDim = values.GetDim()
	}
	w.proto3Varint(1, uint64(outerDim))
	if selected == 0 {
		return w.err
	}
	if selectedDim != 0 {
		sparseSize += protowire.SizeTag(2) + protowire.SizeVarint(uint64(selectedDim))
	}
	return w.message(6, sparseSize, func() error {
		_, err := e.countSelectedVectorRows(field, func(dataIndex int) error {
			w.bytes(1, values.GetContents()[dataIndex])
			return w.err
		})
		if err != nil {
			return err
		}
		w.proto3Varint(2, uint64(selectedDim))
		return w.err
	})
}

func (e *InsertRequestViewEncoder) appendVectorArray(w *insertViewWriter, vector *schemapb.VectorField, values *schemapb.VectorArray) error {
	if values == nil {
		return insertViewInternal("nil ArrayOfVector payload")
	}
	w.proto3Varint(1, uint64(vector.GetDim()))
	arraySize := 0
	for _, row := range e.rows {
		if row >= len(values.GetData()) {
			return insertViewInternal("row offset %d exceeds ArrayOfVector length %d", row, len(values.GetData()))
		}
		itemSize := nullableProtoSize(values.GetData()[row])
		arraySize += protowire.SizeTag(2) + protowire.SizeBytes(itemSize)
	}
	if values.GetDim() != 0 {
		arraySize += protowire.SizeTag(1) + protowire.SizeVarint(uint64(values.GetDim()))
	}
	if values.GetElementType() != schemapb.DataType_None {
		arraySize += protowire.SizeTag(3) + protowire.SizeVarint(uint64(values.GetElementType()))
	}
	return w.message(8, arraySize, func() error {
		w.proto3Varint(1, uint64(values.GetDim()))
		for _, row := range e.rows {
			if err := w.protoMessage(2, values.GetData()[row], "ArrayOfVector row"); err != nil {
				return err
			}
		}
		w.proto3Varint(3, uint64(values.GetElementType()))
		return w.err
	})
}

// countSelectedVectorRows converts logical row offsets into physical compact
// vector indices. Nullable scalar columns and ArrayOfVector remain dense; this
// helper is used only for the regular vector oneofs whose null payload is
// compacted according to ValidData.
func (e *InsertRequestViewEncoder) countSelectedVectorRows(field *schemapb.FieldData, visit func(dataIndex int) error) (int, error) {
	valid := field.GetValidData()
	if len(valid) == 0 {
		for _, row := range e.rows {
			if err := visit(row); err != nil {
				return 0, err
			}
		}
		return len(e.rows), nil
	}

	dataIndex := 0
	scanRow := 0
	if len(e.rows) > 0 && len(e.firstFieldDataIndices) == len(e.source.GetFieldsData()) {
		fieldIndex := e.sourceFieldIndex(field)
		if fieldIndex < 0 {
			return 0, insertViewInternal("field %q (%d) is not part of the borrowed source request", field.GetFieldName(), field.GetFieldId())
		}
		dataIndex = int(e.firstFieldDataIndices[fieldIndex])
		scanRow = e.rows[0]
	}
	selected := 0
	for _, row := range e.rows {
		if row >= len(valid) {
			return 0, insertViewInternal("row offset %d exceeds ValidData length %d for field %q (%d)", row, len(valid), field.GetFieldName(), field.GetFieldId())
		}
		for scanRow < row {
			if valid[scanRow] {
				dataIndex++
			}
			scanRow++
		}
		if valid[row] {
			if err := visit(dataIndex); err != nil {
				return 0, err
			}
			selected++
			dataIndex++
		}
		scanRow = row + 1
	}
	return selected, nil
}

func (e *InsertRequestViewEncoder) sourceFieldIndex(field *schemapb.FieldData) int {
	for fieldIndex, candidate := range e.source.GetFieldsData() {
		if candidate == field {
			return fieldIndex
		}
	}
	return -1
}

func denseVectorStride(dim int64, unit int, label string) (int, error) {
	d, err := nonNegativeDim(dim, label)
	if err != nil {
		return 0, err
	}
	if unit == 8 {
		if d%8 != 0 {
			return 0, insertViewInternal("%s dimension %d is not divisible by 8", label, dim)
		}
		return d / 8, nil
	}
	return checkedProduct(d, unit, label+" row width")
}

func nonNegativeDim(dim int64, label string) (int, error) {
	if dim < 0 || uint64(dim) > uint64(math.MaxInt) {
		return 0, insertViewInternal("%s has invalid dimension %d", label, dim)
	}
	return int(dim), nil
}

func vectorBounds(dataIndex, stride, dataLength int, field *schemapb.FieldData, label string) (int, int, error) {
	if dataIndex < 0 {
		return 0, 0, insertViewInternal("negative compact %s index %d for field %q (%d)", label, dataIndex, field.GetFieldName(), field.GetFieldId())
	}
	if stride == 0 {
		return 0, 0, nil
	}
	if dataIndex > dataLength/stride {
		return 0, 0, insertViewInternal("compact %s index %d exceeds payload length %d for field %q (%d)", label, dataIndex, dataLength, field.GetFieldName(), field.GetFieldId())
	}
	start := dataIndex * stride
	if start > dataLength-stride {
		return 0, 0, insertViewInternal("compact %s row %d needs [%d:%d], payload length is %d for field %q (%d)", label, dataIndex, start, start+stride, dataLength, field.GetFieldName(), field.GetFieldId())
	}
	return start, start + stride, nil
}

func sparseRowDim(row []byte) (int64, error) {
	if len(row) == 0 {
		return 0, nil
	}
	if len(row)%8 != 0 {
		return 0, insertViewInternal("sparse row length %d is not a multiple of 8", len(row))
	}
	lastIndex := binary.LittleEndian.Uint32(row[len(row)-8:])
	return int64(lastIndex) + 1, nil
}

func checkedProduct(a, b int, label string) (int, error) {
	if a < 0 || b < 0 || (a != 0 && b > math.MaxInt/a) {
		return 0, insertViewInternal("%s exceeds addressable memory", label)
	}
	return a * b, nil
}

func nullableProtoSize(message proto.Message) int {
	if isNilProto(message) {
		return 0
	}
	return proto.Size(message)
}

func isNilProto(message proto.Message) bool {
	if message == nil {
		return true
	}
	value := reflect.ValueOf(message)
	return value.Kind() == reflect.Ptr && value.IsNil()
}

func insertViewInternal(format string, args ...any) error {
	return merr.WrapErrServiceInternalMsg("insert request view: "+format, args...)
}

type insertViewWriter struct {
	out     []byte
	n       int
	marshal bool
	err     error
}

func newInsertViewSizeWriter() *insertViewWriter {
	return &insertViewWriter{}
}

func newInsertViewMarshalWriter(dst []byte) *insertViewWriter {
	return &insertViewWriter{out: dst, marshal: true}
}

func (w *insertViewWriter) add(n int) {
	if w.err != nil {
		return
	}
	if n < 0 || w.n > math.MaxInt-n {
		w.err = insertViewInternal("protobuf size exceeds addressable memory")
		return
	}
	w.n += n
}

func (w *insertViewWriter) tag(number protowire.Number, typ protowire.Type) {
	if w.err != nil {
		return
	}
	w.add(protowire.SizeTag(number))
	if w.marshal {
		w.out = protowire.AppendTag(w.out, number, typ)
	}
}

func (w *insertViewWriter) varint(value uint64) {
	if w.err != nil {
		return
	}
	w.add(protowire.SizeVarint(value))
	if w.marshal {
		w.out = protowire.AppendVarint(w.out, value)
	}
}

func (w *insertViewWriter) fixed32(value uint32) {
	if w.err != nil {
		return
	}
	w.add(4)
	if w.marshal {
		w.out = protowire.AppendFixed32(w.out, value)
	}
}

func (w *insertViewWriter) fixed64(value uint64) {
	if w.err != nil {
		return
	}
	w.add(8)
	if w.marshal {
		w.out = protowire.AppendFixed64(w.out, value)
	}
}

func (w *insertViewWriter) raw(value []byte) {
	if w.err != nil {
		return
	}
	w.add(len(value))
	if w.marshal {
		w.out = append(w.out, value...)
	}
}

func (w *insertViewWriter) varintField(number protowire.Number, value uint64) {
	w.tag(number, protowire.VarintType)
	w.varint(value)
}

func (w *insertViewWriter) proto3Varint(number protowire.Number, value uint64) {
	if value != 0 {
		w.varintField(number, value)
	}
}

func (w *insertViewWriter) proto3String(number protowire.Number, value, label string) error {
	return w.string(number, value, false, label)
}

func (w *insertViewWriter) string(number protowire.Number, value string, force bool, label string) error {
	if !force && value == "" {
		return nil
	}
	if !utf8.ValidString(value) {
		return insertViewInternal("%s contains invalid UTF-8", label)
	}
	w.stringBytes(number, value)
	return w.err
}

func (w *insertViewWriter) stringBytes(number protowire.Number, value string) {
	w.tag(number, protowire.BytesType)
	w.varint(uint64(len(value)))
	if w.err != nil {
		return
	}
	w.add(len(value))
	if w.marshal {
		w.out = append(w.out, value...)
	}
}

func (w *insertViewWriter) bytes(number protowire.Number, value []byte) {
	w.tag(number, protowire.BytesType)
	w.varint(uint64(len(value)))
	w.raw(value)
}

func (w *insertViewWriter) message(number protowire.Number, size int, appendBody func() error) error {
	if size < 0 {
		return insertViewInternal("negative nested protobuf size %d", size)
	}
	w.tag(number, protowire.BytesType)
	w.varint(uint64(size))
	if w.err != nil {
		return w.err
	}
	if !w.marshal {
		w.add(size)
		return w.err
	}
	start := w.n
	if err := appendBody(); err != nil {
		return err
	}
	if w.err != nil {
		return w.err
	}
	if actual := w.n - start; actual != size {
		return insertViewInternal("nested protobuf size changed while borrowed data was in use: expected %d, wrote %d", size, actual)
	}
	return nil
}

func (w *insertViewWriter) protoMessage(number protowire.Number, message proto.Message, label string) error {
	size := nullableProtoSize(message)
	return w.message(number, size, func() error {
		if size == 0 || isNilProto(message) {
			return nil
		}
		startLen := len(w.out)
		out, err := (proto.MarshalOptions{}).MarshalAppend(w.out, message)
		if err != nil {
			return merr.WrapErrServiceInternalErr(err, "failed to marshal %s", label)
		}
		actual := len(out) - startLen
		w.out = out
		w.add(actual)
		if actual != size {
			return insertViewInternal("%s size changed while borrowed data was in use: expected %d, wrote %d", label, size, actual)
		}
		return w.err
	})
}
