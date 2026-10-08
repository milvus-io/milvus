package storage

import (
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// nativeArrayLeaf returns the leaf type and leaf nullability of a native-list
// scalar Array column and whether it has two list levels. Storage writes at
// most two levels. The schema was validated at DDL time, so a chain that does
// not resolve is a Milvus bug and is relabeled as an internal error.
func nativeArrayLeaf(field *schemapb.FieldSchema) (schemapb.DataType, bool, bool, error) {
	leaf, leafNullable, depth, err := typeutil.GetFieldArrayLeaf(field)
	if err != nil {
		return schemapb.DataType_None, false, false, merr.WrapErrServiceInternalErr(err, "resolve Array leaf of field %s", field.GetName())
	}
	if depth > 2 {
		return schemapb.DataType_None, false, false, merr.WrapErrServiceInternalMsg(
			"nested Array field %s supports exactly two list levels, got %d", field.GetName(), depth)
	}
	return leaf, leafNullable, depth == 2, nil
}

func nativeArrayLeafArrowType(dt schemapb.DataType) (arrow.DataType, error) {
	switch dt {
	case schemapb.DataType_Bool:
		return arrow.FixedWidthTypes.Boolean, nil
	case schemapb.DataType_Int8:
		return arrow.PrimitiveTypes.Int8, nil
	case schemapb.DataType_Int16:
		return arrow.PrimitiveTypes.Int16, nil
	case schemapb.DataType_Int32:
		return arrow.PrimitiveTypes.Int32, nil
	case schemapb.DataType_Int64:
		return arrow.PrimitiveTypes.Int64, nil
	case schemapb.DataType_Float:
		return arrow.PrimitiveTypes.Float32, nil
	case schemapb.DataType_Double:
		return arrow.PrimitiveTypes.Float64, nil
	case schemapb.DataType_VarChar:
		return arrow.BinaryTypes.String, nil
	default:
		return nil, merr.WrapErrServiceInternalMsg("unsupported native Array leaf type %s", dt)
	}
}

// ArrowTypeForField returns the physical Arrow type selected by the complete field schema.
func ArrowTypeForField(field *schemapb.FieldSchema) (arrow.DataType, error) {
	if field == nil {
		return nil, merr.WrapErrServiceInternalMsg("nil field schema")
	}
	entry, ok := serdeMap[field.GetDataType()]
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("unsupported Arrow field type %s", field.GetDataType())
	}
	return entry.arrowType(field)
}

// arrowType selects the physical type with all schema levels available.
func (entry serdeEntry) arrowType(field *schemapb.FieldSchema) (arrow.DataType, error) {
	if field.GetDataType() == schemapb.DataType_Array && typeutil.IsNativeListArrayField(field) {
		leaf, nullable, nested, err := nativeArrayLeaf(field)
		if err != nil {
			return nil, err
		}
		if nested && (field.GetTypeSchema().GetNullable() != field.GetNullable() ||
			field.GetTypeSchema().GetArrayElement().GetNullable() != field.GetElementNullable()) {
			return nil, merr.WrapErrServiceInternalMsg("nested Array TypeSchema nullable flags differ from FieldSchema")
		}
		leafType, err := nativeArrayLeafArrowType(leaf)
		if err != nil {
			return nil, err
		}
		child := arrow.ListOfField(arrow.Field{Name: "item", Type: leafType, Nullable: nullable})
		if nested {
			return arrow.ListOfField(arrow.Field{Name: "item", Type: child, Nullable: field.GetElementNullable()}), nil
		}
		return child, nil
	}
	if entry.legacyArrowType == nil {
		return nil, merr.WrapErrServiceInternalMsg("unsupported Arrow field type %s", field.GetDataType())
	}
	dim := 0
	if typeutil.IsVectorType(field.GetDataType()) && !typeutil.IsSparseFloatVectorType(field.GetDataType()) {
		value, err := typeutil.GetDim(field)
		if err != nil {
			return nil, merr.WrapErrAsSysError(merr.Wrapf(err, "get dimension for field %s", field.GetName()))
		}
		dim = int(value)
	}
	return entry.legacyArrowType(dim, field.GetElementType(), field.GetElementNullable()), nil
}

func nativeArrayLength(row *schemapb.ScalarField, leaf schemapb.DataType) (int, error) {
	if row == nil {
		return 0, merr.WrapErrDataIntegrityMsg("nil native Array element")
	}
	switch leaf {
	case schemapb.DataType_Bool:
		if row.GetBoolData() == nil {
			break
		}
		return len(row.GetBoolData().GetData()), nil
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		if row.GetIntData() == nil {
			break
		}
		return len(row.GetIntData().GetData()), nil
	case schemapb.DataType_Int64:
		if row.GetLongData() == nil {
			break
		}
		return len(row.GetLongData().GetData()), nil
	case schemapb.DataType_Float:
		if row.GetFloatData() == nil {
			break
		}
		return len(row.GetFloatData().GetData()), nil
	case schemapb.DataType_Double:
		if row.GetDoubleData() == nil {
			break
		}
		return len(row.GetDoubleData().GetData()), nil
	case schemapb.DataType_VarChar:
		if row.GetStringData() == nil {
			break
		}
		return len(row.GetStringData().GetData()), nil
	}
	return 0, merr.WrapErrDataIntegrityMsg("native Array payload does not match leaf type %s", leaf)
}

func appendNativeLeaf(b array.Builder, row *schemapb.ScalarField, leaf schemapb.DataType, nullable bool) error {
	n, err := nativeArrayLength(row, leaf)
	if err != nil {
		return err
	}
	valid := row.GetValidData()
	if nullable && len(valid) != n || !nullable && len(valid) != 0 {
		return merr.WrapErrDataIntegrityMsg("native Array validity length %d does not match payload length %d and nullable=%t", len(valid), n, nullable)
	}
	for i := 0; i < n; i++ {
		if nullable && !valid[i] {
			b.AppendNull()
			continue
		}
		switch leaf {
		case schemapb.DataType_Bool:
			b.(*array.BooleanBuilder).Append(row.GetBoolData().GetData()[i])
		case schemapb.DataType_Int8:
			b.(*array.Int8Builder).Append(int8(row.GetIntData().GetData()[i]))
		case schemapb.DataType_Int16:
			b.(*array.Int16Builder).Append(int16(row.GetIntData().GetData()[i]))
		case schemapb.DataType_Int32:
			b.(*array.Int32Builder).Append(row.GetIntData().GetData()[i])
		case schemapb.DataType_Int64:
			b.(*array.Int64Builder).Append(row.GetLongData().GetData()[i])
		case schemapb.DataType_Float:
			b.(*array.Float32Builder).Append(row.GetFloatData().GetData()[i])
		case schemapb.DataType_Double:
			b.(*array.Float64Builder).Append(row.GetDoubleData().GetData()[i])
		case schemapb.DataType_VarChar:
			b.(*array.StringBuilder).Append(row.GetStringData().GetData()[i])
		}
	}
	return nil
}

// SerializeNativeArrayRow writes a dense ScalarField row as a native Arrow list.
func SerializeNativeArrayRow(b array.Builder, value any, field *schemapb.FieldSchema) error {
	list, ok := b.(*array.ListBuilder)
	if !ok {
		return merr.WrapErrServiceInternalMsg("expected ListBuilder, got %T", b)
	}
	if value == nil {
		list.AppendNull()
		return nil
	}
	row, ok := value.(*schemapb.ScalarField)
	if !ok {
		return merr.WrapErrDataIntegrityMsg("expected ScalarField, got %T", value)
	}
	if row == nil {
		list.AppendNull()
		return nil
	}
	leaf, leafNullable, nested, err := nativeArrayLeaf(field)
	if err != nil {
		return err
	}
	if !nested {
		if _, err := nativeArrayLength(row, leaf); err != nil {
			return err
		}
		list.Append(true)
		return appendNativeLeaf(list.ValueBuilder(), row, leaf, leafNullable)
	}
	outer := row.GetArrayData()
	if outer == nil || outer.GetElementType() != leaf {
		return merr.WrapErrDataIntegrityMsg("nested Array payload missing or leaf type differs from %s", leaf)
	}
	children := outer.GetData()
	valid := row.GetValidData()
	if field.GetElementNullable() && len(valid) != len(children) || !field.GetElementNullable() && len(valid) != 0 {
		return merr.WrapErrDataIntegrityMsg("nested Array validity length %d does not match child count %d", len(valid), len(children))
	}
	list.Append(true)
	inner := list.ValueBuilder().(*array.ListBuilder)
	for i, child := range children {
		if field.GetElementNullable() && !valid[i] {
			if n, err := nativeArrayLength(child, leaf); err != nil || n != 0 || len(child.GetValidData()) != 0 {
				return merr.WrapErrDataIntegrityMsg("null nested Array child %d must have empty typed payload", i)
			}
			inner.AppendNull()
			continue
		}
		if _, err := nativeArrayLength(child, leaf); err != nil {
			return err
		}
		inner.Append(true)
		if err := appendNativeLeaf(inner.ValueBuilder(), child, leaf, leafNullable); err != nil {
			return err
		}
	}
	return nil
}

func emptyNativeScalar(leaf schemapb.DataType) *schemapb.ScalarField {
	row := &schemapb.ScalarField{}
	switch leaf {
	case schemapb.DataType_Bool:
		row.Data = &schemapb.ScalarField_BoolData{BoolData: &schemapb.BoolArray{}}
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		row.Data = &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{}}
	case schemapb.DataType_Int64:
		row.Data = &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{}}
	case schemapb.DataType_Float:
		row.Data = &schemapb.ScalarField_FloatData{FloatData: &schemapb.FloatArray{}}
	case schemapb.DataType_Double:
		row.Data = &schemapb.ScalarField_DoubleData{DoubleData: &schemapb.DoubleArray{}}
	case schemapb.DataType_VarChar:
		row.Data = &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{}}
	}
	return row
}

func readNativeLeaf(values arrow.Array, start, end int64, leaf schemapb.DataType, nullable bool) (*schemapb.ScalarField, error) {
	leafType, err := nativeArrayLeafArrowType(leaf)
	if err != nil {
		return nil, err
	}
	if !arrow.TypeEqual(values.DataType(), leafType) {
		return nil, merr.WrapErrDataIntegrityMsg("native Array leaf Arrow type %s does not match %s", values.DataType(), leafType)
	}
	if start < 0 || end < start || end > int64(values.Len()) {
		return nil, merr.WrapErrDataIntegrityMsg("native Array leaf span [%d,%d) exceeds length %d", start, end, values.Len())
	}
	row := emptyNativeScalar(leaf)
	for i := start; i < end; i++ {
		idx := int(i)
		valid := values.IsValid(idx)
		if !nullable && !valid {
			return nil, merr.WrapErrDataIntegrityMsg("non-nullable Array leaf is null at %d", idx)
		}
		if nullable {
			row.ValidData = append(row.ValidData, valid)
		}
		switch leaf {
		case schemapb.DataType_Bool:
			row.GetBoolData().Data = append(row.GetBoolData().Data, valid && values.(*array.Boolean).Value(idx))
		case schemapb.DataType_Int8:
			if valid {
				row.GetIntData().Data = append(row.GetIntData().Data, int32(values.(*array.Int8).Value(idx)))
			} else {
				row.GetIntData().Data = append(row.GetIntData().Data, 0)
			}
		case schemapb.DataType_Int16:
			if valid {
				row.GetIntData().Data = append(row.GetIntData().Data, int32(values.(*array.Int16).Value(idx)))
			} else {
				row.GetIntData().Data = append(row.GetIntData().Data, 0)
			}
		case schemapb.DataType_Int32:
			if valid {
				row.GetIntData().Data = append(row.GetIntData().Data, values.(*array.Int32).Value(idx))
			} else {
				row.GetIntData().Data = append(row.GetIntData().Data, 0)
			}
		case schemapb.DataType_Int64:
			if valid {
				row.GetLongData().Data = append(row.GetLongData().Data, values.(*array.Int64).Value(idx))
			} else {
				row.GetLongData().Data = append(row.GetLongData().Data, 0)
			}
		case schemapb.DataType_Float:
			if valid {
				row.GetFloatData().Data = append(row.GetFloatData().Data, values.(*array.Float32).Value(idx))
			} else {
				row.GetFloatData().Data = append(row.GetFloatData().Data, 0)
			}
		case schemapb.DataType_Double:
			if valid {
				row.GetDoubleData().Data = append(row.GetDoubleData().Data, values.(*array.Float64).Value(idx))
			} else {
				row.GetDoubleData().Data = append(row.GetDoubleData().Data, 0)
			}
		case schemapb.DataType_VarChar:
			if valid {
				row.GetStringData().Data = append(row.GetStringData().Data, strings.Clone(values.(*array.String).Value(idx)))
			} else {
				row.GetStringData().Data = append(row.GetStringData().Data, "")
			}
		}
	}
	return row, nil
}

// DeserializeNativeArrayRow rebuilds the dense proto form of one Arrow list row.
func DeserializeNativeArrayRow(a arrow.Array, i int, field *schemapb.FieldSchema) (*schemapb.ScalarField, error) {
	list, ok := a.(*array.List)
	if !ok {
		return nil, merr.WrapErrDataIntegrityMsg("expected Array List, got %T", a)
	}
	if i < 0 || i >= list.Len() {
		return nil, merr.WrapErrDataIntegrityMsg("Array row %d out of bounds", i)
	}
	if list.IsNull(i) {
		start, end := list.ValueOffsets(i)
		if start != end {
			return nil, merr.WrapErrDataIntegrityMsg("null Array row %d owns %d child values", i, end-start)
		}
		return nil, nil
	}
	leaf, leafNullable, nested, err := nativeArrayLeaf(field)
	if err != nil {
		return nil, err
	}
	start, end := list.ValueOffsets(i)
	if !nested {
		return readNativeLeaf(list.ListValues(), start, end, leaf, leafNullable)
	}
	inner, ok := list.ListValues().(*array.List)
	if !ok {
		return nil, merr.WrapErrDataIntegrityMsg("expected nested Array List, got %T", list.ListValues())
	}
	row := &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: leaf}}}
	for j := start; j < end; j++ {
		valid := inner.IsValid(int(j))
		if !field.GetElementNullable() && !valid {
			return nil, merr.WrapErrDataIntegrityMsg("non-nullable nested Array child is null at %d", j)
		}
		if field.GetElementNullable() {
			row.ValidData = append(row.ValidData, valid)
		}
		if !valid {
			childStart, childEnd := inner.ValueOffsets(int(j))
			if childStart != childEnd {
				return nil, merr.WrapErrDataIntegrityMsg("null nested Array child %d owns %d leaf values", j, childEnd-childStart)
			}
			row.GetArrayData().Data = append(row.GetArrayData().Data, emptyNativeScalar(leaf))
			continue
		}
		childStart, childEnd := inner.ValueOffsets(int(j))
		child, err := readNativeLeaf(inner.ListValues(), childStart, childEnd, leaf, leafNullable)
		if err != nil {
			return nil, err
		}
		row.GetArrayData().Data = append(row.GetArrayData().Data, child)
	}
	return row, nil
}
