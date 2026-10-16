package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Enumerate descriptors instead of a hand-written fixture so newly added fields
// are exercised even when nobody updates this test alongside the proto upgrade.
// Each oneof alternative gets its own input; setting them together would only
// test the last alternative. These are all wrappers reconstructed field by field
// by CopyInsertRequestMetadata and CopyFieldDataMetadata.
func TestCopyInsertRequestMetadataSchemaCoverage(t *testing.T) {
	copyField := func(src *schemapb.FieldData) *schemapb.FieldData {
		return CopyInsertRequestMetadata(&msgpb.InsertRequest{FieldsData: []*schemapb.FieldData{src}}).FieldsData[0]
	}
	cases := []struct {
		message proto.Message
		copy    func(proto.Message) proto.Message
	}{
		{&msgpb.InsertRequest{}, func(src proto.Message) proto.Message {
			return CopyInsertRequestMetadata(src.(*msgpb.InsertRequest))
		}},
		{&schemapb.FieldData{}, func(src proto.Message) proto.Message {
			return copyField(src.(*schemapb.FieldData))
		}},
		{&schemapb.ScalarField{}, func(src proto.Message) proto.Message {
			return copyField(&schemapb.FieldData{Field: &schemapb.FieldData_Scalars{Scalars: src.(*schemapb.ScalarField)}}).GetScalars()
		}},
		{&schemapb.VectorField{}, func(src proto.Message) proto.Message {
			return copyField(&schemapb.FieldData{Field: &schemapb.FieldData_Vectors{Vectors: src.(*schemapb.VectorField)}}).GetVectors()
		}},
		{&schemapb.StructArrayField{}, func(src proto.Message) proto.Message {
			return copyField(&schemapb.FieldData{Field: &schemapb.FieldData_StructArrays{StructArrays: src.(*schemapb.StructArrayField)}}).GetStructArrays()
		}},
	}
	for _, tc := range cases {
		descriptor := tc.message.ProtoReflect().Descriptor()
		t.Run(string(descriptor.FullName()), func(t *testing.T) {
			fields := descriptor.Fields()
			for i := 0; i < fields.Len(); i++ {
				field := fields.Get(i)
				t.Run(string(field.Name()), func(t *testing.T) {
					src := tc.message.ProtoReflect().New()
					// No wrapper currently has a map. Fail explicitly on a future
					// map until its key/value fixture and ownership are reviewed.
					require.False(t, field.IsMap(), "add a non-empty map fixture for %s", field.FullName())
					if field.IsList() {
						list := src.Mutable(field).List()
						list.Append(metadataCopyFieldValue(t, field, list.NewElement()))
					} else {
						src.Set(field, metadataCopyFieldValue(t, field, src.NewField(field)))
					}
					require.True(t, src.Has(field), "fixture must populate %s", field.FullName())
					dst := tc.copy(src.Interface())
					require.True(t, proto.Equal(src.Interface(), dst),
						"copy lost or changed %s; review the metadata copy when updating protobufs", field.FullName())
				})
			}
		})
	}
}

// Message values need presence, not recursive population: their manually copied
// wrappers are tested separately above. Recursing would also loop on struct arrays.
func metadataCopyFieldValue(t *testing.T, field protoreflect.FieldDescriptor, fresh protoreflect.Value) protoreflect.Value {
	t.Helper()
	switch field.Kind() {
	case protoreflect.BoolKind:
		return protoreflect.ValueOfBool(true)
	case protoreflect.EnumKind:
		values := field.Enum().Values()
		for i := 0; i < values.Len(); i++ {
			if number := values.Get(i).Number(); number != 0 {
				return protoreflect.ValueOfEnum(number)
			}
		}
		t.Fatalf("add a non-default enum fixture for %s", field.FullName())
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return protoreflect.ValueOfInt32(7)
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return protoreflect.ValueOfInt64(7)
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return protoreflect.ValueOfUint32(7)
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return protoreflect.ValueOfUint64(7)
	case protoreflect.FloatKind:
		return protoreflect.ValueOfFloat32(1.5)
	case protoreflect.DoubleKind:
		return protoreflect.ValueOfFloat64(1.5)
	case protoreflect.StringKind:
		return protoreflect.ValueOfString("metadata-copy")
	case protoreflect.BytesKind:
		return protoreflect.ValueOfBytes([]byte{1, 2})
	case protoreflect.MessageKind:
		return fresh
	default:
		t.Fatalf("add a populated fixture for %s (%s)", field.FullName(), field.Kind())
	}
	return protoreflect.Value{}
}

func TestCopyInsertRequestMetadata(t *testing.T) {
	namespace := "namespace"
	scalar := &schemapb.FieldData{
		Type: schemapb.DataType_Int64, FieldId: 100, FieldName: "id", ValidData: []bool{true},
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{7}}}}},
	}
	vector := &schemapb.FieldData{
		Type: schemapb.DataType_FloatVector, FieldId: 101, FieldName: "vector", ValidData: []bool{true},
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}}}}},
	}
	array := &schemapb.FieldData{
		Type: schemapb.DataType_ArrayOfStruct, FieldId: 102,
		Field: &schemapb.FieldData_StructArrays{StructArrays: &schemapb.StructArrayField{Fields: []*schemapb.FieldData{scalar, vector}}},
	}
	src := &msgpb.InsertRequest{
		Base:      &commonpb.MsgBase{Timestamp: 1, Properties: map[string]string{"source": "WAL"}},
		ShardName: "v1", DbName: "db", CollectionName: "collection", PartitionName: "partition",
		DbID: 1, CollectionID: 2, PartitionID: 3, SegmentID: 4, Namespace: &namespace,
		Timestamps: []uint64{1}, RowIDs: []int64{7}, RowData: []*commonpb.Blob{{Value: []byte{1, 2}}},
		FieldsData: []*schemapb.FieldData{scalar, vector, array}, NumRows: 1, Version: msgpb.InsertDataVersion_ColumnBased,
	}
	for _, msg := range []proto.Message{src, scalar, scalar.GetScalars(), vector, vector.GetVectors(), array, array.GetStructArrays()} {
		msg.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	}
	before := proto.Clone(src)
	copy := CopyInsertRequestMetadata(src)
	require.True(t, proto.Equal(src, copy), "all protobuf metadata must survive the copy")
	for _, field := range copy.FieldsData {
		require.True(t, typeutil.ValidateAndNormalizeFieldDataValidData(field))
	}
	copy.Base.Timestamp = 9
	copy.Base.Properties["source"] = "query"
	copy.SegmentID = 10
	copy.Timestamps = []uint64{9}
	*copy.Namespace = "private"
	copy.ProtoReflect().GetUnknown()[2] = 2
	copy.FieldsData[1].GetVectors().Dim = 4
	copy.FieldsData[2].GetStructArrays().Fields[0].FieldName = "private"
	copy.FieldsData = append(copy.FieldsData, &schemapb.FieldData{FieldId: 103})
	require.True(t, proto.Equal(before, src), "normalization and execution metadata cannot modify the cached body")
	require.Same(t, scalar.GetScalars().GetLongData(), copy.FieldsData[0].GetScalars().GetLongData())
	require.Same(t, vector.GetVectors().GetFloatVector(), copy.FieldsData[1].GetVectors().GetFloatVector())
	require.Same(t, &src.RowIDs[0], &copy.RowIDs[0])
	require.Same(t, src.RowData[0], copy.RowData[0])
}

func TestCopyInsertRequestEmptyMetadata(t *testing.T) {
	for _, fields := range [][]*schemapb.FieldData{
		nil,
		{},
		{
			nil,
			{},
			{Field: &schemapb.FieldData_Scalars{}},
			{Field: &schemapb.FieldData_Vectors{}},
			{Field: &schemapb.FieldData_StructArrays{}},
		},
	} {
		src := &msgpb.InsertRequest{FieldsData: fields}
		require.True(t, proto.Equal(src, CopyInsertRequestMetadata(src)))
	}
}

func TestCopyInsertRequestPreservesInvalidValidity(t *testing.T) {
	src := &msgpb.InsertRequest{FieldsData: []*schemapb.FieldData{{
		ValidData: []bool{true},
		Field:     &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{ValidData: []bool{false}}},
	}}}
	copy := CopyInsertRequestMetadata(src)
	require.True(t, proto.Equal(src, copy))
	require.False(t, typeutil.ValidateAndNormalizeFieldDataValidData(copy.FieldsData[0]), "copying cannot silently resolve conflicting validity sources")
	require.True(t, proto.Equal(src, copy))
}
