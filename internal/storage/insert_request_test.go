package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

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
