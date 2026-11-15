package fieldvalidator

import (
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func blobSchemaHelper(t *testing.T, nullable bool, maxLength string) *typeutil.SchemaHelper {
	field := &schemapb.FieldSchema{
		FieldID:  100,
		Name:     "blob",
		DataType: schemapb.DataType_Blob,
		Nullable: nullable,
	}
	if maxLength != "" {
		field.TypeParams = []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: maxLength}}
	}
	h, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}})
	require.NoError(t, err)
	return h
}

func blobData(validData []bool, values ...[]byte) *schemapb.FieldData {
	return &schemapb.FieldData{
		FieldId:   100,
		FieldName: "blob",
		Type:      schemapb.DataType_Blob,
		ValidData: validData,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BytesData{BytesData: &schemapb.BytesArray{Data: values}},
			},
		},
	}
}

func Test_ValidateUtil_checkBlobFieldData(t *testing.T) {
	paramtable.Init()
	v := NewValidateUtil(WithMaxLenCheck())

	t.Run("arbitrary bytes are kept exactly", func(t *testing.T) {
		allBytes := make([]byte, 256)
		for i := range allBytes {
			allBytes[i] = byte(i)
		}
		invalidUTF8 := []byte{0xc3, 0x28, 0xff, 0xfe}
		require.False(t, utf8.Valid(invalidUTF8))
		values := [][]byte{allBytes, {'a', 0x00, 'b', 0x00}, invalidUTF8}

		// Encode and decode the column as the client and proxy would, so the
		// test also covers the protobuf bytes representation.
		wire, err := proto.Marshal(blobData(nil, values...))
		require.NoError(t, err)
		field := &schemapb.FieldData{}
		require.NoError(t, proto.Unmarshal(wire, field))

		err = v.Validate([]*schemapb.FieldData{field}, blobSchemaHelper(t, false, "256"), 3)
		require.NoError(t, err)
		assert.Equal(t, values, field.GetScalars().GetBytesData().GetData())
	})

	t.Run("empty bytes and NULL stay distinct", func(t *testing.T) {
		// Compact form: bytes_data has entries only for the valid rows.
		field := blobData([]bool{true, false, true}, []byte{}, []byte{0x01})

		wire, err := proto.Marshal(field)
		require.NoError(t, err)
		field = &schemapb.FieldData{}
		require.NoError(t, proto.Unmarshal(wire, field))

		err = v.Validate([]*schemapb.FieldData{field}, blobSchemaHelper(t, true, "8"), 3)
		require.NoError(t, err)

		data := field.GetScalars().GetBytesData().GetData()
		require.Len(t, data, 3)
		assert.Equal(t, []bool{true, false, true}, typeutil.GetFieldDataValidData(field))
		assert.Len(t, data[0], 0)
		assert.Len(t, data[1], 0)
		assert.Equal(t, []byte{0x01}, data[2])
	})

	t.Run("all rows NULL", func(t *testing.T) {
		field := blobData([]bool{false, false})
		err := v.Validate([]*schemapb.FieldData{field}, blobSchemaHelper(t, true, "8"), 2)
		require.NoError(t, err)
		assert.Len(t, field.GetScalars().GetBytesData().GetData(), 2)
	})

	t.Run("NULL in a field that is not nullable", func(t *testing.T) {
		field := blobData([]bool{true, false}, []byte{0x01})
		err := v.Validate([]*schemapb.FieldData{field}, blobSchemaHelper(t, false, "8"), 2)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("value at the limit and one byte over", func(t *testing.T) {
		h := blobSchemaHelper(t, false, "4")
		err := v.Validate([]*schemapb.FieldData{blobData(nil, []byte{0, 1, 2, 3})}, h, 1)
		assert.NoError(t, err)

		err = v.Validate([]*schemapb.FieldData{blobData(nil, []byte{0, 1, 2, 3, 4})}, h, 1)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
		assert.ErrorContains(t, err, "exceeds max length")
	})

	t.Run("length is not checked without max length check", func(t *testing.T) {
		err := NewValidateUtil().Validate([]*schemapb.FieldData{blobData(nil, []byte{0, 1, 2, 3, 4})},
			blobSchemaHelper(t, false, "4"), 1)
		assert.NoError(t, err)
	})

	t.Run("max_length missing from schema", func(t *testing.T) {
		err := v.Validate([]*schemapb.FieldData{blobData(nil, []byte{0x01})}, blobSchemaHelper(t, false, ""), 1)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("row count mismatch", func(t *testing.T) {
		err := v.Validate([]*schemapb.FieldData{blobData(nil, []byte{0x01}, []byte{0x02})},
			blobSchemaHelper(t, false, "8"), 3)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("nullable row count mismatch", func(t *testing.T) {
		// Two valid rows but only one value in the compact bytes_data.
		field := blobData([]bool{true, true}, []byte{0x01})
		err := v.Validate([]*schemapb.FieldData{field}, blobSchemaHelper(t, true, "8"), 2)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})

	t.Run("missing bytes_data", func(t *testing.T) {
		field := &schemapb.FieldData{FieldId: 100, FieldName: "blob", Type: schemapb.DataType_Blob}
		err := v.Validate([]*schemapb.FieldData{field}, blobSchemaHelper(t, false, "8"), 1)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})
}
