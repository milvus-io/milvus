package primarykey

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

func requireUnrecoverable(t *testing.T, err error, text string) {
	t.Helper()
	require.Error(t, err)
	streamingErr := status.AsStreamingError(err)
	require.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_UNRECOVERABLE, streamingErr.Code)
	require.Contains(t, err.Error(), text)
}

func insertBody(fields ...*schemapb.FieldData) *msgpb.InsertRequest {
	return &msgpb.InsertRequest{FieldsData: fields}
}

func int64Field(values ...int64) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:    schemapb.DataType_Int64,
		FieldId: 100,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: values}},
			},
		},
	}
}

func varcharField(values ...string) *schemapb.FieldData {
	return &schemapb.FieldData{
		Type:    schemapb.DataType_VarChar,
		FieldId: 100,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: values}},
			},
		},
	}
}

func TestKeysOfInsert(t *testing.T) {
	keys, err := KeysOfInsert(insertBody(int64Field(10, 20)), 100, schemapb.DataType_Int64)
	require.NoError(t, err)
	require.Equal(t, Keys{Kind: KindInt64, Int64Values: []int64{10, 20}}, keys)

	keys, err = KeysOfInsert(insertBody(varcharField("a", "b")), 100, schemapb.DataType_VarChar)
	require.NoError(t, err)
	require.Equal(t, Keys{Kind: KindString, StringValues: []string{"a", "b"}}, keys)

	// the declared type of the field data is not checked, only the payload is.
	undeclared := int64Field(7)
	undeclared.Type = schemapb.DataType_None
	keys, err = KeysOfInsert(insertBody(undeclared), 100, schemapb.DataType_Int64)
	require.NoError(t, err)
	require.Equal(t, []int64{7}, keys.Int64Values)

	_, err = KeysOfInsert(insertBody(int64Field(7)), 100, schemapb.DataType_VarChar)
	requireUnrecoverable(t, err, "primary key payload does not match schema type VarChar")

	_, err = KeysOfInsert(insertBody(int64Field(7)), 100, schemapb.DataType_Float)
	requireUnrecoverable(t, err, "primary key has unsupported data type Float")

	_, err = KeysOfInsert(nil, 100, schemapb.DataType_Int64)
	requireUnrecoverable(t, err, "insert primary key field 100 is missing")
}

func TestKeysOfInsertDeclared(t *testing.T) {
	keys, err := KeysOfInsertDeclared(insertBody(int64Field(7)), 100, schemapb.DataType_Int64)
	require.NoError(t, err)
	require.Equal(t, Keys{Kind: KindInt64, Int64Values: []int64{7}}, keys)

	_, err = KeysOfInsertDeclared(insertBody(int64Field(7)), 100, schemapb.DataType_VarChar)
	requireUnrecoverable(t, err, "primary key field type Int64 does not match schema type VarChar")

	undeclared := int64Field(7)
	undeclared.Type = schemapb.DataType_None
	_, err = KeysOfInsertDeclared(insertBody(undeclared), 100, schemapb.DataType_Int64)
	requireUnrecoverable(t, err, "primary key field type None does not match schema type Int64")

	mislabeled := int64Field(7)
	mislabeled.Type = schemapb.DataType_VarChar
	_, err = KeysOfInsertDeclared(insertBody(mislabeled), 100, schemapb.DataType_VarChar)
	requireUnrecoverable(t, err, "primary key payload does not match schema type VarChar")

	_, err = KeysOfInsertDeclared(insertBody(), 100, schemapb.DataType_Int64)
	requireUnrecoverable(t, err, "insert primary key field 100 is missing")
}

func TestKeysOfInsertField(t *testing.T) {
	keys, err := KeysOfInsertField(insertBody(varcharField("a")), 100)
	require.NoError(t, err)
	require.Equal(t, Keys{Kind: KindString, StringValues: []string{"a"}}, keys)

	_, err = KeysOfInsertField(insertBody(), 100)
	requireUnrecoverable(t, err, "insert primary key field 100 is missing")

	_, err = KeysOfInsertField(insertBody(nil, int64Field(1), int64Field(2)), 100)
	requireUnrecoverable(t, err, "insert primary key field 100 is duplicated")

	vector := int64Field(1)
	vector.Field = &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{}}
	_, err = KeysOfInsertField(insertBody(vector), 100)
	requireUnrecoverable(t, err, "insert primary key field 100 must be int64 or varchar")

	_, err = KeysOfInsertField(insertBody(int64Field()), 100)
	requireUnrecoverable(t, err, "primary keys are empty")
}

func TestKeysOfDelete(t *testing.T) {
	// the values are aliased, not copied.
	data := []int64{1, 2}
	keys, err := KeysOfDelete(&msgpb.DeleteRequest{
		PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: data}}},
	})
	require.NoError(t, err)
	require.Equal(t, KindInt64, keys.Kind)
	require.Same(t, &data[0], &keys.Int64Values[0])

	keys, err = KeysOfDelete(&msgpb.DeleteRequest{
		PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{"a"}}}},
	})
	require.NoError(t, err)
	require.Equal(t, Keys{Kind: KindString, StringValues: []string{"a"}}, keys)

	for _, c := range []struct {
		ids  *schemapb.IDs
		text string
	}{
		{nil, "primary keys are nil"},
		{&schemapb.IDs{}, "unsupported primary key ids type <nil>"},
		{&schemapb.IDs{IdField: &schemapb.IDs_IntId{}}, "int64 primary keys are nil"},
		{&schemapb.IDs{IdField: &schemapb.IDs_StrId{}}, "varchar primary keys are nil"},
		{&schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{}}}, "primary keys are empty"},
		{&schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{}}}, "primary keys are empty"},
	} {
		_, err := KeysOfDelete(&msgpb.DeleteRequest{PrimaryKeys: c.ids})
		requireUnrecoverable(t, err, c.text)
	}

	_, err = KeysOfDelete(nil)
	requireUnrecoverable(t, err, "primary keys are nil")
}
