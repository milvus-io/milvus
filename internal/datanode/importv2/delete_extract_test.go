package importv2

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
)

func TestExtractDeleteData_Int64PK(t *testing.T) {
	data := &storage.InsertData{Data: map[int64]storage.FieldData{
		100: &storage.Int64FieldData{Data: []int64{7, 8, 9}},
	}}
	pkField := &schemapb.FieldSchema{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}

	del, err := ExtractDeleteData(data, pkField, 4242)
	assert.NoError(t, err)
	assert.EqualValues(t, 3, del.RowCount)
	assert.Equal(t, int64(7), del.Pks[0].GetValue())
	assert.Equal(t, int64(9), del.Pks[2].GetValue())
	for _, ts := range del.Tss {
		assert.EqualValues(t, 4242, ts)
	}
}

func TestExtractDeleteData_VarCharPK(t *testing.T) {
	data := &storage.InsertData{Data: map[int64]storage.FieldData{
		100: &storage.StringFieldData{Data: []string{"a", "b"}},
	}}
	pkField := &schemapb.FieldSchema{FieldID: 100, Name: "pk", DataType: schemapb.DataType_VarChar, IsPrimaryKey: true}

	del, err := ExtractDeleteData(data, pkField, 9)
	assert.NoError(t, err)
	assert.EqualValues(t, 2, del.RowCount)
	assert.Equal(t, "a", del.Pks[0].GetValue())
}

func TestExtractDeleteData_MissingPKColumn(t *testing.T) {
	data := &storage.InsertData{Data: map[int64]storage.FieldData{}}
	pkField := &schemapb.FieldSchema{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}

	_, err := ExtractDeleteData(data, pkField, 1)
	assert.Error(t, err)
}

func TestExtractDeleteData_EmptyPKColumn(t *testing.T) {
	data := &storage.InsertData{Data: map[int64]storage.FieldData{
		100: &storage.Int64FieldData{Data: []int64{}},
	}}
	pkField := &schemapb.FieldSchema{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}

	del, err := ExtractDeleteData(data, pkField, 1)
	assert.NoError(t, err)
	assert.EqualValues(t, 0, del.RowCount)
}
