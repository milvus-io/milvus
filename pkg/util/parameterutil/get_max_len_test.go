package parameterutil

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

func TestGetMaxLength(t *testing.T) {
	t.Run("not string type", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_Bool,
		}
		_, err := GetMaxLength(f)
		assert.Error(t, err)
	})

	t.Run("max length not found", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_VarChar,
		}
		_, err := GetMaxLength(f)
		assert.Error(t, err)
	})

	t.Run("max length not int", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxLengthKey,
					Value: "not_int_aha",
				},
			},
		}
		_, err := GetMaxLength(f)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_VarChar,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxLengthKey,
					Value: "100",
				},
			},
		}
		maxLength, err := GetMaxLength(f)
		assert.NoError(t, err)
		assert.Equal(t, int64(100), maxLength)
	})

	t.Run("nested varchar array", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType:   schemapb.DataType_Array,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "100"}},
			ElementSchema: &schemapb.TypeSchema{
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_VarChar,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.MaxCapacityKey, Value: "10"},
					{Key: common.MaxLengthKey, Value: "32"},
				},
			},
		}
		maxLength, err := GetMaxLength(f)
		assert.NoError(t, err)
		assert.Equal(t, int64(32), maxLength)
	})
}

func TestGetMaxCapacity(t *testing.T) {
	t.Run("not array type", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType: schemapb.DataType_Bool,
		}
		_, err := GetMaxCapacity(f)
		assert.Error(t, err)
	})

	t.Run("max capacity not found", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Double,
		}
		_, err := GetMaxCapacity(f)
		assert.Error(t, err)
	})

	t.Run("max capacity not int", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int64,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxCapacityKey,
					Value: "not_int_aha",
				},
			},
		}
		_, err := GetMaxCapacity(f)
		assert.Error(t, err)
	})

	t.Run("normal case", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType:    schemapb.DataType_Array,
			ElementType: schemapb.DataType_Int64,
			TypeParams: []*commonpb.KeyValuePair{
				{
					Key:   common.MaxCapacityKey,
					Value: "100",
				},
			},
		}
		maxCap, err := GetMaxCapacity(f)
		assert.NoError(t, err)
		assert.Equal(t, int64(100), maxCap)
	})

	t.Run("nested array capacity by level", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType:   schemapb.DataType_Array,
			TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "100"}},
			ElementSchema: &schemapb.TypeSchema{
				DataType:   schemapb.DataType_Array,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "10"}},
				ElementSchema: &schemapb.TypeSchema{
					DataType:    schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int64,
					TypeParams:  []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "5"}},
				},
			},
		}
		outerCap, err := GetMaxCapacityByLevel(f, 0)
		assert.NoError(t, err)
		assert.Equal(t, int64(100), outerCap)

		innerCap, err := GetMaxCapacityByLevel(f, 1)
		assert.NoError(t, err)
		assert.Equal(t, int64(10), innerCap)

		deeperCap, err := GetMaxCapacityByLevel(f, 2)
		assert.NoError(t, err)
		assert.Equal(t, int64(5), deeperCap)

		_, err = GetMaxCapacityByLevel(f, 3)
		assert.Error(t, err)
	})

	t.Run("array of vector has no nested capacity levels", func(t *testing.T) {
		f := &schemapb.FieldSchema{
			DataType:    schemapb.DataType_ArrayOfVector,
			ElementType: schemapb.DataType_FloatVector,
			TypeParams:  []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: "100"}},
		}
		outerCap, err := GetMaxCapacityByLevel(f, 0)
		assert.NoError(t, err)
		assert.Equal(t, int64(100), outerCap)

		_, err = GetMaxCapacityByLevel(f, 1)
		assert.Error(t, err)
	})
}
