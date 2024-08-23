package csv

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
)

func TestNewRowParser_Invalid(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      1,
				Name:         "id",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:    2,
				Name:       "vector",
				DataType:   schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
			},
			{
				FieldID:  3,
				Name:     "str",
				DataType: schemapb.DataType_VarChar,
			},
			{
				FieldID:   4,
				Name:      "$meta",
				IsDynamic: true,
				DataType:  schemapb.DataType_JSON,
			},
		},
	}

	type testCase struct {
		header    []string
		expectErr string
	}

	cases := []testCase{
		{header: []string{"id", "vector", "$meta"}, expectErr: "value of field is missed: 'str'"},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("test_%d", i), func(t *testing.T) {
			_, err := NewRowParser(schema, c.header)
			assert.Error(t, err)
			assert.True(t, strings.Contains(err.Error(), c.expectErr))
		})
	}
}

func TestRowParser_Parse_Valid(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      1,
				Name:         "id",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:    2,
				Name:       "vector",
				DataType:   schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
			},
			{
				FieldID:   3,
				Name:      "$meta",
				IsDynamic: true,
				DataType:  schemapb.DataType_JSON,
			},
		},
	}

	type testCase struct {
		header   []string
		row      []string
		dyFields map[string]any // expect dynamic fields
	}

	cases := []testCase{
		{header: []string{"id", "vector", "$meta"}, row: []string{"1", "[1, 2]", "{\"y\": 2}"}, dyFields: map[string]any{"y": 2.0}},
		{header: []string{"id", "vector", "x", "$meta"}, row: []string{"1", "[1, 2]", "8", "{\"y\": 2}"}, dyFields: map[string]any{"x": "8", "y": 2.0}},
		{header: []string{"id", "vector", "x", "$meta"}, row: []string{"1", "[1, 2]", "8", "{}"}, dyFields: map[string]any{"x": "8"}},
		{header: []string{"id", "vector", "x"}, row: []string{"1", "[1, 2]", "8"}, dyFields: map[string]any{"x": "8"}},
		{header: []string{"id", "vector", "str", "$meta"}, row: []string{"1", "[1, 2]", "xxsddsffwq", "{\"y\": 2}"}, dyFields: map[string]any{"y": 2.0, "str": "xxsddsffwq"}},
	}

	for i, c := range cases {
		r, err := NewRowParser(schema, c.header)
		assert.NoError(t, err)
		t.Run(fmt.Sprintf("test_%d", i), func(t *testing.T) {
			data, err := r.Parse(c.row)
			assert.NoError(t, err)

			// validate contains fields
			for _, field := range schema.GetFields() {
				_, ok := data[field.GetFieldID()]
				assert.True(t, ok)
			}

			// validate dynamic fields
			var dynamicFields map[string]interface{}
			err = json.Unmarshal(data[r.(*rowParser).dynamicField.GetFieldID()].([]byte), &dynamicFields)
			assert.NoError(t, err)
			assert.Len(t, dynamicFields, len(c.dyFields))
			for k, v := range c.dyFields {
				rv, ok := dynamicFields[k]
				assert.True(t, ok)
				assert.EqualValues(t, rv, v)
			}
		})
	}
}

func TestRowParser_Parse_Invalid(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      1,
				Name:         "id",
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
			},
			{
				FieldID:    2,
				Name:       "vector",
				DataType:   schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
			},
			{
				FieldID:   3,
				Name:      "$meta",
				IsDynamic: true,
				DataType:  schemapb.DataType_JSON,
			},
		},
	}

	type testCase struct {
		header    []string
		row       []string
		expectErr string
	}

	cases := []testCase{
		{header: []string{"id", "vector", "x", "$meta"}, row: []string{"1", "[1, 2]", "6", "{\"x\": 8}"}, expectErr: "duplicated key in dynamic field, key=x"},
		{header: []string{"id", "vector", "x", "$meta"}, row: []string{"1", "[1, 2]", "8", "{*&%%&$*(&}"}, expectErr: "illegal value for dynamic field, not a JSON format string"},
		{header: []string{"id", "vector", "x", "$meta"}, row: []string{"1", "[1, 2]", "8"}, expectErr: "the number of fields in the row is not equal to the header"},
	}

	for i, c := range cases {
		r, err := NewRowParser(schema, c.header)
		assert.NoError(t, err)
		t.Run(fmt.Sprintf("test_%d", i), func(t *testing.T) {
			_, err := r.Parse(c.row)
			assert.Error(t, err)
			assert.True(t, strings.Contains(err.Error(), c.expectErr))
		})
	}
}
