package row

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/internal/rowutil"
)

type ValidStruct struct {
	ID      int64 `milvus:"primary_key"`
	Attr1   int8
	Attr2   int16
	Attr3   int32
	Attr4   float32
	Attr5   float64
	Attr6   string
	Attr7   bool
	Vector  []float32 `milvus:"dim:16"`
	Vector2 []byte    `milvus:"dim:32"`
}

type ValidStruct2 struct {
	ID      int64 `milvus:"primary_key"`
	Vector  [16]float32
	Attr1   float64
	Ignored bool `milvus:"-"`
}

type ValidStructWithNamedTag struct {
	ID     int64       `milvus:"primary_key;name:id"`
	Vector [16]float32 `milvus:"name:vector"`
}

type RowsSuite struct {
	suite.Suite
}

func (s *RowsSuite) TestColumnCreatorsRejectInvalidSchemas() {
	for _, dataType := range []entity.FieldType{
		entity.FieldTypeFloatVector, entity.FieldTypeBinaryVector,
		entity.FieldTypeFloat16Vector, entity.FieldTypeBFloat16Vector, entity.FieldTypeInt8Vector,
	} {
		for _, dim := range []string{"", "not-a-number"} {
			s.Run(fmt.Sprintf("%s/dim=%s", dataType, dim), func() {
				field := entity.NewField().WithName("vector").WithDataType(dataType)
				if dim != "" {
					field.TypeParams = map[string]string{entity.TypeParamDim: dim}
				}
				creator := getColumnCreators(entity.NewSchema().WithField(field))["vector"]
				col, err := creator(1)
				s.Nil(col)
				s.Error(err)
			})
		}
	}
	for _, field := range []*entity.Field{
		entity.NewField().WithName("array").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeJSON),
		entity.NewField().WithName("array").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeStruct),
	} {
		col, err := getColumnCreators(entity.NewSchema().WithField(field))["array"](1)
		s.Nil(col)
		s.Error(err)
	}
}

func (s *RowsSuite) TestColumnCreatorsStructAndSparseArray() {
	fields := []*entity.Field{
		entity.NewField().WithName("json").WithDataType(entity.FieldTypeJSON),
		entity.NewField().WithName("array").WithDataType(entity.FieldTypeArray).WithElementType(entity.FieldTypeInt64),
		entity.NewField().WithName("sparse").WithDataType(entity.FieldTypeSparseVector),
		entity.NewField().WithName("profile").WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeStruct).WithStructSchema(entity.NewStructSchema().WithField(
			entity.NewField().WithName("age").WithDataType(entity.FieldTypeInt64))),
	}
	for _, field := range fields {
		col, err := getColumnCreators(entity.NewSchema().WithField(field))[field.Name](2)
		s.Require().NoError(err)
		s.Equal(field.Name, col.Name())
		s.Zero(col.Len())
		s.Equal(field.DataType, col.Type())
	}
}

func (s *RowsSuite) TestRowsToColumns() {
	s.Run("valid_cases", func() {
		columns, err := AnyToColumns([]any{&ValidStruct{}}, false)
		s.Nil(err)
		s.Equal(10, len(columns))

		columns, err = AnyToColumns([]any{&ValidStruct2{}}, false)
		s.Nil(err)
		s.Equal(3, len(columns))
	})

	s.Run("auto_id_pk", func() {
		type AutoPK struct {
			ID     int64     `milvus:"primary_key;auto_id"`
			Vector []float32 `milvus:"dim:32"`
		}
		columns, err := AnyToColumns([]any{&AutoPK{}}, false)
		s.Nil(err)
		s.Require().Equal(1, len(columns))
		s.Equal("Vector", columns[0].Name())
	})

	s.Run("bf16", func() {
		type BF16Struct struct {
			ID     int64  `milvus:"primary_key;auto_id"`
			Vector []byte `milvus:"dim:16;vector_type:bf16"`
		}
		columns, err := AnyToColumns([]any{&BF16Struct{}}, false)
		s.Nil(err)
		s.Require().Equal(1, len(columns))
		s.Equal("Vector", columns[0].Name())
		s.Equal(entity.FieldTypeBFloat16Vector, columns[0].Type())
	})

	s.Run("fp16", func() {
		type FP16Struct struct {
			ID     int64  `milvus:"primary_key;auto_id"`
			Vector []byte `milvus:"dim:16;vector_type:fp16"`
		}
		columns, err := AnyToColumns([]any{&FP16Struct{}}, false)
		s.Nil(err)
		s.Require().Equal(1, len(columns))
		s.Equal("Vector", columns[0].Name())
		s.Equal(entity.FieldTypeFloat16Vector, columns[0].Type())
	})

	s.Run("int8", func() {
		type Int8Struct struct {
			ID     int64  `milvus:"primary_key;auto_id"`
			Vector []int8 `milvus:"dim:16;vector_type:int8"`
		}
		columns, err := AnyToColumns([]any{&Int8Struct{}}, false)
		s.Nil(err)
		s.Require().Equal(1, len(columns))
		s.Equal("Vector", columns[0].Name())
		s.Equal(entity.FieldTypeInt8Vector, columns[0].Type())
	})

	s.Run("invalid_cases", func() {
		// empty input
		_, err := AnyToColumns([]any{}, false)
		s.NotNil(err)

		// incompatible rows
		_, err = AnyToColumns([]any{&ValidStruct{}, &ValidStruct2{}}, false)
		s.NotNil(err)

		// schema & row not compatible
		_, err = AnyToColumns([]any{&ValidStruct{}}, false, &entity.Schema{
			Fields: []*entity.Field{
				{
					Name:     "Attr1",
					DataType: entity.FieldTypeInt64,
				},
			},
		})
		s.NotNil(err)
	})
}

func (s *RowsSuite) TestDynamicSchema() {
	s.Run("all_fallback_dynamic", func() {
		columns, err := AnyToColumns([]any{&ValidStruct{}}, false,
			entity.NewSchema().WithDynamicFieldEnabled(true),
		)
		s.NoError(err)
		s.Equal(1, len(columns))
	})

	s.Run("dynamic_not_found", func() {
		_, err := AnyToColumns([]any{&ValidStruct{}}, false,
			entity.NewSchema().WithField(
				entity.NewField().WithName("ID").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true),
			).WithDynamicFieldEnabled(true),
		)
		s.NoError(err)
	})
}

func (s *RowsSuite) TestParseFields() {
	type DynamicRows struct {
		Float float32 `json:"float" milvus:"name:float"`
	}

	cases := []struct {
		tag       string
		v         reflect.Value
		expect    map[string]any
		expectErr bool
	}{
		{
			tag: "MapRow",
			v: reflect.ValueOf(map[string]interface{}{
				"A": "abd", "B": int64(8),
			}),
			expect: map[string]any{
				"A": "abd",
				"B": int64(8),
			},
			expectErr: false,
		},
		{
			tag: "StructRow",
			v: reflect.ValueOf(struct {
				A string
				B int64
			}{A: "abc", B: 16}),
			expect: map[string]any{
				"A": "abc",
				"B": int64(16),
			},
			expectErr: false,
		},
		{
			tag: "StructRow_DuplicateName",
			v: reflect.ValueOf(struct {
				A string `milvus:"name:a"`
				B int64  `milvus:"name:a"`
			}{A: "abc", B: 16}),
			expectErr: true,
		},
		{
			tag: "StructRow_EmbedStruct",
			v: reflect.ValueOf(struct {
				A string `milvus:"name:a"`
				DynamicRows
			}{A: "emb", DynamicRows: DynamicRows{Float: 0.1}}),
			expect: map[string]any{
				"a":     "emb",
				"float": float32(0.1),
			},
			expectErr: false,
		},
		{
			tag: "StructRow_EmbedDuplicateName",
			v: reflect.ValueOf(struct {
				Int64    int64     `json:"int64" milvus:"name:int64"`
				Float    float32   `json:"float" milvus:"name:float"`
				FloatVec []float32 `json:"floatVec" milvus:"name:floatVec"`
				DynamicRows
			}{}),
			expectErr: true,
		},
		{
			tag:       "Unsupported_primitive",
			v:         reflect.ValueOf(int64(1)),
			expectErr: true,
		},
	}

	for _, c := range cases {
		s.Run(c.tag, func() {
			r, err := rowutil.ParseFields(c.v)
			if c.expectErr {
				s.Error(err)
				return
			}
			s.NoError(err)
			s.Equal(len(c.expect), len(r))
			for k, v := range c.expect {
				rv, has := r[k]
				s.Require().True(has, fmt.Sprintf("candidate with key(%s) must provided", k))
				s.Equal(v, rv.Value.Interface())
			}
		})
	}
}

func (s *RowsSuite) TestNullablePointerColumns() {
	s.Run("nil_pointer_appends_null", func() {
		type NullableRow struct {
			ID     int64   `milvus:"primary_key"`
			Name   *string `milvus:"max_length:256"`
			Age    *int32
			Vector []float32 `milvus:"dim:16"`
		}

		columns, err := AnyToColumns([]any{&NullableRow{
			ID:     1,
			Name:   nil,
			Age:    nil,
			Vector: make([]float32, 16),
		}}, false)
		s.NoError(err)

		for _, col := range columns {
			if col.Name() == "Name" || col.Name() == "Age" {
				s.True(col.Nullable(), "column %s should be nullable", col.Name())
				isNull, err := col.IsNull(0)
				s.NoError(err)
				s.True(isNull, "column %s should have null at index 0", col.Name())
			}
		}
	})

	s.Run("non_nil_pointer_appends_value", func() {
		type NullableRow struct {
			ID     int64   `milvus:"primary_key"`
			Name   *string `milvus:"max_length:256"`
			Age    *int32
			Vector []float32 `milvus:"dim:16"`
		}

		name := "test"
		age := int32(25)
		columns, err := AnyToColumns([]any{&NullableRow{
			ID:     1,
			Name:   &name,
			Age:    &age,
			Vector: make([]float32, 16),
		}}, false)
		s.NoError(err)

		for _, col := range columns {
			switch col.Name() {
			case "Name":
				s.True(col.Nullable())
				isNull, err := col.IsNull(0)
				s.NoError(err)
				s.False(isNull)
				val, err := col.Get(0)
				s.NoError(err)
				s.Equal("test", val)
			case "Age":
				s.True(col.Nullable())
				isNull, err := col.IsNull(0)
				s.NoError(err)
				s.False(isNull)
				val, err := col.Get(0)
				s.NoError(err)
				s.Equal(int32(25), val)
			}
		}
	})

	s.Run("mixed_nil_and_values", func() {
		type NullableRow struct {
			ID     int64     `milvus:"primary_key"`
			Name   *string   `milvus:"max_length:256"`
			Vector []float32 `milvus:"dim:16"`
		}

		name := "hello"
		columns, err := AnyToColumns([]any{
			&NullableRow{ID: 1, Name: &name, Vector: make([]float32, 16)},
			&NullableRow{ID: 2, Name: nil, Vector: make([]float32, 16)},
		}, false)
		s.NoError(err)

		for _, col := range columns {
			if col.Name() == "Name" {
				s.True(col.Nullable())

				isNull0, err := col.IsNull(0)
				s.NoError(err)
				s.False(isNull0)
				val, err := col.Get(0)
				s.NoError(err)
				s.Equal("hello", val)

				isNull1, err := col.IsNull(1)
				s.NoError(err)
				s.True(isNull1)
			}
		}
	})
}

func (s *RowsSuite) TestRowsToTimestamptzColumn() {
	s.Run("plain_time", func() {
		type TimestamptzRow struct {
			ID        int64     `milvus:"primary_key"`
			CreatedAt time.Time `milvus:"name:created_at"`
		}

		schema := entity.NewSchema().
			WithField(entity.NewField().WithName("ID").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
			WithField(entity.NewField().WithName("created_at").WithDataType(entity.FieldTypeTimestamptz))

		now := time.Now().UTC().Truncate(time.Nanosecond)
		rows := []any{
			&TimestamptzRow{ID: 1, CreatedAt: now},
			&TimestamptzRow{ID: 2, CreatedAt: now.Add(time.Hour)},
		}

		columns, err := AnyToColumns(rows, false, schema)
		s.Require().NoError(err)
		s.Require().Len(columns, 2)

		for _, col := range columns {
			if col.Name() != "created_at" {
				continue
			}
			s.Equal(entity.FieldTypeTimestamptz, col.Type())
			s.Equal([]string{
				now.Format(time.RFC3339Nano),
				now.Add(time.Hour).Format(time.RFC3339Nano),
			}, col.FieldData().GetScalars().GetStringData().GetData())
		}
	})

	s.Run("nullable_pointer", func() {
		type TimestamptzRow struct {
			ID        int64      `milvus:"primary_key"`
			CreatedAt *time.Time `milvus:"name:created_at"`
		}

		schema := entity.NewSchema().
			WithField(entity.NewField().WithName("ID").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
			WithField(entity.NewField().WithName("created_at").WithDataType(entity.FieldTypeTimestamptz).WithNullable(true))

		now := time.Now().UTC().Truncate(time.Nanosecond)
		rows := []any{
			&TimestamptzRow{ID: 1, CreatedAt: &now},
			&TimestamptzRow{ID: 2, CreatedAt: nil},
		}

		columns, err := AnyToColumns(rows, false, schema)
		s.Require().NoError(err)
		s.Require().Len(columns, 2)

		for _, col := range columns {
			if col.Name() != "created_at" {
				continue
			}
			s.Equal(entity.FieldTypeTimestamptz, col.Type())
			s.True(col.Nullable())
			isNull0, err := col.IsNull(0)
			s.NoError(err)
			s.False(isNull0)
			isNull1, err := col.IsNull(1)
			s.NoError(err)
			s.True(isNull1)
			s.Equal([]string{now.Format(time.RFC3339Nano)},
				col.FieldData().GetScalars().GetStringData().GetData())
		}
	})
}

func (s *RowsSuite) TestColumnCreatorsAllScalarTypes() {
	// Every scalar field type must yield a non-nil column from getColumnCreators.
	// A missing case silently leaves the column nil and panics later in
	// AnyToColumns' AppendValue, so enumerate all supported scalar types here.
	scalarTypes := []entity.FieldType{
		entity.FieldTypeBool,
		entity.FieldTypeInt8,
		entity.FieldTypeInt16,
		entity.FieldTypeInt32,
		entity.FieldTypeInt64,
		entity.FieldTypeFloat,
		entity.FieldTypeDouble,
		entity.FieldTypeString,
		entity.FieldTypeVarChar,
		entity.FieldTypeText,
		entity.FieldTypeJSON,
		entity.FieldTypeTimestamptz,
		entity.FieldTypeGeometry,
	}
	for _, dataType := range scalarTypes {
		s.Run(dataType.Name(), func() {
			field := entity.NewField().WithName("f").WithDataType(dataType)
			creator := getColumnCreators(entity.NewSchema().WithField(field))["f"]
			s.Require().NotNil(creator)
			col, err := creator(1)
			s.Require().NoError(err)
			s.Require().NotNil(col, "field type %s must have a column creator", dataType.Name())
			// FieldTypeString is normalized to a VarChar column by design.
			if dataType == entity.FieldTypeString {
				s.Equal(entity.FieldTypeVarChar, col.Type())
			} else {
				s.Equal(dataType, col.Type())
			}
			s.Zero(col.Len())
		})
	}
}

func (s *RowsSuite) TestRowsToGeometryColumn() {
	type GeometryRow struct {
		ID       int64  `milvus:"primary_key"`
		Location string `milvus:"name:location"`
	}

	schema := entity.NewSchema().
		WithField(entity.NewField().WithName("ID").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("location").WithDataType(entity.FieldTypeGeometry))

	rows := []any{
		&GeometryRow{ID: 1, Location: "POINT (1 1)"},
		&GeometryRow{ID: 2, Location: "POINT (2 2)"},
	}

	columns, err := AnyToColumns(rows, false, schema)
	s.Require().NoError(err)
	s.Require().Len(columns, 2)

	for _, col := range columns {
		if col.Name() != "location" {
			continue
		}
		s.Equal(entity.FieldTypeGeometry, col.Type())
		s.Equal([]string{"POINT (1 1)", "POINT (2 2)"},
			col.FieldData().GetScalars().GetGeometryWktData().GetData())
	}
}

func (s *RowsSuite) TestRowsToTextColumnWithSchema() {
	type TextRow struct {
		ID      int64
		Content string
	}

	schema := entity.NewSchema().
		WithField(entity.NewField().WithName("ID").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("Content").WithDataType(entity.FieldTypeText))
	rows := []any{
		&TextRow{ID: 1, Content: "short text"},
		&TextRow{ID: 2, Content: "中文内容"},
	}

	columns, err := AnyToColumns(rows, false, schema)
	s.Require().NoError(err)
	s.Require().Len(columns, 2)

	var textColumnFound bool
	for _, col := range columns {
		if col.Name() != "Content" {
			continue
		}
		textColumnFound = true
		s.Equal(entity.FieldTypeText, col.Type())
		s.Equal([]string{"short text", "中文内容"}, col.FieldData().GetScalars().GetStringData().GetData())
	}
	s.True(textColumnFound)
}

func (s *RowsSuite) TestSetFieldPointer() {
	s.Run("set_pointer_field_with_value", func() {
		type PtrStruct struct {
			Name *string
		}
		row := &PtrStruct{}
		err := SetField(row, "Name", "hello")
		s.NoError(err)
		s.Require().NotNil(row.Name)
		s.Equal("hello", *row.Name)
	})

	s.Run("set_pointer_field_with_nil", func() {
		type PtrStruct struct {
			Name *string
		}
		name := "old"
		row := &PtrStruct{Name: &name}
		err := SetField(row, "Name", nil)
		s.NoError(err)
		s.Nil(row.Name)
	})

	s.Run("set_non_pointer_field", func() {
		type RegularStruct struct {
			Name string
		}
		row := &RegularStruct{}
		err := SetField(row, "Name", "hello")
		s.NoError(err)
		s.Equal("hello", row.Name)
	})
}

func (s *RowsSuite) TestParseFieldsPointer() {
	s.Run("pointer_field_isPtr", func() {
		type PtrStruct struct {
			Name  *string
			Value int64
		}
		name := "test"
		v := reflect.ValueOf(PtrStruct{Name: &name, Value: 42})
		result, err := rowutil.ParseFields(v)
		s.NoError(err)

		nameCandi, ok := result["Name"]
		s.True(ok)
		s.True(nameCandi.IsPtr)

		valueCandi, ok := result["Value"]
		s.True(ok)
		s.False(valueCandi.IsPtr)
	})
}

func (s *RowsSuite) TestParseFieldsBoundaries() {
	type namedKey string
	name := "kept"
	for _, input := range []any{nil, (*ValidStruct)(nil), map[int]any{1: "value"}} {
		_, err := rowutil.ParseFields(reflect.ValueOf(input))
		s.Error(err)
	}
	for _, input := range []any{
		map[string]*string{"name": &name},
		map[namedKey]*string{"name": &name},
	} {
		fields, err := rowutil.ParseFields(reflect.ValueOf(input))
		s.Require().NoError(err)
		s.Equal(&name, fields["name"].Value.Interface())
		s.False(fields["name"].IsPtr, "map values must retain their representation")
	}
	fields, err := rowutil.ParseFields(reflect.ValueOf(map[string]any(nil)))
	s.NoError(err)
	s.Empty(fields)

	input := &struct {
		ID      int64      `milvus:"primary_key;name:id"`
		Vector  [2]float32 `milvus:"name:vector"`
		Ignored int        `milvus:"-"`
	}{ID: 1, Vector: [2]float32{2, 3}}
	fields, err = rowutil.ParseFields(reflect.ValueOf(&input))
	s.Require().NoError(err)
	s.Len(fields, 2)
	s.Equal(int64(1), fields["id"].Value.Interface())
	s.Equal([]float32{2, 3}, fields["vector"].Value.Interface())
	s.True(fields["id"].Value.CanSet())
}

func (s *RowsSuite) TestParseTagSettingCompatibility() {
	for _, tc := range []struct {
		tag  string
		want map[string]string
	}{
		{"", map[string]string{}},
		{";name:profile;;", map[string]string{"NAME": "profile"}},
		{"primary_key; name:profile;dim:2", map[string]string{"PRIMARY_KEY": "PRIMARY_KEY", "NAME": "profile", "DIM": "2"}},
		{`name:a\;b\;c:tail;auto_id`, map[string]string{"NAME": "a;b;c:tail", "AUTO_ID": "AUTO_ID"}},
	} {
		s.Equal(tc.want, ParseTagSetting(tc.tag, MilvusTagSep))
	}
}

func (s *RowsSuite) TestSetFieldEmbeddedName() {
	type identity struct {
		ID *int64 `milvus:"name:id"`
	}
	input := &struct {
		identity
		Ignored int64 `milvus:"-"`
	}{}
	s.NoError(SetField(&input, "id", int64(42)))
	s.Require().NotNil(input.ID)
	s.Equal(int64(42), *input.ID)
	s.NoError(SetField(input, "Ignored", int64(7)))
	s.Zero(input.Ignored)
	s.NoError(SetField(input, "id", nil))
	s.Nil(input.ID)
}

func TestRows(t *testing.T) {
	suite.Run(t, new(RowsSuite))
}
