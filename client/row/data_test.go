package row

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/client/v2/entity"
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

func (s *RowsSuite) TestReflectValueCandi() {
	type DynamicRows struct {
		Float float32 `json:"float" milvus:"name:float"`
	}

	cases := []struct {
		tag       string
		v         reflect.Value
		expect    map[string]fieldCandi
		expectErr bool
	}{
		{
			tag: "MapRow",
			v: reflect.ValueOf(map[string]interface{}{
				"A": "abd", "B": int64(8),
			}),
			expect: map[string]fieldCandi{
				"A": {
					name: "A",
					v:    reflect.ValueOf("abd"),
				},
				"B": {
					name: "B",
					v:    reflect.ValueOf(int64(8)),
				},
			},
			expectErr: false,
		},
		{
			tag: "StructRow",
			v: reflect.ValueOf(struct {
				A string
				B int64
			}{A: "abc", B: 16}),
			expect: map[string]fieldCandi{
				"A": {
					name: "A",
					v:    reflect.ValueOf("abc"),
				},
				"B": {
					name: "B",
					v:    reflect.ValueOf(int64(16)),
				},
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
			expect: map[string]fieldCandi{
				"a": {
					name: "a",
					v:    reflect.ValueOf("emb"),
				},
				"float": {
					name: "float",
					v:    reflect.ValueOf(float32(0.1)),
				},
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
			r, err := reflectValueCandi(c.v)
			if c.expectErr {
				s.Error(err)
				return
			}
			s.NoError(err)
			s.Equal(len(c.expect), len(r))
			for k, v := range c.expect {
				rv, has := r[k]
				s.Require().True(has, fmt.Sprintf("candidate with key(%s) must provided", k))
				s.Equal(v.name, rv.name)
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

func (s *RowsSuite) TestReflectValueCandiPointer() {
	s.Run("pointer_field_isPtr", func() {
		type PtrStruct struct {
			Name  *string
			Value int64
		}
		name := "test"
		v := reflect.ValueOf(PtrStruct{Name: &name, Value: 42})
		result, err := reflectValueCandi(v)
		s.NoError(err)

		nameCandi, ok := result["Name"]
		s.True(ok)
		s.True(nameCandi.isPtr)

		valueCandi, ok := result["Value"]
		s.True(ok)
		s.False(valueCandi.isPtr)
	})
}

func TestRows(t *testing.T) {
	suite.Run(t, new(RowsSuite))
}
