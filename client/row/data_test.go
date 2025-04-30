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
		columns, err := AnyToColumns([]any{&ValidStruct{}})
		s.Nil(err)
		s.Equal(10, len(columns))

		columns, err = AnyToColumns([]any{&ValidStruct2{}})
		s.Nil(err)
		s.Equal(3, len(columns))
	})

	s.Run("auto_id_pk", func() {
		type AutoPK struct {
			ID     int64     `milvus:"primary_key;auto_id"`
			Vector []float32 `milvus:"dim:32"`
		}
		columns, err := AnyToColumns([]any{&AutoPK{}})
		s.Nil(err)
		s.Require().Equal(1, len(columns))
		s.Equal("Vector", columns[0].Name())
	})

	s.Run("fp16", func() {
		type BF16Struct struct {
			ID     int64  `milvus:"primary_key;auto_id"`
			Vector []byte `milvus:"dim:16;vector_type:bf16"`
		}
		columns, err := AnyToColumns([]any{&BF16Struct{}})
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
		columns, err := AnyToColumns([]any{&FP16Struct{}})
		s.Nil(err)
		s.Require().Equal(1, len(columns))
		s.Equal("Vector", columns[0].Name())
		s.Equal(entity.FieldTypeFloat16Vector, columns[0].Type())
	})

	s.Run("invalid_cases", func() {
		// empty input
		_, err := AnyToColumns([]any{})
		s.NotNil(err)

		// incompatible rows
		_, err = AnyToColumns([]any{&ValidStruct{}, &ValidStruct2{}})
		s.NotNil(err)

		// schema & row not compatible
		_, err = AnyToColumns([]any{&ValidStruct{}}, &entity.Schema{
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
		columns, err := AnyToColumns([]any{&ValidStruct{}},
			entity.NewSchema().WithDynamicFieldEnabled(true),
		)
		s.NoError(err)
		s.Equal(1, len(columns))
	})

	s.Run("dynamic_not_found", func() {
		_, err := AnyToColumns([]any{&ValidStruct{}},
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

func TestRows(t *testing.T) {
	suite.Run(t, new(RowsSuite))
}
