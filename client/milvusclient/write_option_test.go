// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package milvusclient

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/client/v2/column"
	"github.com/milvus-io/milvus/client/v2/entity"
)

type ColumnBasedDataOptionSuite struct {
	MockSuiteBase
}

func (s *ColumnBasedDataOptionSuite) NullableCompatible() {
	intCol := column.NewColumnInt64("rbdo_field", []int64{1, 2, 3})
	rbdo := NewColumnBasedInsertOption("rbdo_nullable", intCol)

	coll := &entity.Collection{
		Schema: entity.NewSchema().WithField(entity.NewField().WithName("rbdo_field").WithDataType(entity.FieldTypeInt64).WithNullable(true)),
	}
	req, err := rbdo.InsertRequest(coll)
	s.NoError(err)

	s.Require().Len(req.GetFieldsData(), 1)
	fd := req.GetFieldsData()[0]
	s.ElementsMatch([]int64{1, 2, 3}, fd.GetScalars().GetLongData())
	s.ElementsMatch([]bool{true, true, true}, fd.GetValidData())
}

func (s *ColumnBasedDataOptionSuite) TestWithStructArrayColumn() {
	dim := 4
	structSchema := entity.NewStructSchema().
		WithField(entity.NewField().WithName("clip_str").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64)).
		WithField(entity.NewField().WithName("clip_emb").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(dim)))

	collSchema := entity.NewSchema().WithName("c").
		WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true)).
		WithField(entity.NewField().WithName("vec").WithDataType(entity.FieldTypeFloatVector).WithDim(int64(dim))).
		WithField(entity.NewField().
			WithName("clips").
			WithDataType(entity.FieldTypeArray).
			WithElementType(entity.FieldTypeStruct).
			WithMaxCapacity(16).
			WithStructSchema(structSchema))

	rows := []map[string]any{
		{"clip_str": []string{"a", "b"}, "clip_emb": [][]float32{{0.1, 0.2, 0.3, 0.4}, {0.5, 0.6, 0.7, 0.8}}},
		{"clip_str": []string{"c"}, "clip_emb": [][]float32{{1.0, 1.0, 1.0, 1.0}}},
	}

	opt := NewColumnBasedInsertOption("c").
		WithInt64Column("id", []int64{1, 2}).
		WithFloatVectorColumn("vec", dim, [][]float32{{0, 0, 0, 0}, {1, 1, 1, 1}}).
		WithStructArrayColumn("clips", structSchema, rows)

	coll := &entity.Collection{Schema: collSchema}
	req, err := opt.InsertRequest(coll)
	s.Require().NoError(err)
	s.EqualValues(2, req.GetNumRows())

	var clipsFD *schemapb.FieldData
	for _, fd := range req.GetFieldsData() {
		if fd.GetFieldName() == "clips" {
			clipsFD = fd
			break
		}
	}
	s.Require().NotNil(clipsFD)
	s.Equal(schemapb.DataType_ArrayOfStruct, clipsFD.GetType())

	subs := clipsFD.GetStructArrays().GetFields()
	s.Require().Equal(2, len(subs))

	// Find each sub by name (order is not guaranteed by builder).
	var strSub, embSub *schemapb.FieldData
	for _, sub := range subs {
		switch sub.GetFieldName() {
		case "clip_str":
			strSub = sub
		case "clip_emb":
			embSub = sub
		}
	}
	s.Require().NotNil(strSub)
	s.Require().NotNil(embSub)

	s.Equal(schemapb.DataType_Array, strSub.GetType())
	arr := strSub.GetScalars().GetArrayData().GetData()
	s.Require().Equal(2, len(arr))
	s.Equal([]string{"a", "b"}, arr[0].GetStringData().GetData())
	s.Equal([]string{"c"}, arr[1].GetStringData().GetData())

	s.Equal(schemapb.DataType_ArrayOfVector, embSub.GetType())
	va := embSub.GetVectors().GetVectorArray()
	s.Require().NotNil(va)
	s.EqualValues(dim, va.GetDim())
	s.Equal(schemapb.DataType_FloatVector, va.GetElementType())
	s.Require().Equal(2, len(va.GetData()))
	s.EqualValues(2*dim, len(va.GetData()[0].GetFloatVector().GetData()))
	s.EqualValues(1*dim, len(va.GetData()[1].GetFloatVector().GetData()))
}

func (s *ColumnBasedDataOptionSuite) TestWithStructArrayColumnDeferredError() {
	structSchema := entity.NewStructSchema().
		WithField(entity.NewField().WithName("clip_str").WithDataType(entity.FieldTypeVarChar).WithMaxLength(64))

	// Pass rows with missing sub-field — builder must NOT panic; error surfaces on InsertRequest.
	s.NotPanics(func() {
		opt := NewColumnBasedInsertOption("c").
			WithInt64Column("id", []int64{1}).
			WithStructArrayColumn("clips", structSchema, []map[string]any{{"wrong_key": []string{"a"}}})

		coll := &entity.Collection{Schema: entity.NewSchema().WithName("c").
			WithField(entity.NewField().WithName("id").WithDataType(entity.FieldTypeInt64).WithIsPrimaryKey(true))}
		_, err := opt.InsertRequest(coll)
		s.Require().Error(err)
	})
}

func TestRowBasedDataOption(t *testing.T) {
	suite.Run(t, new(ColumnBasedDataOptionSuite))
}

type DeleteOptionSuite struct {
	MockSuiteBase
}

func (s *DeleteOptionSuite) TestBasic() {
	collectionName := fmt.Sprintf("coll_%s", s.randString(6))
	opt := NewDeleteOption(collectionName)

	s.Equal(collectionName, opt.Request().GetCollectionName())
}

func TestDeleteOption(t *testing.T) {
	suite.Run(t, new(DeleteOptionSuite))
}
