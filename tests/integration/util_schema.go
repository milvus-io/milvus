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

package integration

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
)

const (
	BoolField           = "boolField"
	Int8Field           = "int8Field"
	Int16Field          = "int16Field"
	Int32Field          = "int32Field"
	Int64Field          = "int64Field"
	FloatField          = "floatField"
	DoubleField         = "doubleField"
	VarCharField        = "varCharField"
	JSONField           = "jsonField"
	FloatVecField       = "floatVecField"
	BinVecField         = "binVecField"
	Float16VecField     = "float16VecField"
	BFloat16VecField    = "bfloat16VecField"
	SparseFloatVecField = "sparseFloatVecField"
)

func ConstructSchema(collection string, dim int, autoID bool, fields ...*schemapb.FieldSchema) *schemapb.CollectionSchema {
	// if fields are specified, construct it
	if len(fields) > 0 {
		return &schemapb.CollectionSchema{
			Name:   collection,
			AutoID: autoID,
			Fields: fields,
		}
	}

	// if no field is specified, use default
	pk := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         Int64Field,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       autoID,
	}
	fVec := &schemapb.FieldSchema{
		FieldID:      101,
		Name:         FloatVecField,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: fmt.Sprintf("%d", dim),
			},
		},
		IndexParams: nil,
	}
	return &schemapb.CollectionSchema{
		Name:   collection,
		AutoID: autoID,
		Fields: []*schemapb.FieldSchema{pk, fVec},
	}
}

func ConstructSchemaOfVecDataType(collection string, dim int, autoID bool, dataType schemapb.DataType) *schemapb.CollectionSchema {
	pk := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         Int64Field,
		IsPrimaryKey: true,
		Description:  "",
		DataType:     schemapb.DataType_Int64,
		TypeParams:   nil,
		IndexParams:  nil,
		AutoID:       autoID,
	}
	var name string
	var typeParams []*commonpb.KeyValuePair
	switch dataType {
	case schemapb.DataType_FloatVector:
		name = FloatVecField
		typeParams = []*commonpb.KeyValuePair{
			{
				Key:   common.DimKey,
				Value: fmt.Sprintf("%d", dim),
			},
		}
	case schemapb.DataType_SparseFloatVector:
		name = SparseFloatVecField
		typeParams = nil
	default:
		panic("unsupported data type")
	}
	fVec := &schemapb.FieldSchema{
		FieldID:      101,
		Name:         name,
		IsPrimaryKey: false,
		Description:  "",
		DataType:     dataType,
		TypeParams:   typeParams,
		IndexParams:  nil,
	}
	return &schemapb.CollectionSchema{
		Name:   collection,
		AutoID: autoID,
		Fields: []*schemapb.FieldSchema{pk, fVec},
	}
}
