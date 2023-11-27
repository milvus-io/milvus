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

package metacache

import (
	"sync"

	"github.com/apache/arrow/go/v12/arrow"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	milvus_storage "github.com/milvus-io/milvus-storage/go/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type StorageV2Cache struct {
	arrowSchema *arrow.Schema
	spaceMu     sync.Mutex
	spaces      map[int64]*milvus_storage.Space
}

func (s *StorageV2Cache) ArrowSchema() *arrow.Schema {
	return s.arrowSchema
}

func (s *StorageV2Cache) GetOrCreateSpace(segmentID int64, creator func() (*milvus_storage.Space, error)) (*milvus_storage.Space, error) {
	s.spaceMu.Lock()
	defer s.spaceMu.Unlock()
	space, ok := s.spaces[segmentID]
	if ok {
		return space, nil
	}
	space, err := creator()
	if err != nil {
		return nil, err
	}
	s.spaces[segmentID] = space
	return space, nil
}

// only for unit test
func (s *StorageV2Cache) SetSpace(segmentID int64, space *milvus_storage.Space) {
	s.spaceMu.Lock()
	defer s.spaceMu.Unlock()
	s.spaces[segmentID] = space
}

func NewStorageV2Cache(schema *schemapb.CollectionSchema) (*StorageV2Cache, error) {
	arrowSchema, err := ConvertToArrowSchema(schema.Fields)
	if err != nil {
		return nil, err
	}
	return &StorageV2Cache{
		arrowSchema: arrowSchema,
		spaces:      make(map[int64]*milvus_storage.Space),
	}, nil
}

func ConvertToArrowSchema(fields []*schemapb.FieldSchema) (*arrow.Schema, error) {
	arrowFields := make([]arrow.Field, 0, len(fields))
	for _, field := range fields {
		switch field.DataType {
		case schemapb.DataType_Bool:
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: arrow.FixedWidthTypes.Boolean,
			})
		case schemapb.DataType_Int8:
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: arrow.PrimitiveTypes.Int8,
			})
		case schemapb.DataType_Int16:
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: arrow.PrimitiveTypes.Int16,
			})
		case schemapb.DataType_Int32:
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: arrow.PrimitiveTypes.Int32,
			})
		case schemapb.DataType_Int64:
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: arrow.PrimitiveTypes.Int64,
			})
		case schemapb.DataType_Float:
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: arrow.PrimitiveTypes.Float32,
			})
		case schemapb.DataType_Double:
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: arrow.PrimitiveTypes.Float64,
			})
		case schemapb.DataType_String, schemapb.DataType_VarChar:
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: arrow.BinaryTypes.String,
			})
		case schemapb.DataType_Array:
			elemType, err := convertToArrowType(field.ElementType)
			if err != nil {
				return nil, err
			}
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: arrow.ListOf(elemType),
			})
		case schemapb.DataType_JSON:
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: arrow.BinaryTypes.Binary,
			})
		case schemapb.DataType_BinaryVector:
			dim, err := storage.GetDimFromParams(field.TypeParams)
			if err != nil {
				return nil, err
			}
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: &arrow.FixedSizeBinaryType{ByteWidth: dim / 8},
			})
		case schemapb.DataType_FloatVector:
			dim, err := storage.GetDimFromParams(field.TypeParams)
			if err != nil {
				return nil, err
			}
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: &arrow.FixedSizeBinaryType{ByteWidth: dim * 4},
			})
		case schemapb.DataType_Float16Vector:
			dim, err := storage.GetDimFromParams(field.TypeParams)
			if err != nil {
				return nil, err
			}
			arrowFields = append(arrowFields, arrow.Field{
				Name: field.Name,
				Type: &arrow.FixedSizeBinaryType{ByteWidth: dim * 2},
			})
		default:
			return nil, merr.WrapErrParameterInvalidMsg("unknown type %v", field.DataType.String())
		}
	}

	return arrow.NewSchema(arrowFields, nil), nil
}

func convertToArrowType(dataType schemapb.DataType) (arrow.DataType, error) {
	switch dataType {
	case schemapb.DataType_Bool:
		return arrow.FixedWidthTypes.Boolean, nil
	case schemapb.DataType_Int8:
		return arrow.PrimitiveTypes.Int8, nil
	case schemapb.DataType_Int16:
		return arrow.PrimitiveTypes.Int16, nil
	case schemapb.DataType_Int32:
		return arrow.PrimitiveTypes.Int32, nil
	case schemapb.DataType_Int64:
		return arrow.PrimitiveTypes.Int64, nil
	case schemapb.DataType_Float:
		return arrow.PrimitiveTypes.Float32, nil
	case schemapb.DataType_Double:
		return arrow.PrimitiveTypes.Float64, nil
	case schemapb.DataType_String, schemapb.DataType_VarChar:
		return arrow.BinaryTypes.String, nil
	default:
		return nil, merr.WrapErrParameterInvalidMsg("unknown type %v", dataType.String())
	}
}
