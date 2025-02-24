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

package httpserver

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// We wrap original protobuf structure for 2 reasons:
// 1. Milvus uses `bytes` as the type of `schema` field,
// while the bytes has to be serialized by proto.Marshal.
// It's very inconvenient for an HTTP clien to do this,
// so we change the type to a struct,
// and does the conversion for user.
// 2. Some fields uses proto.oneof, does not supported directly json marshal
// so we have to implements the marshal procedure. example: InsertReqeust

// WrappedCreateCollectionRequest wraps CreateCollectionRequest
type WrappedCreateCollectionRequest struct {
	// Not useful for now
	Base *commonpb.MsgBase `protobuf:"bytes,1,opt,name=base,proto3" json:"base,omitempty"`
	// Not useful for now
	DbName string `protobuf:"bytes,2,opt,name=db_name,json=dbName,proto3" json:"db_name,omitempty"`
	// The unique collection name in milvus.(Required)
	CollectionName string `protobuf:"bytes,3,opt,name=collection_name,json=collectionName,proto3" json:"collection_name,omitempty"`
	// The serialized `schema.CollectionSchema`(Required)
	Schema schemapb.CollectionSchema `protobuf:"bytes,4,opt,name=schema,proto3" json:"schema,omitempty"`
	// Once set, no modification is allowed (Optional)
	// https://github.com/milvus-io/milvus/issues/6690
	ShardsNum int32 `protobuf:"varint,5,opt,name=shards_num,json=shardsNum,proto3" json:"shards_num,omitempty"`
	// The consistency level that the collection used, modification is not supported now.
	ConsistencyLevel commonpb.ConsistencyLevel `protobuf:"varint,6,opt,name=consistency_level,json=consistencyLevel,proto3,enum=milvus.proto.common.ConsistencyLevel" json:"consistency_level,omitempty"`
	Properties       []*commonpb.KeyValuePair  `protobuf:"bytes,13,rep,name=properties,proto3" json:"properties,omitempty"`
}

// WrappedInsertRequest is the InsertRequest wrapped for RESTful request
type WrappedInsertRequest struct {
	Base           *commonpb.MsgBase `json:"base,omitempty"`
	DbName         string            `json:"db_name,omitempty"`
	CollectionName string            `json:"collection_name,omitempty"`
	PartitionName  string            `json:"partition_name,omitempty"`
	FieldsData     []*FieldData      `json:"fields_data,omitempty"`
	HashKeys       []uint32          `json:"hash_keys,omitempty"`
	NumRows        uint32            `json:"num_rows,omitempty"`
}

func (w *WrappedInsertRequest) AsInsertRequest() (*milvuspb.InsertRequest, error) {
	fieldData, err := convertFieldDataArray(w.FieldsData)
	if err != nil {
		return nil, fmt.Errorf("%w: convert field data failed: %v", errBadRequest, err)
	}
	return &milvuspb.InsertRequest{
		Base:           w.Base,
		DbName:         w.DbName,
		CollectionName: w.CollectionName,
		PartitionName:  w.PartitionName,
		FieldsData:     fieldData,
		HashKeys:       w.HashKeys,
		NumRows:        w.NumRows,
	}, nil
}

// FieldData is the field data in RESTful request that can be convertd to schemapb.FieldData
type FieldData struct {
	Type      schemapb.DataType `json:"type,omitempty"`
	FieldName string            `json:"field_name,omitempty"`
	Field     json.RawMessage   `json:"field,omitempty"` // we use postpone the unmarshal until we know the type
	FieldID   int64             `json:"field_id,omitempty"`
}

func (f *FieldData) makePbFloat16OrBfloat16Array(raw json.RawMessage, serializeFunc func([]float32) []byte) ([]byte, int64, error) {
	wrappedData := [][]float32{}
	err := json.Unmarshal(raw, &wrappedData)
	if err != nil {
		return nil, 0, newFieldDataError(f.FieldName, err)
	}
	if len(wrappedData) < 1 {
		return nil, 0, errors.New("at least one row for insert")
	}
	array0 := wrappedData[0]
	dim := len(array0)
	if dim < 1 {
		return nil, 0, errors.New("dim must >= 1")
	}
	data := make([]byte, 0, len(wrappedData)*dim*2)
	for _, fp32Array := range wrappedData {
		data = append(data, serializeFunc(fp32Array)...)
	}
	return data, int64(dim), nil
}

// AsSchemapb converts the FieldData to schemapb.FieldData
func (f *FieldData) AsSchemapb() (*schemapb.FieldData, error) {
	// is scarlar
	ret := schemapb.FieldData{
		Type:      f.Type,
		FieldName: f.FieldName,
		FieldId:   f.FieldID,
	}

	raw := f.Field
	switch f.Type {
	case schemapb.DataType_Bool:
		data := []bool{}
		err := json.Unmarshal(raw, &data)
		if err != nil {
			return nil, newFieldDataError(f.FieldName, err)
		}
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_BoolData{
					BoolData: &schemapb.BoolArray{
						Data: data,
					},
				},
			},
		}
	case schemapb.DataType_VarChar:
		data := []string{}
		err := json.Unmarshal(raw, &data)
		if err != nil {
			return nil, newFieldDataError(f.FieldName, err)
		}
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: data,
					},
				},
			},
		}
	case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
		data := []int32{}
		err := json.Unmarshal(raw, &data)
		if err != nil {
			return nil, newFieldDataError(f.FieldName, err)
		}
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_IntData{
					IntData: &schemapb.IntArray{
						Data: data,
					},
				},
			},
		}
	case schemapb.DataType_Int64:
		data := []int64{}
		err := json.Unmarshal(raw, &data)
		if err != nil {
			return nil, newFieldDataError(f.FieldName, err)
		}
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_LongData{
					LongData: &schemapb.LongArray{
						Data: data,
					},
				},
			},
		}
	case schemapb.DataType_Float:
		data := []float32{}
		err := json.Unmarshal(raw, &data)
		if err != nil {
			return nil, newFieldDataError(f.FieldName, err)
		}
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{
						Data: data,
					},
				},
			},
		}

	case schemapb.DataType_Double:
		data := []float64{}
		err := json.Unmarshal(raw, &data)
		if err != nil {
			return nil, newFieldDataError(f.FieldName, err)
		}
		ret.Field = &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_DoubleData{
					DoubleData: &schemapb.DoubleArray{
						Data: data,
					},
				},
			},
		}

	case schemapb.DataType_FloatVector:
		wrappedData := [][]float32{}
		err := json.Unmarshal(raw, &wrappedData)
		if err != nil {
			return nil, newFieldDataError(f.FieldName, err)
		}
		if len(wrappedData) < 1 {
			return nil, errors.New("at least one row for insert")
		}
		array0 := wrappedData[0]
		dim := len(array0)
		if dim < 1 {
			return nil, errors.New("dim must >= 1")
		}
		data := make([]float32, len(wrappedData)*dim)

		var i int
		for _, dataArray := range wrappedData {
			for _, v := range dataArray {
				data[i] = v
				i++
			}
		}
		ret.Field = &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_FloatVector{
					FloatVector: &schemapb.FloatArray{
						Data: data,
					},
				},
			},
		}
	case schemapb.DataType_Float16Vector:
		// only support float32 conversion right now
		data, dim, err := f.makePbFloat16OrBfloat16Array(raw, typeutil.Float32ArrayToFloat16Bytes)
		if err != nil {
			return nil, err
		}
		ret.Field = &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: dim,
				Data: &schemapb.VectorField_Float16Vector{
					Float16Vector: data,
				},
			},
		}
	case schemapb.DataType_BFloat16Vector:
		// only support float32 conversion right now
		data, dim, err := f.makePbFloat16OrBfloat16Array(raw, typeutil.Float32ArrayToBFloat16Bytes)
		if err != nil {
			return nil, err
		}
		ret.Field = &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: dim,
				Data: &schemapb.VectorField_Bfloat16Vector{
					Bfloat16Vector: data,
				},
			},
		}
	case schemapb.DataType_SparseFloatVector:
		var wrappedData []map[string]interface{}
		err := json.Unmarshal(raw, &wrappedData)
		if err != nil {
			return nil, newFieldDataError(f.FieldName, err)
		}
		if len(wrappedData) < 1 {
			return nil, errors.New("at least one row for insert")
		}
		data := make([][]byte, len(wrappedData))
		dim := int64(0)
		for _, row := range wrappedData {
			rowData, err := typeutil.CreateSparseFloatRowFromMap(row)
			if err != nil {
				return nil, newFieldDataError(f.FieldName, err)
			}
			data = append(data, rowData)
			rowDim := typeutil.SparseFloatRowDim(rowData)
			if rowDim > dim {
				dim = rowDim
			}
		}

		ret.Field = &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: dim,
				Data: &schemapb.VectorField_SparseFloatVector{
					SparseFloatVector: &schemapb.SparseFloatArray{
						Dim:      dim,
						Contents: data,
					},
				},
			},
		}
	case schemapb.DataType_Int8Vector:
		wrappedData := [][]int8{}
		err := json.Unmarshal(raw, &wrappedData)
		if err != nil {
			return nil, newFieldDataError(f.FieldName, err)
		}
		if len(wrappedData) < 1 {
			return nil, errors.New("at least one row for insert")
		}
		array0 := wrappedData[0]
		dim := len(array0)
		if dim < 1 {
			return nil, errors.New("dim must >= 1")
		}
		data := make([]byte, len(wrappedData)*dim)

		var i int
		for _, dataArray := range wrappedData {
			for _, v := range dataArray {
				data[i] = byte(v)
				i++
			}
		}
		ret.Field = &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(dim),
				Data: &schemapb.VectorField_Int8Vector{
					Int8Vector: data,
				},
			},
		}
	default:
		return nil, errors.New("unsupported data type")
	}
	return &ret, nil
}

func newFieldDataError(field string, err error) error {
	return fmt.Errorf("parse field[%s]: %s", field, err.Error())
}

func convertFieldDataArray(input []*FieldData) ([]*schemapb.FieldData, error) {
	ret := make([]*schemapb.FieldData, len(input))
	for i, v := range input {
		fieldData, err := v.AsSchemapb()
		if err != nil {
			return nil, err
		}
		ret[i] = fieldData
	}
	return ret, nil
}

// SearchRequest is the RESTful request body for search
type SearchRequest struct {
	Base               *commonpb.MsgBase        `protobuf:"bytes,1,opt,name=base,proto3" json:"base,omitempty"`
	DbName             string                   `protobuf:"bytes,2,opt,name=db_name,json=dbName,proto3" json:"db_name,omitempty"`
	CollectionName     string                   `protobuf:"bytes,3,opt,name=collection_name,json=collectionName,proto3" json:"collection_name,omitempty"`
	PartitionNames     []string                 `protobuf:"bytes,4,rep,name=partition_names,json=partitionNames,proto3" json:"partition_names,omitempty"`
	Dsl                string                   `protobuf:"bytes,5,opt,name=dsl,proto3" json:"dsl,omitempty"`
	DslType            commonpb.DslType         `protobuf:"varint,7,opt,name=dsl_type,json=dslType,proto3,enum=milvus.proto.common.DslType" json:"dsl_type,omitempty"`
	BinaryVectors      [][]byte                 `json:"binary_vectors,omitempty"`
	Vectors            [][]float32              `json:"vectors,omitempty"`
	OutputFields       []string                 `protobuf:"bytes,8,rep,name=output_fields,json=outputFields,proto3" json:"output_fields,omitempty"`
	SearchParams       []*commonpb.KeyValuePair `protobuf:"bytes,9,rep,name=search_params,json=searchParams,proto3" json:"search_params,omitempty"`
	TravelTimestamp    uint64                   `protobuf:"varint,10,opt,name=travel_timestamp,json=travelTimestamp,proto3" json:"travel_timestamp,omitempty"`
	GuaranteeTimestamp uint64                   `protobuf:"varint,11,opt,name=guarantee_timestamp,json=guaranteeTimestamp,proto3" json:"guarantee_timestamp,omitempty"`
	Nq                 int64                    `protobuf:"varint,12,opt,name=nq,proto3" json:"nq,omitempty"`
}

func binaryVector2Bytes(vectors [][]byte) []byte {
	ph := &commonpb.PlaceholderValue{
		Tag:    "$0",
		Type:   commonpb.PlaceholderType_BinaryVector,
		Values: make([][]byte, 0, len(vectors)),
	}
	ph.Values = append(ph.Values, vectors...)
	phg := &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			ph,
		},
	}
	ret, _ := proto.Marshal(phg)
	return ret
}

func vector2Bytes(vectors [][]float32) []byte {
	ph := &commonpb.PlaceholderValue{
		Tag:    "$0",
		Type:   commonpb.PlaceholderType_FloatVector,
		Values: make([][]byte, 0, len(vectors)),
	}
	for _, vector := range vectors {
		ph.Values = append(ph.Values, typeutil.Float32ArrayToBytes(vector))
	}
	phg := &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			ph,
		},
	}
	ret, _ := proto.Marshal(phg)
	return ret
}

// WrappedCalcDistanceRequest is the RESTful request body for calc distance
type WrappedCalcDistanceRequest struct {
	Base *commonpb.MsgBase `protobuf:"bytes,1,opt,name=base,proto3" json:"base,omitempty"`

	OpLeft  VectorsArray `json:"op_left,omitempty"`
	OpRight VectorsArray `json:"op_right,omitempty"`

	Params []*commonpb.KeyValuePair `json:"params,omitempty"`
}

// VectorsArray is vector array, assigned by vectors or ids
type VectorsArray struct {
	// Dim of vectors or binary_vectors, not needed when use ids
	Dim int64 `json:"dim,omitempty"`
	// Vectors is an array of vector divided by given dim. Disabled when ids or binary_vectors is set
	Vectors []float32 `json:"vectors,omitempty"`
	// Vectors is an array of binary vector divided by given dim. Disabled when IDs is set
	BinaryVectors []byte `json:"binary_vectors,omitempty"`
	// IDs of vector field in milvus, if not nil, vectors will be ignored
	IDs *VectorIDs `json:"ids,omitempty"`
}

func (v *VectorsArray) isIDs() bool {
	return v.IDs != nil
}

func (v *VectorsArray) isBinaryVector() bool {
	return v.IDs == nil && len(v.BinaryVectors) > 0
}

// AsPbVectorArray convert as milvuspb.VectorArray
func (v *VectorsArray) AsPbVectorArray() *milvuspb.VectorsArray {
	ret := &milvuspb.VectorsArray{}
	switch {
	case v.isIDs():
		ids := &milvuspb.VectorsArray_IdArray{}
		ids.IdArray = &milvuspb.VectorIDs{
			CollectionName: v.IDs.CollectionName,
			FieldName:      v.IDs.FieldName,
		}
		ids.IdArray.PartitionNames = v.IDs.PartitionNames
		ids.IdArray.IdArray = &schemapb.IDs{}
		ids.IdArray.IdArray.IdField = &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{
				Data: v.IDs.IDArray,
			},
		}
		ret.Array = ids
	case v.isBinaryVector():
		vf := &schemapb.VectorField{
			Dim: v.Dim,
		}
		vf.Data = &schemapb.VectorField_BinaryVector{
			BinaryVector: v.BinaryVectors,
		}
		ret.Array = &milvuspb.VectorsArray_DataArray{
			DataArray: vf,
		}
	default:
		// take it as ordinary vectors
		vf := &schemapb.VectorField{
			Dim: v.Dim,
		}
		vf.Data = &schemapb.VectorField_FloatVector{
			FloatVector: &schemapb.FloatArray{
				Data: v.Vectors,
			},
		}
		ret.Array = &milvuspb.VectorsArray_DataArray{
			DataArray: vf,
		}
	}
	return ret
}

// VectorIDs is an array of id reference in milvus
type VectorIDs struct {
	CollectionName string   `protobuf:"bytes,1,opt,name=collection_name,json=collectionName,proto3" json:"collection_name,omitempty"`
	FieldName      string   `protobuf:"bytes,2,opt,name=field_name,json=fieldName,proto3" json:"field_name,omitempty"`
	PartitionNames []string `json:"partition_names"`
	IDArray        []int64  `json:"id_array,omitempty"`
}
