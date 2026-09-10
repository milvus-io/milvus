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
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type CreateCollectionReq struct {
	DbName             string `json:"dbName"`
	CollectionName     string `json:"collectionName" validate:"required"`
	Dimension          int32  `json:"dimension" validate:"required"`
	Description        string `json:"description"`
	MetricType         string `json:"metricType"`
	PrimaryField       string `json:"primaryField"`
	VectorField        string `json:"vectorField"`
	EnableDynamicField bool   `json:"enableDynamicField"`
}

type DropCollectionReq struct {
	DbName         string `json:"dbName"`
	CollectionName string `json:"collectionName" validate:"required"`
}

type QueryReq struct {
	DbName         string   `json:"dbName"`
	CollectionName string   `json:"collectionName" validate:"required"`
	OutputFields   []string `json:"outputFields"`
	Filter         string   `json:"filter" validate:"required"`
	Limit          int32    `json:"limit"`
	Offset         int32    `json:"offset"`
}

type GetReq struct {
	DbName         string      `json:"dbName"`
	CollectionName string      `json:"collectionName" validate:"required"`
	OutputFields   []string    `json:"outputFields"`
	ID             interface{} `json:"id" validate:"required"`
}

type DeleteReq struct {
	DbName         string      `json:"dbName"`
	CollectionName string      `json:"collectionName" validate:"required"`
	ID             interface{} `json:"id"`
	Filter         string      `json:"filter"`
}

type InsertReq struct {
	DbName         string                   `json:"dbName"`
	CollectionName string                   `json:"collectionName" validate:"required"`
	Data           []map[string]interface{} `json:"data" validate:"required"`
}

type SingleInsertReq struct {
	DbName         string                 `json:"dbName"`
	CollectionName string                 `json:"collectionName" validate:"required"`
	Data           map[string]interface{} `json:"data" validate:"required"`
}

type UpsertReq struct {
	DbName         string                    `json:"dbName"`
	CollectionName string                    `json:"collectionName" validate:"required"`
	Data           []map[string]interface{}  `json:"data" validate:"required"`
	PartialUpdate  bool                      `json:"partialUpdate"`
	FieldOps       []FieldPartialUpdateOpReq `json:"fieldOps"`
}

type SingleUpsertReq struct {
	DbName         string                    `json:"dbName"`
	CollectionName string                    `json:"collectionName" validate:"required"`
	Data           map[string]interface{}    `json:"data" validate:"required"`
	PartialUpdate  bool                      `json:"partialUpdate"`
	FieldOps       []FieldPartialUpdateOpReq `json:"fieldOps"`
}

// FloatVectorQuery is a search vector that refuses a null coordinate.
//
// Decoding straight into []float32 turns a null into 0, which is a coordinate
// the caller never sent and which takes full part in the distance computation,
// so the search silently runs against a different point. A vector is a dense
// fixed-width array of numbers with no per-element validity to record "absent"
// in, so there is nothing to store but a number: the only honest answer is to
// refuse. The v2 path applies the same rule in serializeFloatVectors.
type FloatVectorQuery []float32

func (v *FloatVectorQuery) UnmarshalJSON(data []byte) error {
	var elements []*float32
	if err := json.Unmarshal(data, &elements); err != nil {
		return err
	}
	if elements == nil {
		// A whole-value null decodes to a nil slice without an error, and the
		// loop below would then hand back a non-nil empty vector -- which is
		// how a null used to slip past the callers' required-field checks.
		return merr.WrapErrParameterInvalidMsg("a vector cannot be null")
	}
	parsed := make([]float32, 0, len(elements))
	for idx, element := range elements {
		if element == nil {
			return merr.WrapErrParameterInvalidMsg(
				"search vector has a null at index %d; a vector element cannot be null", idx)
		}
		parsed = append(parsed, *element)
	}
	*v = parsed
	return nil
}

// Int64ListQuery is a list of int64 that refuses a null element, for the same
// reason FloatVectorQuery does: encoding/json leaves a null as 0, which names
// a different row than the caller did. See FloatVectorQuery.
type Int64ListQuery []int64

func (v *Int64ListQuery) UnmarshalJSON(data []byte) error {
	var elements []*int64
	if err := json.Unmarshal(data, &elements); err != nil {
		return err
	}
	if elements == nil {
		// same rule as FloatVectorQuery: a null is not an empty list
		return merr.WrapErrParameterInvalidMsg("an id list cannot be null")
	}
	parsed := make([]int64, 0, len(elements))
	for idx, element := range elements {
		if element == nil {
			return merr.WrapErrParameterInvalidMsg(
				"id list has a null at index %d; an id cannot be null", idx)
		}
		parsed = append(parsed, *element)
	}
	*v = parsed
	return nil
}

// Base64VectorQuery is a base64-spelled vector that refuses a null. Decoding
// straight into []byte turns a null into an empty slice, which then travels as
// a zero-length vector rather than being reported.
type Base64VectorQuery []byte

func (v *Base64VectorQuery) UnmarshalJSON(data []byte) error {
	var decoded *[]byte
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded == nil {
		return merr.WrapErrParameterInvalidMsg("a vector cannot be null")
	}
	*v = *decoded
	return nil
}

type SearchReq struct {
	DbName            string                `json:"dbName"`
	CollectionName    string                `json:"collectionName" validate:"required"`
	Filter            string                `json:"filter"`
	Limit             int32                 `json:"limit"`
	Offset            int32                 `json:"offset"`
	OutputFields      []string              `json:"outputFields"`
	Vector            FloatVectorQuery      `json:"vector"`
	Params            map[string]float64    `json:"params"`
	SearchAggregation *SearchAggregationReq `json:"searchAggregation"`
}
