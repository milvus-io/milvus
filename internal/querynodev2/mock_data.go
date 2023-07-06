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

package querynodev2

import (
	"fmt"
	"math"
	"math/rand"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/planpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// ---------- unittest util functions ----------
// common definitions
const (
	metricTypeKey       = common.MetricTypeKey
	defaultTopK         = int64(10)
	defaultRoundDecimal = int64(6)
	defaultDim          = 128
	defaultNProb        = 10
	defaultEf           = 10
	defaultMetricType   = "L2"
	defaultNQ           = 10
)

const (
	// index type
	IndexFaissIDMap      = "FLAT"
	IndexFaissIVFFlat    = "IVF_FLAT"
	IndexFaissIVFPQ      = "IVF_PQ"
	IndexFaissIVFSQ8     = "IVF_SQ8"
	IndexFaissBinIDMap   = "BIN_FLAT"
	IndexFaissBinIVFFlat = "BIN_IVF_FLAT"

	IndexHNSW = "HNSW"
)

// ---------- unittest util functions ----------
// functions of messages and requests
func genBruteForceDSL(schema *schemapb.CollectionSchema, topK int64, roundDecimal int64) (string, error) {
	var vecFieldName string
	var metricType string
	topKStr := strconv.FormatInt(topK, 10)
	nProbStr := strconv.Itoa(defaultNProb)
	roundDecimalStr := strconv.FormatInt(roundDecimal, 10)
	var fieldID int64
	for _, f := range schema.Fields {
		if f.DataType == schemapb.DataType_FloatVector {
			vecFieldName = f.Name
			fieldID = f.FieldID
			for _, p := range f.IndexParams {
				if p.Key == metricTypeKey {
					metricType = p.Value
				}
			}
		}
	}
	if vecFieldName == "" || metricType == "" {
		err := errors.New("invalid vector field name or metric type")
		return "", err
	}
	return `vector_anns: <
              field_id: ` + fmt.Sprintf("%d", fieldID) + `
              query_info: <
                topk: ` + topKStr + `
                round_decimal: ` + roundDecimalStr + `
                metric_type: "` + metricType + `"
                search_params: "{\"nprobe\": ` + nProbStr + `}"
              >
              placeholder_tag: "$0"
            >`, nil
}

func genDSLByIndexType(schema *schemapb.CollectionSchema, indexType string) (string, error) {
	if indexType == IndexFaissIDMap { // float vector
		return genBruteForceDSL(schema, defaultTopK, defaultRoundDecimal)
	}
	return "", fmt.Errorf("Invalid indexType")
}

func genPlaceHolderGroup(nq int64) ([]byte, error) {
	placeholderValue := &commonpb.PlaceholderValue{
		Tag:    "$0",
		Type:   commonpb.PlaceholderType_FloatVector,
		Values: make([][]byte, 0),
	}
	for i := int64(0); i < nq; i++ {
		var vec = make([]float32, defaultDim)
		for j := 0; j < defaultDim; j++ {
			vec[j] = rand.Float32()
		}
		var rawData []byte
		for k, ele := range vec {
			buf := make([]byte, 4)
			common.Endian.PutUint32(buf, math.Float32bits(ele+float32(k*2)))
			rawData = append(rawData, buf...)
		}
		placeholderValue.Values = append(placeholderValue.Values, rawData)
	}

	// generate placeholder
	placeholderGroup := commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{placeholderValue},
	}
	placeGroupByte, err := proto.Marshal(&placeholderGroup)
	if err != nil {
		return nil, err
	}
	return placeGroupByte, nil
}

func genSimpleRetrievePlanExpr(schema *schemapb.CollectionSchema) ([]byte, error) {
	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}
	planNode := &planpb.PlanNode{
		Node: &planpb.PlanNode_Predicates{
			Predicates: &planpb.Expr{
				Expr: &planpb.Expr_TermExpr{
					TermExpr: &planpb.TermExpr{
						ColumnInfo: &planpb.ColumnInfo{
							FieldId:  pkField.FieldID,
							DataType: pkField.DataType,
						},
						Values: []*planpb.GenericValue{
							{
								Val: &planpb.GenericValue_Int64Val{
									Int64Val: 1,
								},
							},
							{
								Val: &planpb.GenericValue_Int64Val{
									Int64Val: 2,
								},
							},
							{
								Val: &planpb.GenericValue_Int64Val{
									Int64Val: 3,
								},
							},
						},
					},
				},
			},
		},
		OutputFieldIds: []int64{pkField.FieldID, 100},
	}
	planExpr, err := proto.Marshal(planNode)
	return planExpr, err
}
