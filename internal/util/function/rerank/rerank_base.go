/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package rerank

import (
	"fmt"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

const (
	reranker string = "reranker"
)

// topk and group related parameters, reranker can choose to process or ignore
type searchParams struct {
	limit int64

	groupByFieldId  int64
	groupSize       int64
	strictGroupSize bool
}

type RerankBase struct {
	coll           *schemapb.CollectionSchema
	funcSchema     *schemapb.FunctionSchema
	rerankerName   string
	isSupportGroup bool

	pkType        schemapb.DataType
	inputFields   []*schemapb.FieldSchema
	inputFieldIDs []int64

	// TODO: The parameter is passed to the reranker, and the reranker decides whether to implement the parameter
	searchParams *searchParams
}

func newRerankBase(coll *schemapb.CollectionSchema, funcSchema *schemapb.FunctionSchema, rerankerName string, isSupportGroup bool, pkType schemapb.DataType) (*RerankBase, error) {
	base := RerankBase{
		coll:           coll,
		funcSchema:     funcSchema,
		rerankerName:   rerankerName,
		isSupportGroup: isSupportGroup,
		pkType:         pkType,
	}

	nameMap := lo.SliceToMap(coll.GetFields(), func(field *schemapb.FieldSchema) (string, *schemapb.FieldSchema) {
		return field.GetName(), field
	})

	if len(funcSchema.GetOutputFieldNames()) != 0 {
		return nil, fmt.Errorf("Rerank function output field names should be empty")
	}

	for _, name := range funcSchema.GetInputFieldNames() {
		if name == "" {
			return nil, fmt.Errorf("Rerank input field name cannot be empty string")
		}
		if lo.Count(funcSchema.GetInputFieldNames(), name) > 1 {
			return nil, fmt.Errorf("Each function input field should be used exactly once in the same function, input field: %s", name)
		}
		inputField, ok := nameMap[name]
		if !ok {
			return nil, fmt.Errorf("Function input field not found: %s", name)
		}
		if inputField.GetNullable() {
			return nil, fmt.Errorf("Function input field cannot be nullable: field %s", inputField.GetName())
		}
		base.inputFields = append(base.inputFields, inputField)
		base.inputFieldIDs = append(base.inputFieldIDs, inputField.FieldID)
	}
	return &base, nil
}

func (base *RerankBase) GetInputFieldNames() []string {
	return base.funcSchema.InputFieldNames
}

func (base *RerankBase) GetInputFieldIDs() []int64 {
	return base.inputFieldIDs
}

func (base *RerankBase) IsSupportGroup() bool {
	return base.isSupportGroup
}

func (base *RerankBase) GetRankName() string {
	return base.rerankerName
}
