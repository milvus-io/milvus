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

package function

import (
	"fmt"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type FunctionBase struct {
	schema       *schemapb.FunctionSchema
	outputFields []*schemapb.FieldSchema

	collectionName   string
	functionTypeName string
	functionName     string
	provider         string
}

func getProvider(functionSchema *schemapb.FunctionSchema) (string, error) {
	for _, param := range functionSchema.Params {
		switch strings.ToLower(param.Key) {
		case Provider:
			return strings.ToLower(param.Value), nil
		default:
		}
	}
	return "", fmt.Errorf("The text embedding service provider parameter:[%s] was not found", Provider)
}

func NewFunctionBase(coll *schemapb.CollectionSchema, fSchema *schemapb.FunctionSchema) (*FunctionBase, error) {
	var base FunctionBase
	base.schema = fSchema
	for _, fieldName := range fSchema.GetOutputFieldNames() {
		for _, field := range coll.GetFields() {
			if field.GetName() == fieldName {
				base.outputFields = append(base.outputFields, field)
				break
			}
		}
	}

	if len(base.outputFields) != len(fSchema.GetOutputFieldNames()) {
		return &base, fmt.Errorf("The collection [%s]'s information is wrong, function [%s]'s outputs does not match the schema",
			coll.Name, fSchema.Name)
	}

	provider, err := getProvider(fSchema)
	if err != nil {
		return nil, err
	}
	base.collectionName = coll.Name
	base.functionName = fSchema.Name
	base.provider = provider
	base.functionTypeName = fSchema.GetType().String()
	return &base, nil
}

func (base *FunctionBase) GetSchema() *schemapb.FunctionSchema {
	return base.schema
}

func (base *FunctionBase) GetOutputFields() []*schemapb.FieldSchema {
	return base.outputFields
}
