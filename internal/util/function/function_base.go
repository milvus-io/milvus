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

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)


type RunnerMode int

const (
	InsertMode RunnerMode = iota
	SearchMode
)


type FunctionBase struct {
	schema      *schemapb.FunctionSchema
	outputFields []*schemapb.FieldSchema
	mode RunnerMode
}

func NewBase(coll *schemapb.CollectionSchema, schema *schemapb.FunctionSchema, mode RunnerMode) (*FunctionBase, error) {
	var base FunctionBase
	base.schema = schema
	base.mode = mode
	for _, field_id := range schema.GetOutputFieldIds() {
		for _, field := range coll.GetFields() {
			if field.GetFieldID() == field_id {
				base.outputFields = append(base.outputFields, field)
				break
			}
		}
	}

	if len(base.outputFields) != len(schema.GetOutputFieldIds()) {
		return &base, fmt.Errorf("Collection [%s]'s function [%s]'s outputs mismatch schema", coll.Name, schema.Name)
	}
	return &base, nil
}

func (base *FunctionBase) GetSchema() *schemapb.FunctionSchema {
	return base.schema
}

func (base *FunctionBase) GetOutputFields() []*schemapb.FieldSchema {
	return base.outputFields
}
