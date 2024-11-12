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

package entity

import (
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

type FunctionType = schemapb.FunctionType

// provide package alias
const (
	FunctionTypeUnknown       = schemapb.FunctionType_Unknown
	FunctionTypeBM25          = schemapb.FunctionType_BM25
	FunctionTypeTextEmbedding = schemapb.FunctionType_TextEmbedding
)

type Function struct {
	Name        string
	Description string
	Type        FunctionType

	InputFieldNames  []string
	OutputFieldNames []string
	Params           map[string]string

	// ids shall be private
	id             int64
	inputFieldIDs  []int64
	outputFieldIDs []int64
}

func NewFunction() *Function {
	return &Function{
		Params: make(map[string]string),
	}
}

func (f *Function) WithName(name string) *Function {
	f.Name = name
	return f
}

func (f *Function) WithInputFields(inputFields ...string) *Function {
	f.InputFieldNames = inputFields
	return f
}

func (f *Function) WithOutputFields(outputFields ...string) *Function {
	f.OutputFieldNames = outputFields
	return f
}

func (f *Function) WithType(funcType FunctionType) *Function {
	f.Type = funcType
	return f
}

func (f *Function) WithParam(key string, value any) *Function {
	f.Params[key] = fmt.Sprintf("%v", value)
	return f
}

// ProtoMessage returns corresponding schemapb.FunctionSchema
func (f *Function) ProtoMessage() *schemapb.FunctionSchema {
	r := &schemapb.FunctionSchema{
		Name:             f.Name,
		Description:      f.Description,
		Type:             f.Type,
		InputFieldNames:  f.InputFieldNames,
		OutputFieldNames: f.OutputFieldNames,
		Params:           MapKvPairs(f.Params),
	}

	return r
}

// ReadProto parses proto Collection Schema
func (f *Function) ReadProto(p *schemapb.FunctionSchema) *Function {
	f.Name = p.GetName()
	f.Description = p.GetDescription()
	f.Type = p.GetType()

	f.InputFieldNames = p.GetInputFieldNames()
	f.OutputFieldNames = p.GetOutputFieldNames()
	f.Params = KvPairsMap(p.GetParams())

	f.id = p.GetId()
	f.inputFieldIDs = p.GetInputFieldIds()
	f.outputFieldIDs = p.GetOutputFieldIds()

	return f
}
