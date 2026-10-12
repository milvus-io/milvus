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
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

type FunctionType = schemapb.FunctionType

// provide package alias
const (
	FunctionTypeUnknown       = schemapb.FunctionType_Unknown
	FunctionTypeBM25          = schemapb.FunctionType_BM25
	FunctionTypeMinHash       = schemapb.FunctionType_MinHash
	FunctionTypeTextEmbedding = schemapb.FunctionType_TextEmbedding
	FunctionTypeRerank        = schemapb.FunctionType_Rerank
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
	f.Params[key] = paramValueToString(value)
	return f
}

// paramValueToString renders a Function/FunctionScore param value on the wire:
// slices are JSON-encoded, everything else uses its default string form.
func paramValueToString(value any) string {
	valueType := reflect.TypeOf(value)
	if valueType == nil {
		return "null"
	}
	if valueType.Kind() == reflect.Slice {
		if jsonBytes, err := json.Marshal(value); err == nil {
			return string(jsonBytes)
		}
		return fmt.Sprintf("%v", value)
	}
	return fmt.Sprintf("%v", value)
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

// FunctionScore models the search-time FunctionScore message: a set of scoring
// Functions (e.g. boost rankers) plus score-option params such as boost_mode
// and function_mode.
type FunctionScore struct {
	Functions []*Function
	Params    map[string]string
}

func NewFunctionScore() *FunctionScore {
	return &FunctionScore{
		Params: make(map[string]string),
	}
}

func (fs *FunctionScore) AddFunction(f *Function) *FunctionScore {
	fs.Functions = append(fs.Functions, f)
	return fs
}

func (fs *FunctionScore) WithParam(key string, value any) *FunctionScore {
	fs.Params[key] = paramValueToString(value)
	return fs
}

// Clone returns a deep copy of fs: functions and params are copied so the
// returned score does not share mutable state with the source.
func (fs *FunctionScore) Clone() *FunctionScore {
	nf := NewFunctionScore()
	for _, f := range fs.Functions {
		nf.AddFunction(f.Clone())
	}
	for k, v := range fs.Params {
		nf.Params[k] = v
	}
	return nf
}

// Clone returns a deep copy of f.
func (f *Function) Clone() *Function {
	nf := &Function{
		Name:             f.Name,
		Description:      f.Description,
		Type:             f.Type,
		InputFieldNames:  append([]string(nil), f.InputFieldNames...),
		OutputFieldNames: append([]string(nil), f.OutputFieldNames...),
		Params:           make(map[string]string, len(f.Params)),
		id:               f.id,
		inputFieldIDs:    append([]int64(nil), f.inputFieldIDs...),
		outputFieldIDs:   append([]int64(nil), f.outputFieldIDs...),
	}
	for k, v := range f.Params {
		nf.Params[k] = v
	}
	return nf
}

// ProtoMessage returns corresponding schemapb.FunctionScore
func (fs *FunctionScore) ProtoMessage() *schemapb.FunctionScore {
	r := &schemapb.FunctionScore{
		Params: MapKvPairs(fs.Params),
	}
	for _, f := range fs.Functions {
		r.Functions = append(r.Functions, f.ProtoMessage())
	}
	return r
}

// ReadProto parses proto FunctionScore
func (fs *FunctionScore) ReadProto(p *schemapb.FunctionScore) *FunctionScore {
	fs.Functions = lo.Map(p.GetFunctions(), func(fn *schemapb.FunctionSchema, _ int) *Function {
		return NewFunction().ReadProto(fn)
	})
	fs.Params = KvPairsMap(p.GetParams())
	return fs
}
