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

package chain

import (
	"encoding/json"
	"fmt"

	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// =============================================================================
// JSON Representation Types (for deserialization)
// =============================================================================

// ChainJSON is the JSON representation of a FuncChain for serialization/deserialization.
type ChainJSON struct {
	Name      string         `json:"name,omitempty"`
	Stage     string         `json:"stage"`
	Operators []OperatorJSON `json:"operators"`
}

// OperatorJSON is the JSON representation of an Operator for serialization/deserialization.
type OperatorJSON struct {
	Type     string                 `json:"type"`
	Params   map[string]interface{} `json:"params,omitempty"`
	Function *FunctionJSON          `json:"function,omitempty"` // for map operator
	Inputs   []string               `json:"inputs,omitempty"`   // input column names
	Outputs  []string               `json:"outputs,omitempty"`  // output column names
}

// FunctionJSON is the JSON representation of a FunctionExpr for serialization/deserialization.
type FunctionJSON struct {
	Name   string                 `json:"name"`
	Params map[string]interface{} `json:"params"`
}

// =============================================================================
// Internal Representation Types (no JSON tags)
// =============================================================================

// ChainRepr is the internal representation of a FuncChain (no JSON tags).
type ChainRepr struct {
	Name      string
	Stage     string
	Operators []OperatorRepr
}

// OperatorRepr is the internal representation of an Operator (no JSON tags).
// It uses a unified Params map instead of specific fields.
type OperatorRepr struct {
	Type     string
	Params   map[string]interface{}
	Function *FunctionRepr // for map operator
	Inputs   []string      // input column names
	Outputs  []string      // output column names
}

// FunctionRepr is the internal representation of a FunctionExpr (no JSON tags).
type FunctionRepr struct {
	Name   string
	Params map[string]interface{}
}

// =============================================================================
// Conversion Functions
// =============================================================================

// functionJSONToRepr converts FunctionJSON to FunctionRepr.
func functionJSONToRepr(json *FunctionJSON) *FunctionRepr {
	if json == nil {
		return nil
	}
	return &FunctionRepr{
		Name:   json.Name,
		Params: json.Params,
	}
}

// operatorJSONToRepr converts OperatorJSON to OperatorRepr.
// It infers Inputs/Outputs based on operator type and parameters.
func operatorJSONToRepr(json *OperatorJSON) (*OperatorRepr, error) {
	if json == nil {
		return nil, merr.WrapErrServiceInternal("OperatorJSON is nil")
	}

	repr := &OperatorRepr{
		Type:     json.Type,
		Params:   make(map[string]interface{}),
		Function: functionJSONToRepr(json.Function),
		Inputs:   json.Inputs,
		Outputs:  json.Outputs,
	}

	// Copy params
	if json.Params != nil {
		for k, v := range json.Params {
			repr.Params[k] = v
		}
	}
	return repr, nil
}

// chainJSONToRepr converts ChainJSON to ChainRepr.
func chainJSONToRepr(json *ChainJSON) (*ChainRepr, error) {
	if json == nil {
		return nil, merr.WrapErrServiceInternal("ChainJSON is nil")
	}

	repr := &ChainRepr{
		Name:      json.Name,
		Stage:     json.Stage,
		Operators: make([]OperatorRepr, 0, len(json.Operators)),
	}

	for i, opJSON := range json.Operators {
		opRepr, err := operatorJSONToRepr(&opJSON)
		if err != nil {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("operator[%d]: %v", i, err))
		}
		repr.Operators = append(repr.Operators, *opRepr)
	}

	return repr, nil
}

// =============================================================================
// Parsing Functions
// =============================================================================

// ParseFuncChainRepr parses a JSON string and creates a FuncChain.
// alloc must not be nil.
func ParseFuncChainRepr(jsonStr string, alloc memory.Allocator) (*FuncChain, error) {
	var chainJSON ChainJSON
	if err := json.Unmarshal([]byte(jsonStr), &chainJSON); err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("failed to parse JSON: %v", err)
	}

	repr, err := chainJSONToRepr(&chainJSON)
	if err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("failed to convert JSON to Repr: %v", err)
	}

	return funcChainFromRepr(repr, alloc)
}

// funcChainFromRepr creates a FuncChain from a ChainRepr.
// Stage is required and validates that all functions support the stage.
// alloc must not be nil.
func funcChainFromRepr(repr *ChainRepr, alloc memory.Allocator) (*FuncChain, error) {
	if alloc == nil {
		return nil, merr.WrapErrServiceInternal("alloc is nil")
	}

	if repr.Stage == "" {
		return nil, merr.WrapErrParameterInvalidMsg("stage is required")
	}

	chain := NewFuncChainWithAllocator(alloc)
	if repr.Name != "" {
		chain.SetName(repr.Name)
	}
	chain.SetStage(repr.Stage)

	for i, opRepr := range repr.Operators {
		op, err := operatorFromRepr(&opRepr)
		if err != nil {
			return nil, merr.WrapErrServiceInternal(fmt.Sprintf("operator[%d]: %v", i, err))
		}
		chain.Add(op)
	}

	// Validate the chain (including stage compatibility)
	if err := chain.Validate(); err != nil {
		return nil, err
	}

	return chain, nil
}

// operatorFromRepr creates an Operator from an OperatorRepr.
func operatorFromRepr(repr *OperatorRepr) (Operator, error) {
	factory, ok := GetOperatorFactory(repr.Type)
	if !ok {
		return nil, merr.WrapErrParameterInvalidMsg("unknown operator type: %s", repr.Type)
	}
	return factory(repr)
}

// FunctionFromRepr creates a FunctionExpr from a FunctionRepr.
func FunctionFromRepr(repr *FunctionRepr) (types.FunctionExpr, error) {
	if repr.Name == "" {
		return nil, merr.WrapErrParameterInvalidMsg("function name is required")
	}

	return types.CreateFunction(repr.Name, repr.Params)
}
