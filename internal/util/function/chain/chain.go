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
	"bytes"
	"context"
	"fmt"

	"github.com/apache/arrow/go/v17/arrow/memory"

	// Register built-in function expressions
	_ "github.com/milvus-io/milvus/internal/util/function/chain/expr"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
)

// Operator is the operator interface.
type Operator interface {
	// Name returns the operator name.
	Name() string

	// Execute executes the operator.
	Execute(ctx *types.FuncContext, input *DataFrame) (*DataFrame, error)

	// Inputs returns the input column names.
	Inputs() []string

	// Outputs returns the output column names.
	Outputs() []string

	// String returns a string representation of the operator.
	String() string
}

// =============================================================================
// FuncChain
// =============================================================================

// FuncChain is a function chain that contains a list of operators.
type FuncChain struct {
	name       string
	stage      string // execution stage for validating function compatibility
	operators  []Operator
	alloc      memory.Allocator
	buildError error // stores error from fluent API calls
}

// NewFuncChainWithAllocator creates a new FuncChain with the given allocator.
func NewFuncChainWithAllocator(alloc memory.Allocator) *FuncChain {
	if alloc == nil {
		alloc = memory.DefaultAllocator
	}
	return &FuncChain{
		operators: make([]Operator, 0),
		alloc:     alloc,
	}
}

// SetName sets the name of the FuncChain.
func (fc *FuncChain) SetName(name string) *FuncChain {
	fc.name = name
	return fc
}

// SetStage sets the execution stage of the FuncChain.
// When stage is set, Validate() will check that all functions support this stage.
func (fc *FuncChain) SetStage(stage string) *FuncChain {
	fc.stage = stage
	return fc
}

// Stage returns the execution stage of the FuncChain.
func (fc *FuncChain) Stage() string {
	return fc.stage
}

// Add adds an operator to the chain.
func (fc *FuncChain) Add(op Operator) *FuncChain {
	fc.operators = append(fc.operators, op)
	return fc
}

// addWithError adds an operator to the chain, recording any error for later.
// This is used by fluent API methods to defer error handling to Execute/Validate.
func (fc *FuncChain) addWithError(op Operator, err error) *FuncChain {
	if err != nil && fc.buildError == nil {
		fc.buildError = err
	}
	if op != nil {
		fc.operators = append(fc.operators, op)
	}
	return fc
}

// Validate validates the chain configuration before execution.
// It checks for build errors, validates stage is set, and validates each operator.
func (fc *FuncChain) Validate() error {
	// Check for errors accumulated during fluent API calls
	if fc.buildError != nil {
		return fmt.Errorf("chain build error: %w", fc.buildError)
	}

	// Stage is required
	if fc.stage == "" {
		return fmt.Errorf("chain stage is required")
	}

	// Validate all operators including stage compatibility
	return fc.validateOperators(fc.stage)
}

// validateOperators is an internal helper that validates all operators in a single pass.
// It checks that operators are not nil, have valid functions, and support the given stage.
func (fc *FuncChain) validateOperators(stage string) error {
	for i, op := range fc.operators {
		if op == nil {
			return fmt.Errorf("operator[%d] is nil", i)
		}

		// Validate MapOp
		if mapOp, ok := op.(*MapOp); ok {
			if mapOp.function == nil {
				return fmt.Errorf("operator[%d] MapOp has nil function", i)
			}
			if !mapOp.function.IsRunnable(stage) {
				return fmt.Errorf("operator[%d] function %q does not support stage %q",
					i, mapOp.function.Name(), stage)
			}
		}

		// Validate FilterOp
		if filterOp, ok := op.(*FilterOp); ok {
			if filterOp.function == nil {
				return fmt.Errorf("operator[%d] FilterOp has nil function", i)
			}
			if !filterOp.function.IsRunnable(stage) {
				return fmt.Errorf("operator[%d] filter function %q does not support stage %q",
					i, filterOp.function.Name(), stage)
			}
		}
	}
	return nil
}

// Execute executes the chain with a single input.
func (fc *FuncChain) Execute(input *DataFrame) (*DataFrame, error) {
	return fc.ExecuteWithContext(context.Background(), input)
}

// ExecuteWithContext executes the chain with context for cancellation support.
// Supports multiple inputs when the first operator is MergeOp.
func (fc *FuncChain) ExecuteWithContext(ctx context.Context, inputs ...*DataFrame) (*DataFrame, error) {
	if len(inputs) == 0 {
		return nil, fmt.Errorf("at least one input is required")
	}

	// Validate chain before execution
	if err := fc.Validate(); err != nil {
		return nil, err
	}

	funcCtx := types.NewFuncContextFull(ctx, fc.alloc, fc.stage)

	var result *DataFrame
	startIdx := 0

	// If first operator is MergeOp, handle multiple inputs
	if len(fc.operators) > 0 {
		if mergeOp, ok := fc.operators[0].(*MergeOp); ok {
			var err error
			result, err = mergeOp.ExecuteMulti(funcCtx, inputs)
			if err != nil {
				return nil, fmt.Errorf("%s failed: %w", mergeOp.Name(), err)
			}
			startIdx = 1
		} else {
			result = inputs[0]
		}
	} else {
		result = inputs[0]
	}

	// Process remaining operators
	for i := startIdx; i < len(fc.operators); i++ {
		op := fc.operators[i]

		// Check for context cancellation before each operator
		select {
		case <-ctx.Done():
			fc.releaseIfOwned(result, inputs)
			return nil, ctx.Err()
		default:
		}

		newResult, err := op.Execute(funcCtx, result)
		if err != nil {
			fc.releaseIfOwned(result, inputs)
			return nil, fmt.Errorf("%s failed: %w", op.Name(), err)
		}

		// Release intermediate results (but not the original inputs)
		if result != newResult {
			fc.releaseIfOwned(result, inputs)
		}
		result = newResult
	}

	return result, nil
}

// releaseIfOwned releases df if it's not one of the original inputs.
func (fc *FuncChain) releaseIfOwned(df *DataFrame, inputs []*DataFrame) {
	if df == nil {
		return
	}
	for _, input := range inputs {
		if df == input {
			return
		}
	}
	df.Release()
}

// Map applies a function to the DataFrame with specified column mappings.
// inputCols: column names to read from DataFrame and pass to the function
// outputCols: column names to write the function output to
// Errors are deferred until Execute() or Validate() is called.
func (fc *FuncChain) Map(fn types.FunctionExpr, inputCols, outputCols []string) *FuncChain {
	op, err := NewMapOp(fn, inputCols, outputCols)
	return fc.addWithError(op, err)
}

// MapWithError is like Map but returns an error immediately instead of deferring it.
// Use this when you want immediate error feedback rather than fluent chaining.
func (fc *FuncChain) MapWithError(fn types.FunctionExpr, inputCols, outputCols []string) (*FuncChain, error) {
	op, err := NewMapOp(fn, inputCols, outputCols)
	if err != nil {
		return fc, err
	}
	return fc.Add(op), nil
}

// Filter filters the DataFrame based on the boolean result of a FunctionExpr.
// The function must return exactly one boolean column.
func (fc *FuncChain) Filter(fn types.FunctionExpr, inputCols []string) *FuncChain {
	op, err := NewFilterOp(fn, inputCols)
	return fc.addWithError(op, err)
}

// Select selects specific columns from the DataFrame.
func (fc *FuncChain) Select(columns ...string) *FuncChain {
	return fc.Add(NewSelectOp(columns))
}

// Sort sorts the DataFrame by a column.
func (fc *FuncChain) Sort(column string, desc bool) *FuncChain {
	return fc.Add(NewSortOp(column, desc))
}

// Limit limits the number of rows in the DataFrame.
func (fc *FuncChain) Limit(limit int64) *FuncChain {
	if limit <= 0 {
		return fc.addWithError(nil, fmt.Errorf("limit must be positive, got %d", limit))
	}
	return fc.Add(NewLimitOp(limit, 0))
}

// LimitWithOffset limits the number of rows with an offset.
func (fc *FuncChain) LimitWithOffset(limit, offset int64) *FuncChain {
	if limit <= 0 {
		return fc.addWithError(nil, fmt.Errorf("limit must be positive, got %d", limit))
	}
	if offset < 0 {
		return fc.addWithError(nil, fmt.Errorf("offset must be non-negative, got %d", offset))
	}
	return fc.Add(NewLimitOp(limit, offset))
}

// Merge adds a MergeOp to merge multiple DataFrames.
// This should be the first operator in the chain when handling multiple inputs.
func (fc *FuncChain) Merge(strategy MergeStrategy, opts ...MergeOption) *FuncChain {
	return fc.Add(NewMergeOp(strategy, opts...))
}

// String returns a string representation of the FuncChain.
func (fc *FuncChain) String() string {
	buf := bytes.NewBufferString(fmt.Sprintf("FuncChain: %s\n", fc.name))
	for i, op := range fc.operators {
		fmt.Fprintf(buf, "  [%d] %s: %v -> %v\n", i, op.Name(), op.Inputs(), op.Outputs())
	}
	return buf.String()
}
