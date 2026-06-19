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

// Package types provides core interfaces and types for the function chain system.
// This package is designed to be imported by both the chain package and its sub-packages
// (like expr) to avoid circular dependencies.
package types

import (
	"context"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

// =============================================================================
// Stage Constants
// =============================================================================

const (
	StageIngestion   = "ingestion"    // insert/upsert/import stage
	StageL2Rerank    = "L2_rerank"    // proxy rerank stage
	StageL1Rerank    = "L1_rerank"    // QN/SN rerank stage
	StageL0Rerank    = "L0_rerank"    // segment rerank stage
	StagePreProcess  = "pre_process"  // pre-processing stage
	StagePostProcess = "post_process" // post-processing stage
)

// =============================================================================
// FuncContext
// =============================================================================

// FuncContext is the execution context for function chains.
// The pool is immutable after creation.
type FuncContext struct {
	ctx   context.Context // context for cancellation and timeouts
	pool  memory.Allocator
	stage string // execution stage for filtering operators
}

// NewFuncContext creates a new FuncContext with the given allocator.
// If pool is nil, uses memory.DefaultAllocator.
func NewFuncContext(pool memory.Allocator) *FuncContext {
	return NewFuncContextWithContext(context.Background(), pool)
}

// NewFuncContextWithContext creates a new FuncContext with context and allocator.
// If pool is nil, uses memory.DefaultAllocator.
func NewFuncContextWithContext(ctx context.Context, pool memory.Allocator) *FuncContext {
	if pool == nil {
		pool = memory.DefaultAllocator
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return &FuncContext{ctx: ctx, pool: pool}
}

// NewFuncContextWithStage creates a new FuncContext with allocator and stage.
// If pool is nil, uses memory.DefaultAllocator.
func NewFuncContextWithStage(pool memory.Allocator, stage string) *FuncContext {
	ctx := NewFuncContext(pool)
	ctx.stage = stage
	return ctx
}

// NewFuncContextFull creates a new FuncContext with all parameters.
// If pool is nil, uses memory.DefaultAllocator.
func NewFuncContextFull(ctx context.Context, pool memory.Allocator, stage string) *FuncContext {
	fctx := NewFuncContextWithContext(ctx, pool)
	fctx.stage = stage
	return fctx
}

// Context returns the context for cancellation and timeouts.
func (ctx *FuncContext) Context() context.Context {
	return ctx.ctx
}

// Pool returns the memory allocator.
func (ctx *FuncContext) Pool() memory.Allocator {
	return ctx.pool
}

// Stage returns the execution stage.
func (ctx *FuncContext) Stage() string {
	return ctx.stage
}

// =============================================================================
// Interfaces
// =============================================================================

// FunctionExpr is a function expression interface that processes columns.
// Functions are stateless and only focus on computation logic.
// Column mapping is handled by the Operator layer (MapOp).
type FunctionExpr interface {
	// Name returns the function name (e.g., "decay", "score_combine").
	Name() string

	// OutputDataTypes returns the data types of output columns.
	// The length determines how many output columns the function produces.
	// Returns nil for dynamic output types (e.g., UDF functions where output
	// types are determined at execution time). When nil is returned, the
	// Operator layer skips compile-time validation and relies on Execute()
	// results for type information.
	OutputDataTypes() []arrow.DataType

	// Execute executes the function on input columns and returns output columns.
	// inputs: ChunkedArrays passed from the Operator
	// returns: ChunkedArrays, one for each OutputDataTypes() (or dynamically determined)
	Execute(ctx *FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error)

	// IsRunnable checks if the function can run in the given stage.
	IsRunnable(stage string) bool
}

// FunctionFactory is a factory function that creates a FunctionExpr from parameters.
type FunctionFactory func(params map[string]interface{}) (FunctionExpr, error)
