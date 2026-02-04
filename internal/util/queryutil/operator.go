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

package queryutil

import (
	"context"

	"go.opentelemetry.io/otel/trace"
)

// Well-known operator names used in pipeline construction.
const (
	OpReduceByPK     = "reduce_by_pk"
	OpReduceByPKTS   = "reduce_by_pk_ts"
	OpReduceByGroups = "reduce_by_groups"
	OpOrderBy        = "orderby"
	OpSlice          = "slice"
	OpRemap          = "remap"
)

// Operator is the interface for query pipeline operators.
// Each operator carries its own parameters and is self-contained.
// Operators can be used at any level: proxy, delegator, or querynode worker.
type Operator interface {
	// Run executes the operator with the given inputs.
	// Inputs and outputs are type-erased to allow flexible composition.
	Run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error)

	// Name returns the operator name for logging/debugging.
	Name() string
}

// LambdaOperator wraps a function as an operator for inline transformations.
type LambdaOperator struct {
	name string
	fn   func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error)
}

// NewLambdaOperator creates an operator from a function.
func NewLambdaOperator(name string, fn func(ctx context.Context, span trace.Span, inputs ...any) ([]any, error)) *LambdaOperator {
	return &LambdaOperator{name: name, fn: fn}
}

func (op *LambdaOperator) Run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
	return op.fn(ctx, span, inputs...)
}

func (op *LambdaOperator) Name() string {
	return op.name
}
