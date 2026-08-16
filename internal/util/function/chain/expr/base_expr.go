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

package expr

import (
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// =============================================================================
// BaseExpr
// =============================================================================

// BaseExpr is a base struct that can be embedded in expression implementations.
// It provides common functionality for all expression types.
type BaseExpr struct {
	name          string               // function name
	supportStages typeutil.Set[string] // supported stages
}

// NewBaseExpr creates a new BaseExpr with the given function name.
func NewBaseExpr(name string, supportStages []string) *BaseExpr {
	return &BaseExpr{
		name:          name,
		supportStages: typeutil.NewSet(supportStages...),
	}
}

// Name returns the function name.
func (b *BaseExpr) Name() string {
	return b.name
}

// IsRunnable returns whether the function is runnable in the given stage.
func (b *BaseExpr) IsRunnable(stage string) bool {
	if b.supportStages == nil || b.supportStages.Len() == 0 {
		return true
	}
	return b.supportStages.Contain(stage)
}

func (b *BaseExpr) ValidateArgs(args []*schemapb.FunctionChainExprArg) error {
	for i, arg := range args {
		if arg == nil {
			return merr.WrapErrParameterInvalidMsg("%s: expr arg[%d] is nil", b.name, i)
		}
		if _, ok := arg.GetArg().(*schemapb.FunctionChainExprArg_Literal); ok {
			return merr.WrapErrParameterInvalidMsg("%s: literal expr arg[%d] is not supported", b.name, i)
		}
	}
	return nil
}

// =============================================================================
// Numeric Value Extraction Functions
// =============================================================================

// GetNumericValue extracts a float64 value from a numeric Arrow array at the given index.
// This function supports Int8, Int16, Int32, Int64, Float32, and Float64 types.
func GetNumericValue(arr arrow.Array, idx int) (float64, error) {
	switch a := arr.(type) {
	case *array.Int8:
		return float64(a.Value(idx)), nil
	case *array.Int16:
		return float64(a.Value(idx)), nil
	case *array.Int32:
		return float64(a.Value(idx)), nil
	case *array.Int64:
		return float64(a.Value(idx)), nil
	case *array.Float32:
		return float64(a.Value(idx)), nil
	case *array.Float64:
		return a.Value(idx), nil
	default:
		return 0, merr.WrapErrParameterInvalidMsg("unsupported input column type %T, expected numeric type", arr)
	}
}
