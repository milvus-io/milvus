// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package types

import "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"

// QueryContext is an immutable snapshot of the search inputs. Each entry is an
// ANN input (one for ordinary Search, one per subrequest for HybridSearch), and
// OriginalQueries holds the pre-PRE request
// payload. Queries is the stage-entry payload for PRE, and the final PRE output
// before native embedding for L2/POST.
// Adapters own the protos. Expressions must neither mutate nor retain them.
// QueryOrigins[ann][i] identifies the original query of prepared query i. A nil
// mapping means identity. ANN branches can have different prepared counts.
// During PRE, Queries remains the stage-entry snapshot; RowQueryOrigins follows
// the current operator's rows independently of that snapshot.
type QueryContext struct {
	OriginalQueries []*commonpb.PlaceholderValue
	Queries         []*commonpb.PlaceholderValue
	QueryOrigins    [][]int64
	RowQueryOrigins []int64
}

// WithQueryContext returns a per-execution context without mutating the caller.
func (ctx *FuncContext) WithQueryContext(queries *QueryContext) *FuncContext {
	result := *ctx
	result.queries = queries
	return &result
}

func (ctx *FuncContext) QueryContext() *QueryContext { return ctx.queries }
