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

// Package native defines the segment scoring boundary without importing QueryNode.
package native

import (
	"context"

	"github.com/apache/arrow/go/v17/arrow"

	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

// Segment borrows the search task's segment and search request. The task retains
// both until all calls (including asynchronous native cleanup) complete.
// Results belong to the caller; errors must preserve their original codes.
type Segment interface {
	ComputeScores(context.Context, *arrow.Chunked, *planpb.ScoreFunction) (*arrow.Chunked, error)
}
