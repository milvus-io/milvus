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

// Package native defines the segment-native Grep execution contract without
// depending on chain or the Go segcore adapter.
package native

import (
	"context"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus/internal/util/function/grep"
)

type CompileLimits struct {
	MaxRegexBytes, MaxDFACacheBytes uint64
}

type ExecutionLimits struct {
	BatchSize      int64 // Candidate rows per read batch; independent of the matching limit.
	MaxResultBytes uint64
}

// ScanRequest is call-local data. The caller snapshots all service limits and
// supplies a private pattern slice; the operator retains no native handles.
type ScanRequest struct {
	FieldID         int64
	Spec            grep.Spec
	Limit           int64
	CompileLimits   CompileLimits
	ExecutionLimits ExecutionLimits
}

// Segment is borrowed and kept pinned by the query task through ScanGrep.
// The native adapter compiles one plan per call, creates a local session, reads
// bounded text batches, skips NULL and stops at Limit complete matching records.
// Plan/session/cancel state and counters belong to that native call and are
// released on success, error or cancellation before it returns. They are never
// shared between segments or retained by the Go operator. Returned details own
// their buffers independently of native temporary results. No partial success.
// The segcore adapter delegates to the independent C++ Grep runner; Segment
// itself has no Grep-specific method.
type Segment interface {
	ScanGrep(context.Context, *array.Int64, ScanRequest) (*ScanResult, error)
}

// ScanResult owns Details even on failure. Positions are increasing immediate-
// input positions, not physical offsets. Existing Arrow ownership handles the
// exported buffers; there is no operator/chain Close interface.
type ScanResult struct {
	Positions []int64
	Details   arrow.Array
}

func (r *ScanResult) Release() {
	if r != nil && r.Details != nil {
		r.Details.Release()
		r.Details = nil
	}
}

// GrepDetailsType describes exact rg line content and content-relative UTF-8
// byte spans. These unsigned coordinates must never be converted through float.
func GrepDetailsType() arrow.DataType {
	span := arrow.StructOf(
		arrow.Field{Name: "start", Type: arrow.PrimitiveTypes.Uint64},
		arrow.Field{Name: "end", Type: arrow.PrimitiveTypes.Uint64},
	)
	line := arrow.StructOf(
		arrow.Field{Name: "content", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "spans", Type: arrow.ListOfField(arrow.Field{Name: "item", Type: span})},
	)
	return arrow.ListOfField(arrow.Field{Name: "item", Type: line})
}
