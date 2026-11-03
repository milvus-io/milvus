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

package requestutil

import (
	"context"
	"sync/atomic"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type conditionalWriteKey struct{}

// ConditionalWriteState arbitrates the first WAL submission against cancellation.
// Its zero value allows submission; once canceled before submission it stays closed.
type ConditionalWriteState struct {
	phase atomic.Uint32
}

const (
	writePending uint32 = iota
	writeSubmitted
	writeCanceled
)

func WithConditionalWriteState(ctx context.Context) (context.Context, *ConditionalWriteState) {
	if state := ConditionalWriteStateFromContext(ctx); state != nil {
		return ctx, state
	}
	state := &ConditionalWriteState{}
	return context.WithValue(ctx, conditionalWriteKey{}, state), state
}

func ConditionalWriteStateFromContext(ctx context.Context) *ConditionalWriteState {
	state, _ := ctx.Value(conditionalWriteKey{}).(*ConditionalWriteState)
	return state
}

func (s *ConditionalWriteState) TryAppend() bool {
	return s.phase.CompareAndSwap(writePending, writeSubmitted) || s.phase.Load() == writeSubmitted
}

// Cancel closes the pre-submission gate or reports that submission already won.
func (s *ConditionalWriteState) Cancel(err error) error {
	if s.phase.CompareAndSwap(writePending, writeCanceled) || s.phase.Load() == writeCanceled {
		return err
	}
	if merr.Code(err) == merr.Code(merr.ErrCollectionConditionalUpsertExecution) {
		return err
	}
	return merr.WrapErrCollectionConditionalUpsertExecutionErr(err, "conditional upsert may have partially committed; automatic replay is unsafe")
}
