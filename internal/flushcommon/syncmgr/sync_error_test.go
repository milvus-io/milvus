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

package syncmgr

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

func TestClassifySyncError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		// canceledCtx marks the cases that only mean "canceled" when the
		// CALLER's context is the one that went away.
		canceledCtx bool
		want        SyncErrorDecision
	}{
		{name: "nil", want: SyncRetry},
		{name: "plain error defaults to retry", err: errors.New("mock transient"), want: SyncRetry},
		{name: "canceled", err: context.Canceled, want: SyncCanceled},
		{name: "caller deadline expired", err: context.DeadlineExceeded, canceledCtx: true, want: SyncCanceled},
		{name: "wrapped canceled", err: errors.Wrap(context.Canceled, "sync"), want: SyncCanceled},
		{
			// The ID allocator wraps its own 5s deadline around every AllocOne,
			// so a briefly slow coordinator surfaces as DeadlineExceeded from
			// deep inside a task whose caller is healthy. Classifying that as
			// canceled dropped the payload, removed the entry, scheduled no
			// retry and raised no fatal — a silent stall until restart.
			name: "inner deadline with a live ctx is transient",
			err:  context.DeadlineExceeded,
			want: SyncRetry,
		},
		{name: "data integrity", err: merr.WrapErrDataIntegrityMsg("row count mismatch"), want: SyncTerminal},
		{name: "unrecoverable marker", err: retry.Unrecoverable(errors.New("loon permanent")), want: SyncTerminal},
		{
			// InputError-typed errors are deterministic: the request content
			// itself forces the branch, so retrying re-derives the same
			// failure. Without this the RetryErr predicate in ioRetryOptions
			// overrides the retry framework's own InputError short-circuit
			// and the task spins forever.
			name: "input error type",
			err:  merr.WrapErrParameterInvalidMsg("bad dim"),
			want: SyncTerminal,
		},
		{
			name: "explicitly marked input error",
			err:  merr.WrapErrAsInputError(errors.New("schema rejects row")),
			want: SyncTerminal,
		},
		{
			// ErrServiceInternal stays retryable: on the growing-source path
			// it means "source not ready yet", which the next round resolves.
			name: "service internal stays retryable",
			err:  merr.WrapErrServiceInternalMsg("growing source behind target"),
			want: SyncRetry,
		},
		{
			name: "layout mismatch by message",
			err:  errors.New("Invalid: Column count mismatch at index 0"),
			want: SyncTerminal,
		},
		{
			name: "column group size mismatch by message",
			err:  errors.New("Invalid: Column group size mismatch: expected 3 but got 2"),
			want: SyncTerminal,
		},
		{
			// DataCoord says the target is gone; without a terminal decision the
			// task would be re-driven on every timetick forever, pinning the
			// channel checkpoint behind a segment that no longer exists.
			name: "segment not found",
			err:  merr.WrapErrSegmentNotFound(1),
			want: SyncTerminal,
		},
		{
			name: "channel not found",
			err:  merr.WrapErrChannelNotFound("ch"),
			want: SyncTerminal,
		},
		{
			// A merr sentinel marked non-retriable but NOT typed InputError and
			// NOT wrapped with retry.Unrecoverable — the dedicated
			// merr.IsNonRetryableErr branch is what must catch it.
			name: "merr non-retryable branch",
			err:  merr.ErrIoPermissionDenied,
			want: SyncTerminal,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.canceledCtx {
				cancel()
			}
			assert.Equal(t, tc.want, ClassifySyncError(ctx, tc.err))
		})
	}
}

// TestIORetryOptionsStopOnTerminal proves the inner writer retry gives up
// immediately on a deterministic error instead of burning its budget.
func TestIORetryOptionsStopOnTerminal(t *testing.T) {
	attempts := 0
	err := retry.Do(context.Background(), func() error {
		attempts++
		return merr.WrapErrParameterInvalidMsg("deterministic")
	}, ioRetryOptions(context.Background(), defaultIORetryPolicy())...)
	assert.Error(t, err)
	assert.Equal(t, 1, attempts)

	attempts = 0
	err = retry.Do(context.Background(), func() error {
		attempts++
		return errors.New("transient blip")
	}, ioRetryOptions(context.Background(), defaultIORetryPolicy())...)
	assert.Error(t, err)
	assert.Equal(t, int(DefaultIORetryAttempts), attempts)
}
