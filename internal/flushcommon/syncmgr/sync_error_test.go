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
		want SyncErrorDecision
	}{
		{"nil", nil, SyncRetry},
		{"plain error defaults to retry", errors.New("mock transient"), SyncRetry},
		{"canceled", context.Canceled, SyncCanceled},
		{"deadline", context.DeadlineExceeded, SyncCanceled},
		{"wrapped canceled", errors.Wrap(context.Canceled, "sync"), SyncCanceled},
		{"data integrity", merr.WrapErrDataIntegrityMsg("row count mismatch"), SyncTerminal},
		{"unrecoverable marker", retry.Unrecoverable(errors.New("loon permanent")), SyncTerminal},
		{
			// InputError-typed errors are deterministic: the request content
			// itself forces the branch, so retrying re-derives the same
			// failure. Without this the RetryErr predicate in ioRetryOptions
			// overrides the retry framework's own InputError short-circuit
			// and the task spins forever.
			"input error type",
			merr.WrapErrParameterInvalidMsg("bad dim"),
			SyncTerminal,
		},
		{
			"explicitly marked input error",
			merr.WrapErrAsInputError(errors.New("schema rejects row")),
			SyncTerminal,
		},
		{
			// ErrServiceInternal stays retryable: on the growing-source path
			// it means "source not ready yet", which the next round resolves.
			"service internal stays retryable",
			merr.WrapErrServiceInternalMsg("growing source behind target"),
			SyncRetry,
		},
		{
			"layout mismatch by message",
			errors.New("Invalid: Column count mismatch at index 0"),
			SyncTerminal,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, ClassifySyncError(tc.err))
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
	}, ioRetryOptions(DefaultIORetryAttempts)...)
	assert.Error(t, err)
	assert.Equal(t, 1, attempts)

	attempts = 0
	err = retry.Do(context.Background(), func() error {
		attempts++
		return errors.New("transient blip")
	}, ioRetryOptions(DefaultIORetryAttempts)...)
	assert.Error(t, err)
	assert.Equal(t, int(DefaultIORetryAttempts), attempts)
}
