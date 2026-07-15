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

package storage

import (
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// wrapLocalIoError must classify permanent local filesystem errors onto the denylist
// (KeyNotFound / PermissionDenied) so the import retry wrappers fail fast on them,
// while unknown errors stay ErrIoFailed (retryable by the denylist wrappers).
func TestWrapLocalIoError(t *testing.T) {
	assert.NoError(t, wrapLocalIoError("f", nil))

	// os.Stat/os.Open wrap the syscall errno in *PathError; errors.Is unwraps to the
	// sentinel, matching what production emits.
	notFound := &os.PathError{Op: "stat", Path: "f", Err: os.ErrNotExist}
	err := wrapLocalIoError("f", notFound)
	assert.True(t, errors.Is(err, merr.ErrIoKeyNotFound))
	assert.True(t, merr.IsNonRetryableErr(err), "not-found must be on the denylist")

	permission := &os.PathError{Op: "open", Path: "f", Err: os.ErrPermission}
	err = wrapLocalIoError("f", permission)
	assert.True(t, errors.Is(err, merr.ErrIoPermissionDenied))
	assert.True(t, merr.IsNonRetryableErr(err), "permission denied must be on the denylist (fail fast, no retry)")

	other := fmt.Errorf("disk gremlins")
	err = wrapLocalIoError("f", other)
	assert.True(t, errors.Is(err, merr.ErrIoFailed))
	assert.False(t, merr.IsNonRetryableErr(err), "unknown errors stay ErrIoFailed (retryable by the denylist wrappers)")
}
