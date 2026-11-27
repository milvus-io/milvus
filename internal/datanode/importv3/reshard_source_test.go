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

package importv3

import (
	"fmt"
	"io"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// TestSourceReadErrClassifiesTruncation pins that a source read that ends inside
// a declared value fails the task instead of retrying. Without this, Reshard
// keeps re-reading the same immutable truncated object: the worker scheduler
// sees a non-Milvus error, records Retry, and DataCoord starts another run with
// backoff until the client gives up (issue #54490).
func TestSourceReadErrClassifiesTruncation(t *testing.T) {
	t.Run("raw unexpected EOF", func(t *testing.T) {
		err := sourceReadErr(7, io.ErrUnexpectedEOF)
		require.True(t, errors.Is(err, merr.ErrImportFailed), "truncated source must be an input error: %v", err)
		require.Contains(t, err.Error(), "unexpected EOF")
	})

	t.Run("wrapped unexpected EOF", func(t *testing.T) {
		err := sourceReadErr(7, fmt.Errorf("read numpy batch: %w", io.ErrUnexpectedEOF))
		require.True(t, errors.Is(err, merr.ErrImportFailed), "wrapped truncation must be an input error: %v", err)
	})

	t.Run("other errors keep their own classification", func(t *testing.T) {
		err := sourceReadErr(7, io.ErrClosedPipe)
		require.False(t, errors.Is(err, merr.ErrImportFailed), "a raw read failure is not reclassified: %v", err)
	})
}
