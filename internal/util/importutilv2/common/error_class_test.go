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

package common

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// An object-store outage that outlives the reader's retries reaches the decoder.
// Folding it into ErrImportFailed (2100, InputError) reports the outage as bad
// input data, and retry.Do aborts on InputError -- so the IO code has to survive
// the wrap, including when the decoder wraps it first.
func TestWrapDecodeErr(t *testing.T) {
	t.Run("keeps a typed IO cause", func(t *testing.T) {
		cause := merr.WrapErrIoFailed("a.parquet", errors.New("connection reset"))
		got := WrapDecodeErr(cause, "read parquet footer failed")

		assert.Equal(t, merr.Code(merr.ErrIoFailed), merr.Code(got))
		assert.NotEqual(t, merr.Code(merr.ErrImportFailed), merr.Code(got))
		assert.Contains(t, got.Error(), "read parquet footer failed")
	})

	t.Run("keeps a typed IO cause the decoder wrapped first", func(t *testing.T) {
		// Arrow wraps with %w; npyio hands the reader's error back verbatim.
		cause := errors.Wrap(merr.WrapErrIoTooManyRequests("a.parquet", errors.New("slow down")), "parquet: could not read footer")
		got := WrapDecodeErr(cause, "read parquet footer failed")

		assert.Equal(t, merr.Code(merr.ErrIoTooManyRequests), merr.Code(got))
	})

	t.Run("a real decode failure stays input", func(t *testing.T) {
		got := WrapDecodeErr(errors.New("parquet: invalid magic"), "read parquet footer failed")

		assert.Equal(t, merr.Code(merr.ErrImportFailed), merr.Code(got))
		assert.Contains(t, got.Error(), "parquet: invalid magic")
	})

	t.Run("IsTypedIOErr covers the whole family", func(t *testing.T) {
		for _, sentinel := range ioErrSentinels {
			assert.True(t, IsTypedIOErr(errors.Wrap(sentinel, "wrapped")),
				"sentinel %v must be recognized", sentinel)
		}
		assert.False(t, IsTypedIOErr(errors.New("plain")))
		assert.False(t, IsTypedIOErr(merr.WrapErrImportFailedMsg("bad file")))
	})
}
