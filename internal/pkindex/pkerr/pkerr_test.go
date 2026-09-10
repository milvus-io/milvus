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

package pkerr

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestToMerrNil(t *testing.T) {
	assert.NoError(t, ToMerr(nil))
}

func TestToMerrPassesMerrThrough(t *testing.T) {
	in := merr.WrapErrCollectionNotFound("books")
	assert.Equal(t, in, ToMerr(in))
}

func TestToMerrCategories(t *testing.T) {
	// the context each call site adds must survive the translation, because at
	// a gRPC boundary the message is all the caller gets
	const context = "increment-7/000123.sst"

	for _, tc := range []struct {
		name     string
		category error
		wantCode int32
	}{
		{"corrupted", ErrCorrupted, merr.Code(merr.ErrDataIntegrity)},
		{"io", ErrIO, merr.Code(merr.ErrIoFailed)},
		{"unavailable", ErrUnavailable, merr.Code(merr.ErrServiceUnavailable)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := errors.Mark(errors.Newf("boom at %s", context), tc.category)
			got := ToMerr(in)
			require.Error(t, got)
			assert.Equal(t, tc.wantCode, merr.Code(got))
			assert.Contains(t, got.Error(), context)
		})
	}
}

// An error carrying no category is a pkindex bug rather than a condition the
// caller can act on, so it becomes ServiceInternal.
func TestToMerrUncategorized(t *testing.T) {
	in := errors.Wrapf(errors.New("generation is not draining"), "generation %d", 7)
	got := ToMerr(in)
	require.Error(t, got)
	assert.Equal(t, merr.Code(merr.ErrServiceInternal), merr.Code(got))
	assert.Contains(t, got.Error(), "generation 7")
	assert.Contains(t, got.Error(), "not draining")
}
