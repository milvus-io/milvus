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

package index

import (
	"context"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestIsFatal(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		fatal bool
	}{
		{"fenced", errors.Wrap(ErrFenced, "v0"), true},
		{"rejected", errors.Wrap(ErrRejected, "v0"), true},
		{"corrupted", errors.Mark(errors.New("bad block"), pkroot.ErrCorrupted), true},
		{"unavailable", errors.Mark(errors.New("connection reset"), pkroot.ErrUnavailable), false},
		{"io", errors.Mark(errors.New("disk full"), pkroot.ErrIO), false},
		// The Committer translates the codes of the manifest service. A code
		// that it did not translate is retryable.
		{"untranslated service internal", merr.WrapErrServiceInternal("bug"), false},
		{"deadline", context.DeadlineExceeded, false},
		{"canceled", context.Canceled, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.fatal, IsFatal(c.err))
		})
	}
}
