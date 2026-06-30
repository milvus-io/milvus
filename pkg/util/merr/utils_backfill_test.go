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

package merr

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

func TestWrapErrServiceFieldBackfillInProgress(t *testing.T) {
	err := WrapErrServiceFieldBackfillInProgress(100, 10)
	assert.True(t, errors.Is(err, ErrServiceFieldBackfillInProgress))
	assert.Equal(t, Code(ErrServiceFieldBackfillInProgress), Code(err))
	// Retriable so the SDK polls until the gate is revoked.
	assert.True(t, IsRetryableErr(err))

	withMsg := WrapErrServiceFieldBackfillInProgress(100, 10, "search denied")
	assert.True(t, errors.Is(withMsg, ErrServiceFieldBackfillInProgress))
	assert.Contains(t, withMsg.Error(), "search denied")
}
