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

package datacoord

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// --- Test refreshExternalCollectionV2AckCallback ---

// --- Test isNonRetriableRefreshError ---

func TestIsNonRetriableRefreshError(t *testing.T) {
	t.Run("collection_not_found", func(t *testing.T) {
		err := merr.WrapErrCollectionNotFound(100)
		assert.True(t, isNonRetriableRefreshError(err))
	})

	t.Run("collection_illegal_schema", func(t *testing.T) {
		err := merr.WrapErrCollectionIllegalSchema("test", "not external")
		assert.True(t, isNonRetriableRefreshError(err))
	})

	t.Run("task_duplicate", func(t *testing.T) {
		err := merr.WrapErrTaskDuplicate("refresh", "already in progress")
		assert.True(t, isNonRetriableRefreshError(err))
	})

	t.Run("generic_error_is_retriable", func(t *testing.T) {
		err := errors.New("connection timeout")
		assert.False(t, isNonRetriableRefreshError(err))
	})
}
