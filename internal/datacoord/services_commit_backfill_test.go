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

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestBackfillCommitStatus(t *testing.T) {
	// Everything committed, no broadcast failure -> success.
	assert.NoError(t, merr.Error(backfillCommitStatus(false, 0, 10)))

	// Pre-validation rejects only (no broadcast failure): several are permanent, and the
	// all-rejected case is exactly an idempotent resubmit of an already-applied result. Must
	// stay success so a fully-applied resubmit does not loop forever.
	assert.NoError(t, merr.Error(backfillCommitStatus(false, 10, 10)))

	// A broadcast batch failed -> retriable top-level failure so the caller resubmits (instead
	// of the old behavior that returned success/200 as long as one segment committed).
	err := merr.Error(backfillCommitStatus(true, 3, 10))
	assert.ErrorIs(t, err, merr.ErrServiceUnavailable)

	// Even when every batch failed, it is the same retriable failure (not a hard error).
	assert.ErrorIs(t, merr.Error(backfillCommitStatus(true, 10, 10)), merr.ErrServiceUnavailable)
}

func TestIsStaleBackfillSchema(t *testing.T) {
	// Unstamped result (old Spark build that does not set schemaVersion) -> fence skipped.
	assert.False(t, isStaleBackfillSchema(0, 5))
	// Computed against the current schema -> not stale.
	assert.False(t, isStaleBackfillSchema(5, 5))
	// Computed against an older schema (schema advanced while the job ran) -> stale.
	assert.True(t, isStaleBackfillSchema(4, 5))
	// Any mismatch, even a version ahead of current, is treated as stale.
	assert.True(t, isStaleBackfillSchema(6, 5))
}
