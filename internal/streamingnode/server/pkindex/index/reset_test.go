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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInstallAfterAResetInstallsNothing checks that the install of the
// manifest of the open does not bring back the tables before a reset.
func TestInstallAfterAResetInstallsNothing(t *testing.T) {
	ctx := context.Background()
	idx := newTestIndex(t)
	idx.insert(t, 10, 1)
	idx.observed.Store(10)
	require.True(t, idx.handover(t))
	m := idx.committer.manifest()
	idx.reopen(t)

	idx.SetCommitted(m)
	_, err := idx.Reset(ctx, 15)
	require.NoError(t, err)
	require.NoError(t, idx.Install(ctx))
	assert.True(t, idx.Ready())
	assert.Zero(t, idx.EngineForTest().Stats().CommittedTables)
	assert.Zero(t, idx.segmentOf(t, 1))

	// an install that read the manifest before the reset installs nothing either
	require.NoError(t, idx.installIfNewer(ctx, m, nil))
	assert.Zero(t, idx.EngineForTest().Stats().CommittedTables)
}
