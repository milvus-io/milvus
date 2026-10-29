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

package vchannel

import (
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

// TestCheckpointUpdaterReleasesAVChannelWhoseCollectionIsGone: a read back
// answering ErrCollectionGone ends the wait for good, whatever tick is asked
// for, while every other failed read back keeps it. The gauge of awaited
// vchannels follows the wait, and Forget starts it over.
func TestCheckpointUpdaterReleasesAVChannelWhoseCollectionIsGone(t *testing.T) {
	reporter := &stubCheckpointReporter{}
	reader := &stubStoredCheckpointReader{}
	updater := newTestCheckpointUpdater(reporter, func() *utility.WALCheckpoint {
		return &utility.WALCheckpoint{MessageID: rmq.NewRmqID(1), TimeTick: 100, Magic: 1}
	})
	updater.storedReader = reader

	assert.Zero(t, testutil.ToFloat64(updater.awaiting))
	require.False(t, updater.Reported("v1", 100))
	assert.Equal(t, float64(1), testutil.ToFloat64(updater.awaiting))
	require.False(t, updater.Reported("v1", 100), "asked again, still one awaited")
	assert.Equal(t, float64(1), testutil.ToFloat64(updater.awaiting))

	reader.set("v1", 0, errors.New("coordinator down"))
	updater.execute()
	require.Equal(t, []string{"v1"}, reader.takeReads())
	require.False(t, updater.Reported("v1", 100), "a failed read back proves nothing")
	assert.Equal(t, float64(1), testutil.ToFloat64(updater.awaiting))

	reader.set("v1", 0, ErrCollectionGone)
	updater.execute()
	require.Equal(t, []string{"v1"}, reader.takeReads())
	assert.True(t, updater.Reported("v1", 100))
	assert.True(t, updater.Reported("v1", 1<<60), "nothing of a gone collection is ever waited for")
	assert.Zero(t, testutil.ToFloat64(updater.awaiting))
	updater.execute()
	assert.Empty(t, reader.takeReads(), "nothing awaited, nothing read back")

	updater.Forget("v1")
	require.False(t, updater.Reported("v1", 100), "a forgotten vchannel is read back afresh")
	assert.Equal(t, float64(1), testutil.ToFloat64(updater.awaiting))
	updater.Forget("v1")
	assert.Zero(t, testutil.ToFloat64(updater.awaiting))
}
