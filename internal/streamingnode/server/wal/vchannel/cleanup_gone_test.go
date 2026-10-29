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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// TestRetiredSplitSourceLeavesWhenItsCollectionIsGone: a retired source whose
// collection was dropped before its checkpoint read-back passed has nothing
// left to confirm -- DataCoord dropped the checkpoint with the collection and
// collects any later report -- so the gate lets it go once the read-back says
// the collection is gone. Every other failed read-back still holds it: a
// channel DataCoord has no position for may belong to a live collection.
func TestRetiredSplitSourceLeavesWhenItsCollectionIsGone(t *testing.T) {
	manager := newCleanupTestManager(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, nil)
	fenceCleanupTestVChannel(t, manager)
	reporter := &stubCheckpointReporter{}
	reader := &stubStoredCheckpointReader{}
	updater := newPChannelCheckpointUpdater("p1", manager.activeVChannels,
		func() *utility.WALCheckpoint {
			return &utility.WALCheckpoint{MessageID: rmq.NewRmqID(1), TimeTick: 20, Magic: 1}
		}, reporter, reader)
	manager.checkpointUpdater = updater
	cleanup := moduleapi.CleanupContext{PhysicalTimeTick: 21, SummaryRetired: func(string, uint64) bool { return true }}

	require.Empty(t, manager.ConsumeCleanupSnapshots(cleanup))
	reader.set("v1", 0, merr.WrapErrChannelNotAvailable("v1", "start position is nil"))
	updater.execute()
	require.Equal(t, []string{"v1"}, reader.takeReads())
	require.Empty(t, manager.ConsumeCleanupSnapshots(cleanup), "no position at DataCoord is not the collection being gone")
	require.NotNil(t, manager.Module("v1"))
	assert.Contains(t, manager.activeVChannels(), "v1", "still reported")

	reader.set("v1", 0, ErrCollectionGone)
	updater.execute()
	require.Equal(t, []string{"v1"}, reader.takeReads())
	snapshots := manager.ConsumeCleanupSnapshots(cleanup)
	require.Len(t, snapshots, 1)
	require.Equal(t, moduleapi.SnapshotOpDelete, snapshots[0].Op())
	snapshots[0].MarkPersisted()
	require.Nil(t, manager.Module("v1"))
	assert.NotContains(t, manager.activeVChannels(), "v1")
}
