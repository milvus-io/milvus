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

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestRestoreSnapshotCreatesRecoverableVChannel(t *testing.T) {
	manager, err := NewPChannelRecoveryManager(PChannelManagerConfig{PChannel: "p1"})
	require.NoError(t, err)
	defer manager.Close()
	body := &message.RestoreSnapshotMessageBody{
		CollectionHeader: &message.CreateCollectionMessageHeader{CollectionId: 10, PartitionIds: []int64{11, 12}},
		Collection:       &message.CreateCollectionRequest{CollectionID: 10, PartitionIDs: []int64{11, 12}, CollectionSchema: &schemapb.CollectionSchema{Name: "restored", Version: 3}},
	}
	raw := message.NewRestoreSnapshotMessageBuilderV2().WithVChannel("p1_10v0").
		WithHeader(&message.RestoreSnapshotMessageHeader{CollectionId: 10, JobId: 20}).WithBody(body).MustBuildMutable().
		WithTimeTick(123).WithLastConfirmed(walimplstest.NewTestMessageID(7)).IntoImmutableMessage(walimplstest.NewTestMessageID(8))
	for i := 0; i < 2; i++ {
		owner := message.NewOwnedImmutableMessage(raw, nil)
		retained := owner.Clone()
		manager.ObserveMessage(t.Context(), retained)
		retained.Release()
		owner.Release()
	}
	module := manager.Module("p1_10v0")
	require.NotNil(t, module)
	snapshots := module.ConsumeDirtySnapshots()
	require.NotEmpty(t, snapshots)
	meta := module.vchannelView.AssignmentMeta()
	require.EqualValues(t, 123, meta.GetCheckpointTimeTick())
	require.EqualValues(t, 10, meta.GetCollectionInfo().GetCollectionId())
	require.Len(t, meta.GetCollectionInfo().GetPartitions(), 2)
	require.EqualValues(t, 3, meta.GetCollectionInfo().GetSchemas()[0].GetSchema().GetVersion())
	encoded, err := proto.Marshal(meta)
	require.NoError(t, err)
	recovered := &streamingpb.VChannelMeta{}
	require.NoError(t, proto.Unmarshal(encoded, recovered))
	restarted, err := NewPChannelRecoveryManager(PChannelManagerConfig{PChannel: "p1", VChannelMetas: map[string]*streamingpb.VChannelMeta{"p1_10v0": recovered}})
	require.NoError(t, err)
	defer restarted.Close()
	require.EqualValues(t, 10, restarted.Module("p1_10v0").vchannelView.AssignmentMeta().GetCollectionInfo().GetCollectionId())
}

func TestLegacyRestoreSnapshotDoesNotCreateVChannel(t *testing.T) {
	manager, err := NewPChannelRecoveryManager(PChannelManagerConfig{PChannel: "p1"})
	require.NoError(t, err)
	defer manager.Close()
	raw := message.NewRestoreSnapshotMessageBuilderV2().WithVChannel("p1_10v0").
		WithHeader(&message.RestoreSnapshotMessageHeader{CollectionId: 10}).WithBody(&message.RestoreSnapshotMessageBody{}).MustBuildMutable().
		WithTimeTick(123).WithLastConfirmed(walimplstest.NewTestMessageID(7)).IntoImmutableMessage(walimplstest.NewTestMessageID(8))
	require.Nil(t, manager.moduleForMessage(raw))
}
