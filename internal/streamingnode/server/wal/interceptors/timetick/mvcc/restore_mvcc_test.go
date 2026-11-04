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

package mvcc

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
)

func TestRestoreCollectionMVCCMatchesCreate(t *testing.T) {
	for _, restore := range []bool{false, true} {
		name := "create"
		if restore {
			name = "restore"
		}
		t.Run(name, func(t *testing.T) {
			manager := NewMVCCManager(100)
			body := &message.CreateCollectionRequest{VirtualChannelNames: []string{"p_1v0"}}
			header := &message.CreateCollectionMessageHeader{CollectionId: 1}
			var create message.MutableMessage
			if restore {
				create = message.NewRestoreSnapshotMessageBuilderV2().WithVChannel("p_1v0").
					WithHeader(&message.RestoreSnapshotMessageHeader{CollectionId: 1}).
					WithBody(&message.RestoreSnapshotMessageBody{CollectionHeader: header, Collection: body}).MustBuildMutable()
			} else {
				create = message.NewCreateCollectionMessageBuilderV1().WithVChannel("p_1v0").WithHeader(header).WithBody(body).MustBuildMutable()
			}
			manager.UpdateMVCC(create.WithTimeTick(120))
			require.Equal(t, VChannelMVCC{Timetick: 120}, manager.GetMVCCOfVChannel("p_1v0"))
			for _, write := range []struct {
				kind message.MessageType
				tick uint64
			}{{message.MessageTypeInsert, 150}, {message.MessageTypeDelete, 160}} {
				manager.UpdateMVCC(createQueryTestMessage(t, write.tick, "p_1v0", write.kind, false, true))
				require.Equal(t, VChannelMVCC{Timetick: write.tick}, manager.GetMVCCOfVChannel("p_1v0"))
			}
			manager.UpdateMVCC(create.WithTimeTick(120))
			manager.UpdateMVCC(createQueryTestMessage(t, 200, "", message.MessageTypeTimeTick, false, true))
			require.Equal(t, VChannelMVCC{Timetick: 160, Confirmed: true}, manager.GetMVCCOfVChannel("p_1v0"))
			manager.UpdateMVCC(createQueryTestMessage(t, 250, "p_1v0", message.MessageTypeDropCollection, false, true))
			manager.UpdateMVCC(createQueryTestMessage(t, 240, "p_1v0", message.MessageTypeInsert, false, true))
			require.NotContains(t, manager.vchannelMVCCTimestamps, "p_1v0")
		})
	}
}

func TestLegacyRestoreMVCCRequiresExistingCollection(t *testing.T) {
	manager := NewMVCCManager(100)
	restore := message.NewRestoreSnapshotMessageBuilderV2().WithVChannel("p_1v0").
		WithHeader(&message.RestoreSnapshotMessageHeader{CollectionId: 1}).
		WithBody(&message.RestoreSnapshotMessageBody{}).MustBuildMutable()
	manager.UpdateMVCC(restore.WithTimeTick(120))
	require.NotContains(t, manager.vchannelMVCCTimestamps, "p_1v0")
	manager.ApplyRecoveryBarrier("p_1v0", 120)
	manager.UpdateMVCC(restore.WithTimeTick(150))
	require.Equal(t, VChannelMVCC{Timetick: 150}, manager.GetMVCCOfVChannel("p_1v0"))
	manager.UpdateMVCC(createQueryTestMessage(t, 200, "p_1v0", message.MessageTypeDropCollection, false, true))
	manager.UpdateMVCC(restore.WithTimeTick(250))
	require.NotContains(t, manager.vchannelMVCCTimestamps, "p_1v0")
}

func TestRestoreCollectionMVCCIgnoresControlChannel(t *testing.T) {
	manager := NewMVCCManager(100)
	restore := message.NewRestoreSnapshotMessageBuilderV2().WithVChannel(funcutil.GetControlChannel("p")).
		WithHeader(&message.RestoreSnapshotMessageHeader{CollectionId: 1}).
		WithBody(&message.RestoreSnapshotMessageBody{Collection: &message.CreateCollectionRequest{}}).MustBuildMutable()
	manager.UpdateMVCC(restore.WithTimeTick(120))
	require.Empty(t, manager.vchannelMVCCTimestamps)
}
