// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package streaming

import (
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	pulsarwal "github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/pulsar"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
)

func TestReplicatedRestoreCreationIdentity(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "creation recipe", true: "legacy"}[legacy], func(t *testing.T) {
			cfg := replicateutil.MustNewConfigHelper("secondary", &commonpb.ReplicateConfiguration{
				Clusters: []*commonpb.MilvusCluster{
					{ClusterId: "primary", Pchannels: []string{"primary-dml_0", "primary-dml_1"}},
					{ClusterId: "secondary", Pchannels: []string{"secondary-dml_0", "secondary-dml_1"}},
				},
				CrossClusterTopology: []*commonpb.CrossClusterTopology{{SourceClusterId: "primary", TargetClusterId: "secondary"}},
			})
			header := &message.RestoreSnapshotMessageHeader{CollectionId: 551, SourceCollectionId: 100, JobId: 77, PinId: 31, SnapshotName: "snapshot", SnapshotId: 9001}
			body := &messagespb.RestoreSnapshotMessageBody{}
			if !legacy {
				body.CollectionHeader = &message.CreateCollectionMessageHeader{CollectionId: 551, DbId: 9, PartitionIds: []int64{601, 602}}
				body.Collection = &msgpb.CreateCollectionRequest{
					CollectionID: 551, DbID: 9, PartitionIDs: []int64{601, 602}, PartitionNames: []string{"_default", "p1"},
					PhysicalChannelNames: []string{"primary-dml_0", "primary-dml_1"},
					VirtualChannelNames:  []string{"primary-dml_0_551v0", "primary-dml_1_551v1"},
				}
				body.Indexes = []*indexpb.FieldIndex{{IndexInfo: &indexpb.IndexInfo{CollectionID: 551, IndexID: 701, FieldID: 101}}}
			}
			original := proto.Clone(body)
			bc := message.NewRestoreSnapshotMessageBuilderV2().WithHeader(header).WithBody(body).
				WithBroadcast([]string{"primary-dml_0_551v0", "primary-dml_1_551v1"}).MustBuildBroadcast().WithBroadcastID(801)
			rs := replicateService{walAccesserImpl: &walAccesserImpl{clusterID: "secondary"}}
			for _, mutable := range bc.SplitIntoMutableMessage() {
				im := mutable.WithTimeTick(1).WithLastConfirmedUseMessageID().IntoImmutableMessage(pulsarwal.NewPulsarID(pulsar.NewMessageID(1, 2, 3, 4)))
				replica := message.MustNewReplicateMessage("primary", im.IntoImmutableMessageProto())
				require.NoError(t, rs.overwriteRestoreSnapshotMessage(cfg.GetCluster("primary"), replica))
				restored := message.MustAsMutableRestoreSnapshotMessageV2(replica)
				require.True(t, proto.Equal(header, restored.Header()))
				got := restored.MustBody()
				if legacy {
					require.Nil(t, got.GetCollection())
					continue
				}
				require.Equal(t, []string{"secondary-dml_0", "secondary-dml_1"}, got.GetCollection().GetPhysicalChannelNames())
				require.Equal(t, []string{"secondary-dml_0_551v0", "secondary-dml_1_551v1"}, got.GetCollection().GetVirtualChannelNames())
				require.EqualValues(t, 551, got.GetCollection().GetCollectionID())
				require.EqualValues(t, 9, got.GetCollection().GetDbID())
				require.Equal(t, []int64{601, 602}, got.GetCollection().GetPartitionIDs())
				require.True(t, proto.Equal(body.GetCollectionHeader(), got.GetCollectionHeader()))
				require.True(t, proto.Equal(body.GetIndexes()[0], got.GetIndexes()[0]))
			}
			require.True(t, proto.Equal(original, body), "mutable decoding must not edit the source recipe")
		})
	}
}
