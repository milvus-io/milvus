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

package snapshot

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/rootcoord"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type residueRestoreTestReplicator struct {
	streaming.ReplicateService
	config *commonpb.ReplicateConfiguration
	err    error
	calls  int
}

func (r *residueRestoreTestReplicator) GetReplicateConfiguration(context.Context) (*commonpb.ReplicateConfiguration, error) {
	r.calls++
	return r.config, r.err
}

type residueRestoreTestWAL struct {
	streaming.WALAccesser
	service streaming.ReplicateService
}

func (w *residueRestoreTestWAL) Replicate() streaming.ReplicateService { return w.service }

func TestResidueRestoreRejectsReplicationBeforeTargetAllocation(t *testing.T) {
	topology := &commonpb.ReplicateConfiguration{CrossClusterTopology: []*commonpb.CrossClusterTopology{{SourceClusterId: "primary", TargetClusterId: "secondary"}}}
	readFailure := merr.WrapErrServiceUnavailableMsg("configuration read unavailable")
	allocationProbe := merr.WrapErrServiceUnavailableMsg("target allocation boundary")
	for _, tc := range []struct {
		name               string
		modulus            uint64
		config             *commonpb.ReplicateConfiguration
		readErr            error
		missingWAL         bool
		want               error
		reads, allocations int
	}{
		{name: "replicating primary", modulus: 4, config: topology, want: merr.ErrOperationNotSupported, reads: 1},
		{name: "multiple configured clusters without edges", modulus: 4, config: &commonpb.ReplicateConfiguration{Clusters: []*commonpb.MilvusCluster{{ClusterId: "primary"}, {ClusterId: "secondary"}}}, want: merr.ErrOperationNotSupported, reads: 1},
		{name: "fresh configuration read fails", modulus: 4, readErr: readFailure, want: readFailure, reads: 1},
		{name: "WAL not ready", modulus: 4, missingWAL: true, want: merr.ErrServiceNotReady},
		{name: "standalone without configuration", modulus: 4, want: allocationProbe, reads: 1, allocations: 1},
		{name: "standalone explicit configuration", modulus: 4, config: &commonpb.ReplicateConfiguration{}, want: allocationProbe, reads: 1, allocations: 1},
		{name: "legacy snapshot retains behavior", config: topology, want: allocationProbe, allocations: 1},
	} {
		mockey.PatchConvey(tc.name, t, func() {
			oldWAL := streaming.WAL()
			defer streaming.SetWALForTest(oldWAL)
			service := &residueRestoreTestReplicator{config: tc.config, err: tc.readErr}
			if tc.missingWAL {
				streaming.SetWALForTest(nil)
			} else {
				streaming.SetWALForTest(&residueRestoreTestWAL{service: service})
			}
			capture := &meta.CaptureBroadcastAPI{}
			mockey.Mock(broadcast.StartBroadcastWithResourceKeys).Return(capture, nil).Build()
			allocation := mockey.Mock((*rootcoord.Core).CheckRestoreTargetAbsent).Return(allocationProbe).Build()
			sm := &Manager{CollectionRestorer: &rootcoord.Core{}}
			_, err := sm.broadcastRestoreCollection(t.Context(), &snapshotstorage.SnapshotData{Collection: &datapb.CollectionDescription{RoutingModulus: tc.modulus}}, "snapshot", 1, "target", "default", 0, false, "", "")
			require.ErrorIs(t, err, tc.want)
			require.Equal(t, tc.reads, service.calls)
			require.Equal(t, tc.allocations, allocation.Times(), "no target preparation may precede admission")
		})
	}
}
