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

package meta

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore/kv/querycoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestReplicaTargetPersistence(t *testing.T) {
	ctx := context.Background()
	var saved []byte
	var saveErr error
	defer mockey.Mock(querycoord.Catalog.SaveCollection).To(func(_ querycoord.Catalog, _ context.Context, info *querypb.CollectionLoadInfo, _ ...*querypb.PartitionLoadInfo) error {
		if saveErr != nil {
			return saveErr
		}
		var err error
		saved, err = proto.Marshal(info)
		return err
	}).Build().UnPatch()
	m := NewCollectionManager(querycoord.NewCatalog(nil))
	original := &Collection{CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: 100, ReplicaNumber: 1, ResourceGroupReplicaNumbers: map[string]int32{"A": 1}}}
	partition := &Partition{PartitionLoadInfo: &querypb.PartitionLoadInfo{CollectionID: 100, PartitionID: 10, ReplicaNumber: 1}}
	require.NoError(t, m.PutCollection(ctx, original, partition))
	target := map[string]int32{"B": 2}
	require.NoError(t, m.UpdateReplicaConfig(ctx, 100, 2, true, target))
	target["B"] = 99
	require.Equal(t, int32(1), partition.GetReplicaNumber())
	require.Equal(t, int32(2), m.GetPartitionsByCollection(ctx, 100)[0].GetReplicaNumber())
	recovered := &querypb.CollectionLoadInfo{}
	require.NoError(t, proto.Unmarshal(saved, recovered))
	require.Equal(t, map[string]int32{"B": 2}, recovered.GetResourceGroupReplicaNumbers())
	require.Equal(t, int32(2), recovered.GetReplicaNumber())
	require.True(t, recovered.GetUserSpecifiedReplicaMode())
	require.Equal(t, map[string]int32{"A": 1}, original.GetResourceGroupReplicaNumbers())
	require.Equal(t, map[string]int32{"B": 2}, m.GetCollection(ctx, 100).GetResourceGroupReplicaNumbers())
	saveErr = merr.WrapErrServiceUnavailableMsg("etcd unavailable")
	require.ErrorIs(t, m.UpdateReplicaConfig(ctx, 100, 3, false, map[string]int32{"C": 3}), saveErr)
	require.Equal(t, map[string]int32{"B": 2}, m.GetCollection(ctx, 100).GetResourceGroupReplicaNumbers())
	require.Error(t, m.UpdateReplicaConfig(ctx, 101, 1, false, map[string]int32{"A": 1}))
}

func TestRecoverReplicaTargets(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		target map[string]int32
		count  int32
		actual []string
		want   map[string]int32
	}{
		{name: "legacy", count: 2, actual: []string{"A", "A"}, want: map[string]int32{"A": 2}},
		{name: "already persisted", count: 2, target: map[string]int32{"B": 2}, actual: []string{"A"}, want: map[string]int32{"B": 2}},
		{name: "incomplete legacy remains unknown", count: 2, actual: []string{"A"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			saves := 0
			defer mockey.Mock(querycoord.Catalog.SaveCollection).To(func(querycoord.Catalog, context.Context, *querypb.CollectionLoadInfo, ...*querypb.PartitionLoadInfo) error {
				saves++
				return nil
			}).Build().UnPatch()
			m := NewMeta(nil, querycoord.NewCatalog(nil), nil)
			require.NoError(t, m.PutCollection(ctx, &Collection{CollectionLoadInfo: &querypb.CollectionLoadInfo{CollectionID: 100, ReplicaNumber: tc.count, ResourceGroupReplicaNumbers: tc.target}}))
			replicas := make([]*Replica, 0, len(tc.actual))
			for i, rg := range tc.actual {
				replicas = append(replicas, NewReplica(&querypb.Replica{ID: int64(i + 1), CollectionID: 100, ResourceGroup: rg}, typeutil.NewUniqueSet()))
			}
			defer mockey.Mock((*ReplicaManager).GetByCollection).Return(replicas).Build().UnPatch()
			require.NoError(t, m.RecoverReplicaTargets(ctx))
			require.Equal(t, tc.want, m.GetCollection(ctx, 100).GetResourceGroupReplicaNumbers())
			firstSaves := saves
			require.NoError(t, m.RecoverReplicaTargets(ctx))
			require.Equal(t, firstSaves, saves)
		})
	}
}

func TestReplicaCollectionIDsIncludeDrainingCollections(t *testing.T) {
	m := NewMeta(nil, nil, nil)
	// Collection registration is absent during release, but replicas remain
	// until physical resource cleanup completes.
	m.putReplicasInMemory(100, NewReplica(&querypb.Replica{ID: 1, CollectionID: 100}, typeutil.NewUniqueSet()))
	require.Empty(t, m.GetAllCollections(context.Background()))
	require.Equal(t, []int64{100}, m.GetCollectionIDs())
	m.removeReplicasInMemory(100, 1)
	require.Empty(t, m.GetCollectionIDs())
}
