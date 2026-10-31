// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"context"
	"path"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	qvcatalog "github.com/milvus-io/milvus/internal/metastore/kv/queryview"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestShowReplicasUsesLatestLoadVersion(t *testing.T) {
	paramtable.Init()
	cfg := &paramtable.Get().EtcdCfg
	client, err := etcd.GetEtcdClient(cfg.UseEmbedEtcd.GetAsBool(), cfg.EtcdUseSSL.GetAsBool(), cfg.Endpoints.GetAsStrings(),
		cfg.EtcdTLSCert.GetValue(), cfg.EtcdTLSKey.GetValue(), cfg.EtcdTLSCACert.GetValue(), cfg.EtcdTLSMinVersion.GetValue())
	require.NoError(t, err)
	root := "test-qv-replica-watcher-" + uuid.NewString()
	storage := etcdkv.NewEtcdKV(client, path.Join(root, "meta"))
	t.Cleanup(func() {
		require.NoError(t, storage.RemoveWithPrefix(context.Background(), ""))
		storage.Close()
	})
	catalog := qvcatalog.NewCollectionLoadInfoCatalog(storage)
	// Numeric version 10 must win over 9, irrespective of catalog key order.
	// Retained history must neither resurrect removed replicas nor duplicate IDs.
	for _, info := range []*viewpb.CollectionLoadConfig{
		{CollectionID: 1, Version: 9, Replicas: []*viewpb.LoadReplicaConfig{{ReplicaId: 11}, {ReplicaId: 12}}},
		{CollectionID: 1, Version: 10, Replicas: []*viewpb.LoadReplicaConfig{{ReplicaId: 11, ResourceGroupName: "new-rg"}}},
		{CollectionID: 2, Version: 1, Replicas: []*viewpb.LoadReplicaConfig{{ReplicaId: 21, ResourceGroupName: "other-rg"}}},
	} {
		require.NoError(t, catalog.Save(t.Context(), info))
	}
	watcher := &EtcdMetaWatcher{rootPath: root, etcdCli: client}
	replicas, err := watcher.ShowReplicas()
	require.NoError(t, err)
	require.Len(t, replicas, 2)
	require.EqualValues(t, 11, replicas[0].GetID())
	require.EqualValues(t, 1, replicas[0].GetCollectionID())
	require.Equal(t, "new-rg", replicas[0].GetResourceGroup())
	require.EqualValues(t, 21, replicas[1].GetID())
	require.EqualValues(t, 2, replicas[1].GetCollectionID())
	require.NoError(t, catalog.RemoveCollection(t.Context(), 1))
	replicas, err = watcher.ShowReplicas()
	require.NoError(t, err)
	require.Len(t, replicas, 1)
	require.EqualValues(t, 21, replicas[0].GetID())
}
