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

package utils

import (
	"testing"

	etcdKV "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	. "github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/querycoordv2/session"
	"github.com/milvus-io/milvus/internal/util/etcd"
)

func TestSpawnReplicasWithRG(t *testing.T) {
	Params.Init()
	config := GenerateEtcdConfig()
	cli, _ := etcd.GetEtcdClient(
		config.UseEmbedEtcd.GetAsBool(),
		config.EtcdUseSSL.GetAsBool(),
		config.Endpoints.GetAsStrings(),
		config.EtcdTLSCert.GetValue(),
		config.EtcdTLSKey.GetValue(),
		config.EtcdTLSCACert.GetValue(),
		config.EtcdTLSMinVersion.GetValue())
	kv := etcdKV.NewEtcdKV(cli, config.MetaRootPath.GetValue())

	store := meta.NewMetaStore(kv)
	nodeMgr := session.NewNodeManager()
	m := meta.NewMeta(RandomIncrementIDAllocator(), store, nodeMgr)
	m.ResourceManager.AddResourceGroup("rg1")
	m.ResourceManager.AddResourceGroup("rg2")
	m.ResourceManager.AddResourceGroup("rg3")

	for i := 1; i < 10; i++ {
		nodeMgr.Add(session.NewNodeInfo(int64(i), "localhost"))

		if i%3 == 0 {
			m.ResourceManager.AssignNode("rg1", int64(i))
		}
		if i%3 == 1 {
			m.ResourceManager.AssignNode("rg2", int64(i))
		}
		if i%3 == 2 {
			m.ResourceManager.AssignNode("rg3", int64(i))
		}
	}

	type args struct {
		m              *meta.Meta
		collection     int64
		resourceGroups []string
		replicaNumber  int32
	}

	tests := []struct {
		name           string
		args           args
		wantReplicaNum int
		wantErr        bool
	}{
		{
			name:           "test 3 replica on 1 rg",
			args:           args{m, 1000, []string{"rg1"}, 3},
			wantReplicaNum: 3,
			wantErr:        false,
		},

		{
			name:           "test 3 replica on 2 rg",
			args:           args{m, 1000, []string{"rg1", "rg2"}, 3},
			wantReplicaNum: 0,
			wantErr:        true,
		},

		{
			name:           "test 3 replica on 3 rg",
			args:           args{m, 1000, []string{"rg1", "rg2", "rg3"}, 3},
			wantReplicaNum: 3,
			wantErr:        false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SpawnReplicasWithRG(tt.args.m, tt.args.collection, tt.args.resourceGroups, tt.args.replicaNumber)
			if (err != nil) != tt.wantErr {
				t.Errorf("SpawnReplicasWithRG() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(got) != tt.wantReplicaNum {
				t.Errorf("SpawnReplicasWithRG() = %v, want %d replicas", got, tt.args.replicaNumber)
			}
		})
	}
}
