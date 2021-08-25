// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querycoord

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
)

func TestQueryNodeCluster_getMetrics(t *testing.T) {
	log.Info("TestQueryNodeCluster_getMetrics, todo")
}

func TestReloadClusterFromKV(t *testing.T) {
	refreshParams()
	kv, err := etcdkv.NewEtcdKV(Params.EtcdEndpoints, Params.MetaRootPath)
	assert.Nil(t, err)
	cluster := &queryNodeCluster{
		client:    kv,
		nodes:     make(map[int64]Node),
		newNodeFn: newQueryNodeTest,
	}

	kvs := make(map[string]string)
	session := &sessionutil.Session{
		ServerID: 100,
		Address:  "localhost",
	}
	sessionBlob, err := json.Marshal(session)
	assert.Nil(t, err)
	sessionKey := fmt.Sprintf("%s/%d", queryNodeInfoPrefix, 100)
	kvs[sessionKey] = string(sessionBlob)

	collectionInfo := &querypb.CollectionInfo{
		CollectionID: defaultCollectionID,
	}
	collectionBlobs := proto.MarshalTextString(collectionInfo)
	nodeKey := fmt.Sprintf("%s/%d", queryNodeMetaPrefix, 100)
	kvs[nodeKey] = collectionBlobs

	err = kv.MultiSave(kvs)
	assert.Nil(t, err)

	cluster.reloadFromKV()

	assert.Equal(t, 1, len(cluster.nodes))
	collection := cluster.getCollectionInfosByID(context.Background(), 100)
	assert.Equal(t, defaultCollectionID, collection[0].CollectionID)
}
