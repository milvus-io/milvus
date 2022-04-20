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

package querynode

import (
	"context"
	"fmt"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.etcd.io/etcd/server/v3/etcdserver/api/v3client"
)

func TestEtcdShardNodeDetector_watch(t *testing.T) {

	client := v3client.New(embedetcdServer.Server)
	defer client.Close()

	type testCase struct {
		name               string
		ids                []int64
		oldRecords         map[string]*milvuspb.ReplicaInfo
		oldGarbage         map[string]string
		updateRecords      map[string]*milvuspb.ReplicaInfo
		updateGarbage      map[string]string
		delRecords         []string
		expectInitEvents   []nodeEvent
		expectupdateEvents []nodeEvent
		collectionID       int64
		replicaID          int64
		channel            string
	}
	cases := []testCase{
		{
			name: "init normal case",
			ids:  []int64{1, 2},
			oldRecords: map[string]*milvuspb.ReplicaInfo{
				"replica_1": {
					CollectionID: 1,
					ReplicaID:    1,
					NodeIds:      []int64{1, 2},
				},
			},
			oldGarbage: map[string]string{
				"noice": string([]byte{23, 11}),
			},
			expectInitEvents: []nodeEvent{
				{
					nodeID:    1,
					nodeAddr:  "1",
					eventType: nodeAdd,
				},
				{
					nodeID:    2,
					nodeAddr:  "2",
					eventType: nodeAdd,
				},
			},
			collectionID: 1,
			replicaID:    1,
		},
		{
			name: "normal case with other replica",
			ids:  []int64{1, 2},
			oldRecords: map[string]*milvuspb.ReplicaInfo{
				"replica_1": {
					CollectionID: 1,
					ReplicaID:    1,
					NodeIds:      []int64{1, 2},
				},
				"replica_2": {
					CollectionID: 1,
					ReplicaID:    2,
					NodeIds:      []int64{1, 2},
				},
			},
			expectInitEvents: []nodeEvent{
				{
					nodeID:    1,
					nodeAddr:  "1",
					eventType: nodeAdd,
				},
				{
					nodeID:    2,
					nodeAddr:  "2",
					eventType: nodeAdd,
				},
			},
			collectionID: 1,
			replicaID:    1,
		},
		{
			name: "init normal missing node",
			ids:  []int64{1},
			oldRecords: map[string]*milvuspb.ReplicaInfo{
				"replica_1": {
					CollectionID: 1,
					ReplicaID:    1,
					NodeIds:      []int64{1, 2},
				},
			},
			expectInitEvents: []nodeEvent{
				{
					nodeID:    1,
					nodeAddr:  "1",
					eventType: nodeAdd,
				},
			},
			collectionID: 1,
			replicaID:    1,
		},
		{
			name: "normal updates",
			ids:  []int64{1, 2, 3},
			oldRecords: map[string]*milvuspb.ReplicaInfo{
				"replica_1": {
					CollectionID: 1,
					ReplicaID:    1,
					NodeIds:      []int64{1},
				},
			},
			expectInitEvents: []nodeEvent{
				{
					nodeID:    1,
					nodeAddr:  "1",
					eventType: nodeAdd,
				},
			},
			updateRecords: map[string]*milvuspb.ReplicaInfo{
				"replica_1": {
					CollectionID: 1,
					ReplicaID:    1,
					NodeIds:      []int64{2},
				},
				"replica_1_extra": {
					CollectionID: 1,
					ReplicaID:    1,
					NodeIds:      []int64{3, 4},
				},
			},
			updateGarbage: map[string]string{
				"noice2": string([]byte{23, 23}),
			},
			expectupdateEvents: []nodeEvent{
				{
					nodeID:    2,
					nodeAddr:  "2",
					eventType: nodeAdd,
				},
				{
					nodeID:    1,
					nodeAddr:  "1",
					eventType: nodeDel,
				},
				{
					nodeID:    3,
					nodeAddr:  "3",
					eventType: nodeAdd,
				},
			},
			collectionID: 1,
			replicaID:    1,
		},
		{
			name: "normal updates with other replica",
			ids:  []int64{1, 2},
			oldRecords: map[string]*milvuspb.ReplicaInfo{
				"replica_1": {
					CollectionID: 1,
					ReplicaID:    1,
					NodeIds:      []int64{1},
				},
			},
			expectInitEvents: []nodeEvent{
				{
					nodeID:    1,
					nodeAddr:  "1",
					eventType: nodeAdd,
				},
			},
			updateRecords: map[string]*milvuspb.ReplicaInfo{
				"replica_1": {
					CollectionID: 1,
					ReplicaID:    1,
					NodeIds:      []int64{2},
				},
				"replica_2": {
					CollectionID: 1,
					ReplicaID:    2,
					NodeIds:      []int64{2},
				},
			},
			updateGarbage: map[string]string{
				"noice2": string([]byte{23, 23}),
			},
			expectupdateEvents: []nodeEvent{
				{
					nodeID:    2,
					nodeAddr:  "2",
					eventType: nodeAdd,
				},
				{
					nodeID:    1,
					nodeAddr:  "1",
					eventType: nodeDel,
				},
			},
			collectionID: 1,
			replicaID:    1,
		},
		{
			name: "normal deletes",
			ids:  []int64{1, 2},
			oldRecords: map[string]*milvuspb.ReplicaInfo{
				"replica_1": {
					CollectionID: 1,
					ReplicaID:    1,
					NodeIds:      []int64{1},
				},
				"replica_2": {
					CollectionID: 1,
					ReplicaID:    2,
					NodeIds:      []int64{2},
				},
			},
			oldGarbage: map[string]string{
				"noice": string([]byte{23}),
			},
			expectInitEvents: []nodeEvent{
				{
					nodeID:    1,
					nodeAddr:  "1",
					eventType: nodeAdd,
				},
			},
			delRecords: []string{"replica_1", "replica_2", "noice"},
			expectupdateEvents: []nodeEvent{
				{
					nodeID:    1,
					nodeAddr:  "1",
					eventType: nodeDel,
				},
			},
			collectionID: 1,
			replicaID:    1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			suffix := funcutil.RandomString(6)
			rootPath := fmt.Sprintf("qn_shard_node_detector_watch_%s", suffix)
			ctx := context.Background()
			for key, info := range tc.oldRecords {
				bs, err := proto.Marshal(info)
				require.NoError(t, err)
				_, err = client.Put(ctx, path.Join(rootPath, key), string(bs))
				require.NoError(t, err)
			}

			for k, v := range tc.oldGarbage {
				_, err := client.Put(ctx, path.Join(rootPath, k), v)
				require.NoError(t, err)
			}

			nd := NewEtcdShardNodeDetector(client, rootPath, func() (map[int64]string, error) {
				r := make(map[int64]string)
				for _, id := range tc.ids {
					r[id] = strconv.FormatInt(id, 10)
				}
				return r, nil
			})

			nodes, ch := nd.watchNodes(tc.collectionID, tc.replicaID, tc.channel)
			assert.ElementsMatch(t, tc.expectInitEvents, nodes)

			go func() {
				for key, info := range tc.updateRecords {
					bs, err := proto.Marshal(info)
					require.NoError(t, err)
					_, err = client.Put(ctx, path.Join(rootPath, key), string(bs))
					require.NoError(t, err)
				}
				for k, v := range tc.updateGarbage {
					_, err := client.Put(ctx, path.Join(rootPath, k), v)
					require.NoError(t, err)
				}
				for _, k := range tc.delRecords {
					_, err := client.Delete(ctx, path.Join(rootPath, k))
					require.NoError(t, err)
				}
				time.Sleep(100 * time.Millisecond)
				nd.Close()
			}()
			var newEvents []nodeEvent
			for event := range ch {
				newEvents = append(newEvents, event)
			}

			assert.ElementsMatch(t, tc.expectupdateEvents, newEvents)
		})
	}
}
