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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/proto/querypb"
)

func genSimpleQueryShard(ctx context.Context) (*queryShard, error) {
	collectionID := defaultCollectionID
	vchannelName := defaultDMLChannel
	replicaID := int64(0)

	tSafe := newTSafeReplica()
	historical, err := genSimpleHistorical(ctx, tSafe)
	if err != nil {
		return nil, err
	}

	streaming, err := genSimpleStreaming(ctx, tSafe)
	if err != nil {
		return nil, err
	}

	localCM, err := genLocalChunkManager()
	if err != nil {
		return nil, err
	}

	remoteCM, err := genRemoteChunkManager(ctx)
	if err != nil {
		return nil, err
	}

	nodeEvents := []nodeEvent{
		{
			nodeID:   1,
			nodeAddr: "addr_1",
		},
		{
			nodeID:   2,
			nodeAddr: "addr_2",
		},
	}

	segmentEvents := []segmentEvent{
		{
			segmentID: 1,
			nodeID:    1,
			state:     segmentStateLoaded,
		},
		{
			segmentID: 2,
			nodeID:    2,
			state:     segmentStateLoaded,
		},
		{
			segmentID: 3,
			nodeID:    2,
			state:     segmentStateLoaded,
		},
	}

	shardCluster := NewShardCluster(collectionID, replicaID, vchannelName,
		&mockNodeDetector{
			initNodes: nodeEvents,
		}, &mockSegmentDetector{
			initSegments: segmentEvents,
		}, buildMockQueryNode)

	qs := newQueryShard(ctx, collectionID, vchannelName, replicaID, shardCluster,
		historical, streaming, localCM, remoteCM, false)
	return qs, nil
}

func TestQueryShard_Search(t *testing.T) {
	qs, err := genSimpleQueryShard(context.Background())
	assert.NoError(t, err)
	_, err = qs.search(context.Background(), &querypb.SearchRequest{})
	assert.Error(t, err)
}

func TestQueryShard_Query(t *testing.T) {
	qs, err := genSimpleQueryShard(context.Background())
	assert.NoError(t, err)

	req, err := genSimpleRetrieveRequest()
	assert.NoError(t, err)

	t.Run("query follower", func(t *testing.T) {
		request := &querypb.QueryRequest{
			Req:        req,
			DmlChannel: "",
			SegmentIDs: []int64{defaultSegmentID},
		}

		resp, err := qs.query(context.Background(), request)
		assert.NoError(t, err)
		assert.ElementsMatch(t, resp.Ids.GetIntId().Data, []int64{1, 2, 3})
	})

	t.Run("query leader", func(t *testing.T) {
		request := &querypb.QueryRequest{
			Req:        req,
			DmlChannel: defaultDMLChannel,
			SegmentIDs: []int64{},
		}

		resp, err := qs.query(context.Background(), request)
		assert.NoError(t, err)
		assert.ElementsMatch(t, resp.Ids.GetIntId().Data, []int64{}) // use mock node builder for now
		// assert.ElementsMatch(t, resp.Ids.GetIntId().Data, []int64{1, 2, 3}) // expected behavior in future
	})
}
