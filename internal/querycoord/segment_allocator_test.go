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

package querycoord

import (
	"context"
	"errors"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/internal/util/etcd"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func TestShuffleSegmentsToQueryNode(t *testing.T) {
	refreshParams()
	baseCtx, cancel := context.WithCancel(context.Background())
	etcdCli, err := etcd.GetEtcdClient(&Params.EtcdCfg)
	defer etcdCli.Close()
	assert.Nil(t, err)
	kv := etcdkv.NewEtcdKV(etcdCli, Params.EtcdCfg.MetaRootPath)
	clusterSession := sessionutil.NewSession(context.Background(), Params.EtcdCfg.MetaRootPath, etcdCli)
	clusterSession.Init(typeutil.QueryCoordRole, Params.QueryCoordCfg.Address, true, false)
	factory := dependency.NewDefaultFactory(true) //msgstream.NewPmsFactory()

	id := UniqueID(rand.Int31())
	idAllocator := func() (UniqueID, error) {
		newID := atomic.AddInt64(&id, 1)
		return newID, nil
	}
	meta, err := newMeta(baseCtx, kv, factory, idAllocator)
	assert.Nil(t, err)
	cleaner, err := NewChannelCleaner(baseCtx, kv, factory)
	assert.Nil(t, err)
	cluster := &queryNodeCluster{
		ctx:         baseCtx,
		cancel:      cancel,
		client:      kv,
		clusterMeta: meta,
		cleaner:     cleaner,
		nodes:       make(map[int64]Node),
		newNodeFn:   newQueryNodeTest,
		session:     clusterSession,
	}

	schema := genDefaultCollectionSchema(false)
	firstReq := &querypb.LoadSegmentsRequest{
		CollectionID: defaultCollectionID,
		Schema:       schema,
		Infos: []*querypb.SegmentLoadInfo{
			{
				SegmentID:    defaultSegmentID,
				PartitionID:  defaultPartitionID,
				CollectionID: defaultCollectionID,
				NumOfRows:    defaultNumRowPerSegment,
			},
		},
	}
	secondReq := &querypb.LoadSegmentsRequest{
		CollectionID: defaultCollectionID,
		Schema:       schema,
		Infos: []*querypb.SegmentLoadInfo{
			{
				SegmentID:    defaultSegmentID + 1,
				PartitionID:  defaultPartitionID,
				CollectionID: defaultCollectionID,
				NumOfRows:    defaultNumRowPerSegment,
			},
		},
	}
	reqs := []*querypb.LoadSegmentsRequest{firstReq, secondReq}

	t.Run("Test shuffleSegmentsWithoutQueryNode", func(t *testing.T) {
		err = shuffleSegmentsToQueryNode(baseCtx, reqs, cluster, meta, false, nil, nil, -1)
		assert.NotNil(t, err)
	})

	node1, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)
	node1Session := node1.session
	node1ID := node1.queryNodeID
	cluster.RegisterNode(baseCtx, node1Session, node1ID, disConnect)
	waitQueryNodeOnline(cluster, node1ID)

	t.Run("Test shuffleSegmentsToQueryNode", func(t *testing.T) {
		err = shuffleSegmentsToQueryNode(baseCtx, reqs, cluster, meta, false, nil, nil, -1)
		assert.Nil(t, err)

		assert.Equal(t, node1ID, firstReq.DstNodeID)
		assert.Equal(t, node1ID, secondReq.DstNodeID)
	})

	node2, err := startQueryNodeServer(baseCtx)
	assert.Nil(t, err)
	node2Session := node2.session
	node2ID := node2.queryNodeID
	cluster.RegisterNode(baseCtx, node2Session, node2ID, disConnect)
	waitQueryNodeOnline(cluster, node2ID)
	cluster.StopNode(node1ID)

	t.Run("Test shuffleSegmentsToQueryNodeV2", func(t *testing.T) {
		err = shuffleSegmentsToQueryNodeV2(baseCtx, reqs, cluster, meta, false, nil, nil, -1)
		assert.Nil(t, err)

		assert.Equal(t, node2ID, firstReq.DstNodeID)
		assert.Equal(t, node2ID, secondReq.DstNodeID)

		err = shuffleSegmentsToQueryNodeV2(baseCtx, reqs, cluster, meta, true, nil, nil, -1)
		assert.Nil(t, err)

		assert.Equal(t, node2ID, firstReq.DstNodeID)
		assert.Equal(t, node2ID, secondReq.DstNodeID)
	})

	cluster.StopNode(node2ID)

	t.Run("Test shuffleSegmentsToQueryNodeV2 ctx", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err = shuffleSegmentsToQueryNodeV2(ctx, reqs, cluster, meta, true, nil, nil, -1)
		assert.Error(t, err)

		assert.True(t, errors.Is(err, context.Canceled))
	})

	err = removeAllSession()
	assert.Nil(t, err)
}

func Test_waitWithContext(t *testing.T) {
	t.Run("normal wait", func(t *testing.T) {
		ctx := context.Background()

		err := waitWithContext(ctx, time.Millisecond)
		assert.NoError(t, err)
	})

	t.Run("context canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := waitWithContext(ctx, time.Second)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))
	})

	t.Run("context deadline", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()

		err := waitWithContext(ctx, time.Second)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.DeadlineExceeded))
	})
}
