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
	"errors"
	"fmt"
	"math"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type queryShard struct {
	ctx    context.Context
	cancel context.CancelFunc

	collectionID UniqueID
	collection   *Collection // quick reference from meta
	channel      Channel
	deltaChannel Channel
	replicaID    int64

	clusterService *ShardClusterService

	tSafeReplica TSafeReplicaInterface
	historical   ReplicaInterface
	streaming    ReplicaInterface

	vectorChunkManager *storage.VectorChunkManager
	localCacheEnabled  bool
	localCacheSize     int64
}

func newQueryShard(
	ctx context.Context,
	collectionID UniqueID,
	channel Channel,
	replicaID int64,
	clusterService *ShardClusterService,
	historical ReplicaInterface,
	streaming ReplicaInterface,
	tSafeReplica TSafeReplicaInterface,
	localChunkManager storage.ChunkManager,
	remoteChunkManager storage.ChunkManager,
	localCacheEnabled bool,
) (*queryShard, error) {

	collection, err := streaming.getCollectionByID(collectionID)
	if err != nil {
		return nil, err
	}
	if localChunkManager == nil {
		return nil, fmt.Errorf("can not create vector chunk manager for local chunk manager is nil")
	}
	if remoteChunkManager == nil {
		return nil, fmt.Errorf("can not create vector chunk manager for remote chunk manager is nil")
	}
	vectorChunkManager, err := storage.NewVectorChunkManager(localChunkManager, remoteChunkManager,
		&etcdpb.CollectionMeta{
			ID:     collectionID,
			Schema: collection.schema,
		}, Params.QueryNodeCfg.CacheMemoryLimit, localCacheEnabled)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	qs := &queryShard{
		ctx:                ctx,
		cancel:             cancel,
		collectionID:       collectionID,
		collection:         collection,
		channel:            channel,
		replicaID:          replicaID,
		clusterService:     clusterService,
		historical:         historical,
		streaming:          streaming,
		vectorChunkManager: vectorChunkManager,
		tSafeReplica:       tSafeReplica,
	}
	deltaChannel, err := funcutil.ConvertChannelName(channel, Params.CommonCfg.RootCoordDml, Params.CommonCfg.RootCoordDelta)
	if err != nil {
		log.Warn("failed to convert dm channel to delta", zap.String("channel", channel), zap.Error(err))
	}
	qs.deltaChannel = deltaChannel

	return qs, nil
}

// Close cleans query shard
func (q *queryShard) Close() {
	q.cancel()
}

type tsType int32

const (
	tsTypeDML   tsType = 1
	tsTypeDelta tsType = 2
)

func (tp tsType) String() string {
	switch tp {
	case tsTypeDML:
		return "DML tSafe"
	case tsTypeDelta:
		return "Delta tSafe"
	}
	return ""
}

func (q *queryShard) getNewTSafe(tp tsType) (Timestamp, error) {
	var channel string
	switch tp {
	case tsTypeDML:
		channel = q.channel
	case tsTypeDelta:
		channel = q.deltaChannel
	default:
		return 0, errors.New("invalid ts type")
	}
	t := Timestamp(math.MaxInt64)
	ts, err := q.tSafeReplica.getTSafe(channel)
	if err != nil {
		return 0, err
	}
	if ts <= t {
		t = ts
	}
	return t, nil
}

func (q *queryShard) getServiceableTime(tp tsType) (Timestamp, error) {
	var channel string
	switch tp {
	case tsTypeDML:
		channel = q.channel
	case tsTypeDelta:
		channel = q.deltaChannel
	}

	ts, err := q.tSafeReplica.getTSafe(channel)
	if err != nil {
		return 0, err
	}
	gracefulTimeInMilliSecond := Params.QueryNodeCfg.GracefulTime
	gracefulTime := typeutil.ZeroTimestamp
	if gracefulTimeInMilliSecond > 0 {
		gracefulTime = tsoutil.ComposeTS(gracefulTimeInMilliSecond, 0)
	}
	return (ts + gracefulTime), nil
}
