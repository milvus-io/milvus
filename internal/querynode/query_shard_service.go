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
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/dependency"
	"go.uber.org/zap"
)

type queryShardService struct {
	ctx    context.Context
	cancel context.CancelFunc

	queryShardsMu sync.Mutex              // guards queryShards
	queryShards   map[Channel]*queryShard // Virtual Channel -> *queryShard

	queryChannelMu sync.Mutex              // guards queryChannels
	queryChannels  map[int64]*queryChannel // Collection ID -> query channel

	factory dependency.Factory

	historical *historical
	streaming  *streaming

	shardClusterService *ShardClusterService
	localChunkManager   storage.ChunkManager
	remoteChunkManager  storage.ChunkManager
	localCacheEnabled   bool
}

func newQueryShardService(ctx context.Context, historical *historical, streaming *streaming, clusterService *ShardClusterService, factory dependency.Factory) *queryShardService {
	queryShardServiceCtx, queryShardServiceCancel := context.WithCancel(ctx)

	path := Params.LoadWithDefault("localStorage.Path", "/tmp/milvus/data")

	localChunkManager := storage.NewLocalChunkManager(storage.RootPath(path))
	remoteChunkManager, _ := factory.NewVectorStorageChunkManager(ctx)

	qss := &queryShardService{
		ctx:                 queryShardServiceCtx,
		cancel:              queryShardServiceCancel,
		queryShards:         make(map[Channel]*queryShard),
		queryChannels:       make(map[int64]*queryChannel),
		historical:          historical,
		streaming:           streaming,
		shardClusterService: clusterService,
		localChunkManager:   localChunkManager,
		remoteChunkManager:  remoteChunkManager,
		localCacheEnabled:   Params.QueryNodeCfg.CacheEnabled,
		factory:             factory,
	}
	return qss
}

func (q *queryShardService) addQueryShard(collectionID UniqueID, channel Channel, replicaID int64) error {
	q.queryShardsMu.Lock()
	defer q.queryShardsMu.Unlock()
	if _, ok := q.queryShards[channel]; ok {
		return errors.New(fmt.Sprintln("query shard(channel) ", channel, " already exists"))
	}
	qs, err := newQueryShard(
		q.ctx,
		collectionID,
		channel,
		replicaID,
		q.shardClusterService,
		q.historical,
		q.streaming,
		q.localChunkManager,
		q.remoteChunkManager,
		q.localCacheEnabled,
	)
	if err != nil {
		return err
	}
	q.queryShards[channel] = qs
	log.Info("Successfully add query shard", zap.Int64("collection", collectionID), zap.Int64("replica", replicaID), zap.String("channel", channel))
	return nil
}

func (q *queryShardService) removeQueryShard(channel Channel) error {
	q.queryShardsMu.Lock()
	defer q.queryShardsMu.Unlock()
	if _, ok := q.queryShards[channel]; !ok {
		return errors.New(fmt.Sprintln("query shard(channel) ", channel, " does not exist"))
	}
	delete(q.queryShards, channel)
	log.Info("Successfully remove query shard", zap.String("channel", channel))
	return nil
}

func (q *queryShardService) hasQueryShard(channel Channel) bool {
	q.queryShardsMu.Lock()
	defer q.queryShardsMu.Unlock()
	_, found := q.queryShards[channel]
	return found
}

func (q *queryShardService) getQueryShard(channel Channel) (*queryShard, error) {
	q.queryShardsMu.Lock()
	defer q.queryShardsMu.Unlock()
	if _, ok := q.queryShards[channel]; !ok {
		return nil, errors.New(fmt.Sprintln("query shard(channel) ", channel, " does not exist"))
	}
	return q.queryShards[channel], nil
}

func (q *queryShardService) close() {
	log.Warn("Close query shard service")
	q.cancel()
	q.queryShardsMu.Lock()
	defer q.queryShardsMu.Unlock()

	for _, queryShard := range q.queryShards {
		queryShard.Close()
	}
}

func (q *queryShardService) getQueryChannel(collectionID int64) *queryChannel {
	q.queryChannelMu.Lock()
	defer q.queryChannelMu.Unlock()

	qc, ok := q.queryChannels[collectionID]
	if !ok {
		queryStream, _ := q.factory.NewQueryMsgStream(q.ctx)
		qc = NewQueryChannel(collectionID, q.shardClusterService, queryStream, q.streaming)
		q.queryChannels[collectionID] = qc
	}

	return qc
}

func (q *queryShardService) releaseCollection(collectionID int64) {
	q.queryChannelMu.Lock()
	qc, ok := q.queryChannels[collectionID]
	if ok && qc != nil {
		qc.Stop()
		delete(q.queryChannels, collectionID)
	}
	q.queryChannelMu.Unlock()

	q.queryShardsMu.Lock()
	for channel, queryShard := range q.queryShards {
		if queryShard.collectionID == collectionID {
			queryShard.Close()
			delete(q.queryShards, channel)
		}
	}
	q.queryShardsMu.Unlock()
	log.Info("release collection in query shard service", zap.Int64("collectionId", collectionID))
}
