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
)

type queryShardService struct {
	ctx    context.Context
	cancel context.CancelFunc

	queryShardsMu sync.Mutex              // guards queryShards
	queryShards   map[Channel]*queryShard // Virtual Channel -> *queryShard
}

func newQueryShardService(ctx context.Context) *queryShardService {
	queryShardServiceCtx, queryShardServiceCancel := context.WithCancel(ctx)
	qss := &queryShardService{
		ctx:         queryShardServiceCtx,
		cancel:      queryShardServiceCancel,
		queryShards: make(map[Channel]*queryShard),
	}
	return qss
}

func (q *queryShardService) addQueryShard(collectionID UniqueID, channel Channel) error {
	q.queryShardsMu.Lock()
	defer q.queryShardsMu.Unlock()
	if _, ok := q.queryShards[channel]; ok {
		return errors.New(fmt.Sprintln("query shard(channel) ", channel, " already exists"))
	}
	qs := newQueryShard(
		q.ctx,
		collectionID,
		channel,
	)
	q.queryShards[channel] = qs
	return nil
}

func (q *queryShardService) removeQueryShard(channel Channel) error {
	q.queryShardsMu.Lock()
	defer q.queryShardsMu.Unlock()
	if _, ok := q.queryShards[channel]; !ok {
		return errors.New(fmt.Sprintln("query shard(channel) ", channel, " does not exist"))
	}
	delete(q.queryShards, channel)
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
