// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package shardclient

import (
	"math/rand"

	"github.com/samber/lo"
	"go.uber.org/atomic"
)

// shardLeaders wraps shard leader mapping for iteration.
type shardLeaders struct {
	idx          *atomic.Int64
	collectionID int64
	shardLeaders map[string][]NodeInfo
}

func (sl *shardLeaders) Get(channel string) []NodeInfo {
	return sl.shardLeaders[channel]
}

func (sl *shardLeaders) GetShardLeaderList() []string {
	return lo.Keys(sl.shardLeaders)
}

type shardLeadersReader struct {
	leaders *shardLeaders
	idx     int64
}

// Shuffle returns the shuffled shard leader list.
func (it shardLeadersReader) Shuffle() map[string][]NodeInfo {
	result := make(map[string][]NodeInfo)
	for channel, leaders := range it.leaders.shardLeaders {
		l := len(leaders)
		// shuffle all replica at random order
		shuffled := make([]NodeInfo, l)
		for i, randIndex := range rand.Perm(l) {
			shuffled[i] = leaders[randIndex]
		}

		// make each copy has same probability to be first replica
		for index, leader := range shuffled {
			if leader == leaders[int(it.idx)%l] {
				shuffled[0], shuffled[index] = shuffled[index], shuffled[0]
			}
		}

		result[channel] = shuffled
	}
	return result
}

// GetReader returns shuffer reader for shard leader.
func (sl *shardLeaders) GetReader() shardLeadersReader {
	idx := sl.idx.Inc()
	return shardLeadersReader{
		leaders: sl,
		idx:     idx,
	}
}
