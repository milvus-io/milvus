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

package balance

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindShardDiskMoveCandidateProjectsInflightShard(t *testing.T) {
	rwNodes := []int64{1, 2, 3}
	nodeDiskBytes := map[int64]float64{
		1: 800,
		2: 500,
		3: 10,
	}
	shardNodeDiskBytes := map[string]map[int64]float64{
		"shard-1": {1: 600},
		"shard-2": {2: 500},
		"shard-3": {3: 10},
		"shard-4": {1: 200},
	}
	busyShards := map[string]struct{}{
		"shard-2": {},
	}

	projectShardDiskPlacement(nodeDiskBytes, shardNodeDiskBytes, "shard-2", 3)
	candidate, ok := findShardDiskMoveCandidate(rwNodes, nodeDiskBytes, shardNodeDiskBytes, busyShards)

	require.True(t, ok)
	assert.Equal(t, "shard-1", candidate.shard)
	assert.Equal(t, int64(2), candidate.target)
}

func TestFindShardDiskMoveCandidateRejectsNonImprovingMove(t *testing.T) {
	rwNodes := []int64{1, 2}
	nodeDiskBytes := map[int64]float64{
		1: 1000,
		2: 10,
	}
	shardNodeDiskBytes := map[string]map[int64]float64{
		"shard-1": {1: 1000},
		"shard-2": {2: 10},
	}

	_, ok := findShardDiskMoveCandidate(rwNodes, nodeDiskBytes, shardNodeDiskBytes, nil)

	assert.False(t, ok)
}
