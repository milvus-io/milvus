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

package session

import "github.com/milvus-io/milvus/pkg/v3/proto/querypb"

type stats struct {
	segmentCnt               int
	channelCnt               int
	memCapacityInMB          float64
	CPUNum                   int64
	cacheShardDiskUsageStats []*querypb.CacheShardDiskUsageStats
}

func (s *stats) setSegmentCnt(cnt int) {
	s.segmentCnt = cnt
}

func (s *stats) getSegmentCnt() int {
	return s.segmentCnt
}

func (s *stats) setChannelCnt(cnt int) {
	s.channelCnt = cnt
}

func (s *stats) getChannelCnt() int {
	return s.channelCnt
}

func (s *stats) setMemCapacity(capacity float64) {
	s.memCapacityInMB = capacity
}

func (s *stats) getMemCapacity() float64 {
	return s.memCapacityInMB
}

func (s *stats) setCPUNum(num int64) {
	s.CPUNum = num
}

func (s *stats) getCPUNum() int64 {
	return s.CPUNum
}

func (s *stats) setCacheShardDiskUsageStats(stats []*querypb.CacheShardDiskUsageStats) {
	s.cacheShardDiskUsageStats = cloneCacheShardDiskUsageStats(stats)
}

func (s *stats) getCacheShardDiskUsageStats() []*querypb.CacheShardDiskUsageStats {
	return cloneCacheShardDiskUsageStats(s.cacheShardDiskUsageStats)
}

func newStats() stats {
	return stats{}
}

func cloneCacheShardDiskUsageStats(stats []*querypb.CacheShardDiskUsageStats) []*querypb.CacheShardDiskUsageStats {
	if len(stats) == 0 {
		return nil
	}

	cloned := make([]*querypb.CacheShardDiskUsageStats, 0, len(stats))
	for _, stat := range stats {
		if stat == nil {
			cloned = append(cloned, nil)
			continue
		}
		cloned = append(cloned, &querypb.CacheShardDiskUsageStats{
			DataType:  stat.GetDataType(),
			Shard:     stat.GetShard(),
			DiskBytes: stat.GetDiskBytes(),
		})
	}
	return cloned
}

func (n *NodeInfo) CacheShardDiskUsageStats() []*querypb.CacheShardDiskUsageStats {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.getCacheShardDiskUsageStats()
}

func WithCacheShardDiskUsageStats(stats []*querypb.CacheShardDiskUsageStats) StatsOption {
	return func(n *NodeInfo) {
		n.setCacheShardDiskUsageStats(stats)
	}
}
