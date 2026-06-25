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

package datanode

import (
	"github.com/milvus-io/milvus/internal/datanode/compactor"
	"github.com/milvus-io/milvus/internal/datanode/importv2"
	"github.com/milvus-io/milvus/internal/datanode/index"
	"github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
)

// collectPoolStats returns current thread-count stats for the DataNode's
// long-lived goroutine pools. It is registered with metrics.SetDataNodePoolCollectFn
// and invoked at Prometheus scrape time (pull model), mirroring the QueryNode path.
func collectPoolStats() []metrics.PoolStats {
	type poolRef struct {
		name string
		pool interface {
			Cap() int
			Running() int
			Waiting() int
		}
	}

	refs := []poolRef{
		{"CompactionExecPool", compactor.GetExecPool()},
		{"IndexBuildPool", index.GetVecIndexBuildPool()},
		{"ImportExecPool", importv2.GetExecPool()},
		{"IOPool", io.GetOrCreateIOPool()},
		{"StatsPool", io.GetOrCreateStatsPool()},
	}

	stats := make([]metrics.PoolStats, 0, len(refs))
	for _, r := range refs {
		stats = append(stats, metrics.PoolStats{
			Name:    r.name,
			Cap:     r.pool.Cap(),
			Running: r.pool.Running(),
			Waiting: r.pool.Waiting(),
		})
	}
	return stats
}
