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

package segments

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/segcore"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type diskUsageFetcher struct {
	ctx   context.Context
	path  string
	usage *atomic.Int64
	err   *atomic.Error
}

func NewDiskUsageFetcher(ctx context.Context) *diskUsageFetcher {
	return &diskUsageFetcher{
		ctx:   ctx,
		path:  paramtable.Get().LocalStorageCfg.Path.GetValue(),
		usage: atomic.NewInt64(0),
		err:   atomic.NewError(nil),
	}
}

func (d *diskUsageFetcher) GetDiskUsage() (int64, error) {
	return d.usage.Load(), d.err.Load()
}

func (d *diskUsageFetcher) fetch() {
	diskUsage, err := segcore.GetLocalUsedSize(d.path)
	if err != nil {
		log.Warn("failed to get disk usage", zap.Error(err))
		d.err.Store(err)
		return
	}
	d.usage.Store(diskUsage)
	d.err.Store(nil)
	metrics.QueryNodeDiskUsedSize.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Set(float64(diskUsage) / 1024 / 1024) // in MB
	log.Ctx(d.ctx).WithRateGroup("diskUsageFetcher", 1, 300).
		RatedInfo(300, "querynode disk usage", zap.Int64("size", diskUsage), zap.Int64("nodeID", paramtable.GetNodeID()))
}

func (d *diskUsageFetcher) Start() {
	d.fetch() // Synchronously fetch once before starting.

	interval := paramtable.Get().QueryNodeCfg.DiskSizeFetchInterval.GetAsDuration(time.Second)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			d.fetch()
		}
	}
}
