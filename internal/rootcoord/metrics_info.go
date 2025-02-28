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

package rootcoord

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (c *Core) getSystemInfoMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (string, error) {
	used, total, err := hardware.GetDiskUsage(paramtable.Get().LocalStorageCfg.Path.GetValue())
	if err != nil {
		log.Ctx(ctx).Warn("get disk usage failed", zap.Error(err))
	}

	ioWait, err := hardware.GetIOWait()
	if err != nil {
		log.Ctx(ctx).Warn("get iowait failed", zap.Error(err))
	}

	rootCoordTopology := metricsinfo.RootCoordTopology{
		Self: metricsinfo.RootCoordInfos{
			BaseComponentInfos: metricsinfo.BaseComponentInfos{
				Name: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, c.session.ServerID),
				HardwareInfos: metricsinfo.HardwareMetrics{
					IP:               c.session.Address,
					CPUCoreCount:     hardware.GetCPUNum(),
					CPUCoreUsage:     hardware.GetCPUUsage(),
					Memory:           hardware.GetMemoryCount(),
					MemoryUsage:      hardware.GetUsedMemoryCount(),
					Disk:             total,
					DiskUsage:        used,
					IOWaitPercentage: ioWait,
				},
				SystemInfo:  metricsinfo.DeployMetrics{},
				CreatedTime: paramtable.GetCreateTime().String(),
				UpdatedTime: paramtable.GetUpdateTime().String(),
				Type:        typeutil.RootCoordRole,
				ID:          c.session.ServerID,
			},
			SystemConfigurations: metricsinfo.RootCoordConfiguration{
				MinSegmentSizeToEnableIndex: Params.RootCoordCfg.MinSegmentSizeToEnableIndex.GetAsInt64(),
			},
		},
		Connections: metricsinfo.ConnTopology{
			Name: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, c.session.ServerID),
			// TODO(dragondriver): fill ConnectedComponents if necessary
			ConnectedComponents: []metricsinfo.ConnectionInfo{},
		},
	}
	metricsinfo.FillDeployMetricsWithEnv(&rootCoordTopology.Self.SystemInfo)
	return metricsinfo.MarshalTopology(rootCoordTopology)
}
