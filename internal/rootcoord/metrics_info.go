// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package rootcoord

import (
	"context"
	"os"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func (c *Core) getSystemInfoMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	rootCoordTopology := metricsinfo.RootCoordTopology{
		Self: metricsinfo.RootCoordInfos{
			BaseComponentInfos: metricsinfo.BaseComponentInfos{
				Name: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, c.session.ServerID),
				HardwareInfos: metricsinfo.HardwareMetrics{
					IP:           c.session.Address,
					CPUCoreCount: metricsinfo.GetCPUCoreCount(false),
					CPUCoreUsage: metricsinfo.GetCPUUsage(),
					Memory:       metricsinfo.GetMemoryCount(),
					MemoryUsage:  metricsinfo.GetUsedMemoryCount(),
					Disk:         metricsinfo.GetDiskCount(),
					DiskUsage:    metricsinfo.GetDiskUsage(),
				},
				SystemInfo: metricsinfo.DeployMetrics{
					SystemVersion: os.Getenv(metricsinfo.GitCommitEnvKey),
					DeployMode:    os.Getenv(metricsinfo.DeployModeEnvKey),
				},
				CreatedTime: Params.CreatedTime.String(),
				UpdatedTime: Params.UpdatedTime.String(),
				Type:        typeutil.RootCoordRole,
				ID:          c.session.ServerID,
			},
			SystemConfigurations: metricsinfo.RootCoordConfiguration{
				MinSegmentSizeToEnableIndex: Params.MinSegmentSizeToEnableIndex,
			},
		},
		Connections: metricsinfo.ConnTopology{
			Name: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, c.session.ServerID),
			// TODO(dragondriver): fill ConnectedComponents if necessary
			ConnectedComponents: []metricsinfo.ConnectionInfo{},
		},
	}

	resp, err := metricsinfo.MarshalTopology(rootCoordTopology)
	if err != nil {
		log.Warn("Failed to marshal system info metrics of root coordinator",
			zap.Error(err))

		return &milvuspb.GetMetricsResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_UnexpectedError,
				Reason:    err.Error(),
			},
			Response:      "",
			ComponentName: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, c.session.ServerID),
		}, nil
	}

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.RootCoordRole, c.session.ServerID),
	}, nil
}
