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

package indexnode

import (
	"context"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// TODO(dragondriver): maybe IndexNode should be an interface so that we can mock it in the test cases
func getSystemInfoMetrics(
	ctx context.Context,
	req *milvuspb.GetMetricsRequest,
	node *IndexNode,
) (*milvuspb.GetMetricsResponse, error) {
	// TODO(dragondriver): add more metrics

	used, total, err := hardware.GetDiskUsage(paramtable.Get().LocalStorageCfg.Path.GetValue())
	if err != nil {
		log.Ctx(ctx).Warn("get disk usage failed", zap.Error(err))
	}

	iowait, err := hardware.GetIOWait()
	if err != nil {
		log.Ctx(ctx).Warn("get iowait failed", zap.Error(err))
	}

	nodeInfos := metricsinfo.IndexNodeInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			Name: metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, paramtable.GetNodeID()),

			HardwareInfos: metricsinfo.HardwareMetrics{
				IP:               node.session.Address,
				CPUCoreCount:     hardware.GetCPUNum(),
				CPUCoreUsage:     hardware.GetCPUUsage(),
				Memory:           hardware.GetMemoryCount(),
				MemoryUsage:      hardware.GetUsedMemoryCount(),
				Disk:             total,
				DiskUsage:        used,
				IOWaitPercentage: iowait,
			},
			SystemInfo:  metricsinfo.DeployMetrics{},
			CreatedTime: paramtable.GetCreateTime().String(),
			UpdatedTime: paramtable.GetUpdateTime().String(),
			Type:        typeutil.IndexNodeRole,
			ID:          node.session.ServerID,
		},
		SystemConfigurations: metricsinfo.IndexNodeConfiguration{
			MinioBucketName: Params.MinioCfg.BucketName.GetValue(),
			SimdType:        Params.CommonCfg.SimdType.GetValue(),
		},
	}

	metricsinfo.FillDeployMetricsWithEnv(&nodeInfos.SystemInfo)

	resp, err := metricsinfo.MarshalComponentInfos(nodeInfos)
	if err != nil {
		return &milvuspb.GetMetricsResponse{
			Status:        merr.Status(err),
			Response:      "",
			ComponentName: metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, paramtable.GetNodeID()),
		}, nil
	}

	return &milvuspb.GetMetricsResponse{
		Status:        merr.Success(),
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, paramtable.GetNodeID()),
	}, nil
}
