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

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/types"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

// Mock is an alternative to IndexNode, it will return specific results based on specific parameters.
type Mock struct {
	types.IndexNode

	createJob func(ctx context.Context, req *indexpb.CreateJobRequest) (*commonpb.Status, error)
	queryJobs func(ctx context.Context, in *indexpb.QueryJobsRequest) (*indexpb.QueryJobsResponse, error)
	dropJobs  func(ctx context.Context, in *indexpb.DropJobsRequest, opts ...grpc.CallOption) (*commonpb.Status, error)
	getJobNum func(ctx context.Context, in *indexpb.GetJobNumRequest, opts ...grpc.CallOption) (*indexpb.GetJobNumResponse, error)
}

func (m *Mock) CreateJob(ctx context.Context, req *indexpb.CreateJobRequest) (*commonpb.Status, error) {
	return m.createJob(ctx, req)
}

func (m *Mock) QueryJobs(ctx context.Context, req *indexpb.QueryJobsRequest) (*indexpb.QueryJobsResponse, error) {
	return m.queryJobs(ctx, req)
}

func (m *Mock) DropJobs(ctx context.Context, req *indexpb.DropJobsRequest) (*commonpb.Status, error) {
	return m.dropJobs(ctx, req)
}

func (m *Mock) GetJobNum(ctx context.Context, req *indexpb.GetJobNumRequest) (*indexpb.GetJobNumResponse, error) {
	return m.getJobNum(ctx, req)
}

func getMockSystemInfoMetrics(
	ctx context.Context,
	req *milvuspb.GetMetricsRequest,
	node *Mock,
) (*milvuspb.GetMetricsResponse, error) {
	// TODO(dragondriver): add more metrics
	nodeInfos := metricsinfo.IndexNodeInfos{
		BaseComponentInfos: metricsinfo.BaseComponentInfos{
			Name: metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, Params.IndexNodeCfg.GetNodeID()),
			HardwareInfos: metricsinfo.HardwareMetrics{
				CPUCoreCount: metricsinfo.GetCPUCoreCount(false),
				CPUCoreUsage: metricsinfo.GetCPUUsage(),
				Memory:       1000,
				MemoryUsage:  metricsinfo.GetUsedMemoryCount(),
				Disk:         metricsinfo.GetDiskCount(),
				DiskUsage:    metricsinfo.GetDiskUsage(),
			},
			SystemInfo:  metricsinfo.DeployMetrics{},
			CreatedTime: Params.IndexNodeCfg.CreatedTime.String(),
			UpdatedTime: Params.IndexNodeCfg.UpdatedTime.String(),
			Type:        typeutil.IndexNodeRole,
		},
		SystemConfigurations: metricsinfo.IndexNodeConfiguration{
			MinioBucketName: Params.MinioCfg.BucketName,
			SimdType:        Params.CommonCfg.SimdType,
		},
	}

	metricsinfo.FillDeployMetricsWithEnv(&nodeInfos.SystemInfo)

	resp, _ := metricsinfo.MarshalComponentInfos(nodeInfos)

	return &milvuspb.GetMetricsResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
			Reason:    "",
		},
		Response:      resp,
		ComponentName: metricsinfo.ConstructComponentName(typeutil.IndexNodeRole, Params.IndexNodeCfg.GetNodeID()),
	}, nil
}

type MockIndexNode struct {
	types.IndexNode

	CreateIndexMock  func(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error)
	GetTaskSlotsMock func(ctx context.Context, req *indexpb.GetTaskSlotsRequest) (*indexpb.GetTaskSlotsResponse, error)
	GetMetricsMock   func(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error)
}

func (min *MockIndexNode) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) (*commonpb.Status, error) {
	return min.CreateIndexMock(ctx, req)
}

func (min *MockIndexNode) GetTaskSlots(ctx context.Context, req *indexpb.GetTaskSlotsRequest) (*indexpb.GetTaskSlotsResponse, error) {
	return min.GetTaskSlotsMock(ctx, req)
}

func (min *MockIndexNode) GetMetrics(ctx context.Context, req *milvuspb.GetMetricsRequest) (*milvuspb.GetMetricsResponse, error) {
	return min.GetMetricsMock(ctx, req)
}
