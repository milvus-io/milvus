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

package milvusclient

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/client/v2/entity"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// RefreshExternalCollectionResult contains the result of a refresh external collection operation.
type RefreshExternalCollectionResult struct {
	JobID int64
}

// RefreshExternalCollection triggers a refresh job for an external collection.
// Returns a job ID that can be used to track the progress of the refresh operation.
func (c *Client) RefreshExternalCollection(ctx context.Context, option RefreshExternalCollectionOption, callOptions ...grpc.CallOption) (*RefreshExternalCollectionResult, error) {
	req := option.Request()

	var result RefreshExternalCollectionResult

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.RefreshExternalCollection(ctx, req, callOptions...)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}

		result.JobID = resp.GetJobId()
		return nil
	})

	return &result, err
}

// GetRefreshExternalCollectionProgress returns the progress of a refresh external collection job.
func (c *Client) GetRefreshExternalCollectionProgress(ctx context.Context, option GetRefreshExternalCollectionProgressOption, callOptions ...grpc.CallOption) (*entity.RefreshExternalCollectionJobInfo, error) {
	req := option.Request()

	var jobInfo *entity.RefreshExternalCollectionJobInfo

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.GetRefreshExternalCollectionProgress(ctx, req, callOptions...)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}

		jobInfo = convertToEntityJobInfo(resp.GetJobInfo())
		return nil
	})

	return jobInfo, err
}

// ListRefreshExternalCollectionJobs returns a list of refresh external collection jobs for a collection.
func (c *Client) ListRefreshExternalCollectionJobs(ctx context.Context, option ListRefreshExternalCollectionJobsOption, callOptions ...grpc.CallOption) ([]*entity.RefreshExternalCollectionJobInfo, error) {
	req := option.Request()

	var jobs []*entity.RefreshExternalCollectionJobInfo

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.ListRefreshExternalCollectionJobs(ctx, req, callOptions...)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}

		jobs = make([]*entity.RefreshExternalCollectionJobInfo, 0, len(resp.GetJobs()))
		for _, job := range resp.GetJobs() {
			jobs = append(jobs, convertToEntityJobInfo(job))
		}
		return nil
	})

	return jobs, err
}

// convertToEntityJobInfo converts a proto RefreshExternalCollectionJobInfo to entity.RefreshExternalCollectionJobInfo.
func convertToEntityJobInfo(info *milvuspb.RefreshExternalCollectionJobInfo) *entity.RefreshExternalCollectionJobInfo {
	if info == nil {
		return nil
	}
	return &entity.RefreshExternalCollectionJobInfo{
		JobID:          info.GetJobId(),
		CollectionName: info.GetCollectionName(),
		State:          entity.RefreshExternalCollectionState(info.GetState()),
		Progress:       info.GetProgress(),
		Reason:         info.GetReason(),
		ExternalSource: info.GetExternalSource(),
		StartTime:      info.GetStartTime(),
		EndTime:        info.GetEndTime(),
	}
}
