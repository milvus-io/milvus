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
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// CreateSnapshot creates a snapshot for the specified collection
func (c *Client) CreateSnapshot(ctx context.Context, opt CreateSnapshotOption, callOptions ...grpc.CallOption) error {
	if opt == nil {
		return merr.WrapErrParameterInvalid("CreateSnapshotOption", "nil", "option cannot be nil")
	}
	req := opt.Request()

	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.CreateSnapshot(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}

// DropSnapshot drops a snapshot by name
func (c *Client) DropSnapshot(ctx context.Context, opt DropSnapshotOption, callOptions ...grpc.CallOption) error {
	if opt == nil {
		return merr.WrapErrParameterInvalid("DropSnapshotOption", "nil", "option cannot be nil")
	}
	req := opt.Request()

	return c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.DropSnapshot(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})
}

// ListSnapshots lists all snapshots for the specified collection or all snapshots if no collection is specified
func (c *Client) ListSnapshots(ctx context.Context, opt ListSnapshotsOption, callOptions ...grpc.CallOption) ([]string, error) {
	if opt == nil {
		return nil, merr.WrapErrParameterInvalid("ListSnapshotsOption", "nil", "option cannot be nil")
	}
	req := opt.Request()

	var snapshots []string

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.ListSnapshots(ctx, req, callOptions...)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		snapshots = resp.GetSnapshots()
		return nil
	})

	return snapshots, err
}

// DescribeSnapshot describes a snapshot by name
func (c *Client) DescribeSnapshot(ctx context.Context, opt DescribeSnapshotOption, callOptions ...grpc.CallOption) (*milvuspb.DescribeSnapshotResponse, error) {
	if opt == nil {
		return nil, merr.WrapErrParameterInvalid("DescribeSnapshotOption", "nil", "option cannot be nil")
	}
	req := opt.Request()

	var resp *milvuspb.DescribeSnapshotResponse

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		var err error
		resp, err = milvusService.DescribeSnapshot(ctx, req, callOptions...)
		return merr.CheckRPCCall(resp, err)
	})

	return resp, err
}

// RestoreSnapshot restores a snapshot to a target collection and returns the job ID for tracking
func (c *Client) RestoreSnapshot(ctx context.Context, opt RestoreSnapshotOption, callOptions ...grpc.CallOption) (int64, error) {
	if opt == nil {
		return 0, merr.WrapErrParameterInvalid("RestoreSnapshotOption", "nil", "option cannot be nil")
	}
	req := opt.Request()

	var jobID int64

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.RestoreSnapshot(ctx, req, callOptions...)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		jobID = resp.GetJobId()
		return nil
	})

	return jobID, err
}

// GetRestoreSnapshotState gets the state of a restore snapshot job
func (c *Client) GetRestoreSnapshotState(ctx context.Context, opt GetRestoreSnapshotStateOption, callOptions ...grpc.CallOption) (*milvuspb.RestoreSnapshotInfo, error) {
	if opt == nil {
		return nil, merr.WrapErrParameterInvalid("GetRestoreSnapshotStateOption", "nil", "option cannot be nil")
	}
	req := opt.Request()

	var info *milvuspb.RestoreSnapshotInfo

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.GetRestoreSnapshotState(ctx, req, callOptions...)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		info = resp.GetInfo()
		return nil
	})

	return info, err
}

// ListRestoreSnapshotJobs lists all restore snapshot jobs, optionally filtered by collection name
func (c *Client) ListRestoreSnapshotJobs(ctx context.Context, opt ListRestoreSnapshotJobsOption, callOptions ...grpc.CallOption) ([]*milvuspb.RestoreSnapshotInfo, error) {
	if opt == nil {
		return nil, merr.WrapErrParameterInvalid("ListRestoreSnapshotJobsOption", "nil", "option cannot be nil")
	}
	req := opt.Request()

	var jobs []*milvuspb.RestoreSnapshotInfo

	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.ListRestoreSnapshotJobs(ctx, req, callOptions...)
		if err = merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		jobs = resp.GetJobs()
		return nil
	})

	return jobs, err
}
