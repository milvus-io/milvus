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
	"fmt"
	"time"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

// CreateCompactionTarget submits a new target and returns its persisted ID and initial row count.
func (c *Client) CreateCompactionTarget(ctx context.Context, opt CreateCompactionTargetOption, callOptions ...grpc.CallOption) (*milvuspb.CreateCompactionTargetResponse, error) {
	if opt == nil {
		return nil, merr.WrapErrParameterMissingMsg("compaction target options")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	req := opt.Request()
	if req == nil {
		return nil, merr.WrapErrParameterMissingMsg("compaction target request")
	}
	if req.GetDbName() == "" {
		req.DbName = c.getCurrentDB()
	}

	// Disable both middleware retries and gRPC replay after sending the request.
	// Once the request can reach the server, a retry may create a second target.
	opts := append(append([]grpc.CallOption(nil), callOptions...), grpc_retry.Disable(), grpc.MaxRetryRPCBufferSize(0))
	var result *milvuspb.CreateCompactionTargetResponse
	err := c.callService(func(service milvuspb.MilvusServiceClient) error {
		resp, err := service.CreateCompactionTarget(ctx, req, opts...)
		if err != nil {
			switch status.Code(err) {
			case codes.Unimplemented, codes.Unauthenticated, codes.PermissionDenied, codes.InvalidArgument:
				return err
			default:
				return merr.WrapErrCompactionTargetCreateOutcomeUnknown("RPC ended with " + status.Code(err).String())
			}
		}
		if resp.GetStatus() == nil {
			return merr.WrapErrCompactionTargetCreateOutcomeUnknown("missing response status")
		}
		if err := merr.Error(resp.GetStatus()); err != nil {
			return err
		}
		if resp.GetTargetId() <= 0 || resp.GetTotalRows() < 0 {
			return merr.WrapErrCompactionTargetCreateOutcomeUnknown("invalid target ID or row count")
		}
		optimize := resp.GetOptimize()
		if requested := req.GetOptimize(); requested != nil {
			if optimize.GetInputTargetSizeBytes() <= 0 ||
				optimize.GetInputTargetSizeBytes() != requested.GetInputTargetSizeBytes() ||
				optimize.GetActualTargetSizeBytes() <= 0 {
				return merr.WrapErrCompactionTargetCreateOutcomeUnknown("invalid optimize sizes")
			}
		} else if optimize != nil || req.GetRewrite() == nil {
			return merr.WrapErrCompactionTargetCreateOutcomeUnknown("result does not match the requested target family")
		}
		result = resp
		return nil
	})
	return result, err
}

// DescribeCompactionTarget returns the persisted target and its current row estimate, when available.
func (c *Client) DescribeCompactionTarget(ctx context.Context, targetID int64, callOptions ...grpc.CallOption) (*milvuspb.CompactionTargetInfo, error) {
	if targetID <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg("target ID must be positive")
	}
	var result *milvuspb.CompactionTargetInfo
	err := c.callService(func(service milvuspb.MilvusServiceClient) error {
		resp, err := service.DescribeCompactionTarget(ctx, &milvuspb.DescribeCompactionTargetRequest{TargetId: targetID}, callOptions...)
		if err = checkCompactionTargetStatus(resp.GetStatus(), err); err != nil {
			return err
		}
		if resp.GetTarget().GetTargetId() != targetID {
			return merr.WrapErrServiceInternal("invalid compaction target in Describe response")
		}
		result = resp.GetTarget()
		return nil
	})
	return result, err
}

// ListCompactionTargets returns the authorized targets matching the options.
func (c *Client) ListCompactionTargets(ctx context.Context, opt ListCompactionTargetsOption, callOptions ...grpc.CallOption) ([]*milvuspb.CompactionTargetInfo, error) {
	if opt == nil {
		return nil, merr.WrapErrParameterMissingMsg("compaction target list options")
	}
	req := opt.Request()
	if req == nil {
		return nil, merr.WrapErrParameterMissingMsg("compaction target list request")
	}
	var result []*milvuspb.CompactionTargetInfo
	err := c.callService(func(service milvuspb.MilvusServiceClient) error {
		resp, err := service.ListCompactionTargets(ctx, req, callOptions...)
		if err = checkCompactionTargetStatus(resp.GetStatus(), err); err != nil {
			return err
		}
		result = resp.GetTargets()
		return nil
	})
	return result, err
}

// DropCompactionTarget stops an active user target by ID.
func (c *Client) DropCompactionTarget(ctx context.Context, targetID int64, callOptions ...grpc.CallOption) error {
	if targetID <= 0 {
		return merr.WrapErrParameterInvalidMsg("target ID must be positive")
	}
	return c.callService(func(service milvuspb.MilvusServiceClient) error {
		resp, err := service.DropCompactionTarget(ctx, &milvuspb.DropCompactionTargetRequest{TargetId: targetID}, callOptions...)
		return checkCompactionTargetStatus(resp.GetStatus(), err)
	})
}

func checkCompactionTargetStatus(rpcStatus *commonpb.Status, err error) error {
	if err != nil {
		return err
	}
	if rpcStatus == nil {
		return merr.WrapErrServiceInternal("missing compaction target response status")
	}
	return merr.Error(rpcStatus)
}

// CompactionTargetStoppedError reports why a target stopped before it was satisfied.
type CompactionTargetStoppedError struct {
	TargetID int64
	Reason   string
}

func (e *CompactionTargetStoppedError) Error() string {
	return fmt.Sprintf("compaction target %d stopped with reason %q", e.TargetID, e.Reason)
}

// WaitForCompactionTarget polls Describe until the target stops or ctx ends.
// A zero poll interval uses one second.
func (c *Client) WaitForCompactionTarget(ctx context.Context, targetID int64, pollInterval time.Duration, callOptions ...grpc.CallOption) (*milvuspb.CompactionTargetInfo, error) {
	if pollInterval < 0 {
		return nil, merr.WrapErrParameterInvalidMsg("poll interval must be nonnegative")
	}
	if pollInterval == 0 {
		pollInterval = time.Second
	}
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		target, err := c.DescribeCompactionTarget(ctx, targetID, callOptions...)
		if err != nil {
			return nil, err
		}
		switch target.GetState() {
		case milvuspb.CompactionTargetState_COMPACTION_TARGET_STATE_INACTIVE:
			if target.GetStopReason() == "satisfied" {
				return target, nil
			}
			return target, &CompactionTargetStoppedError{TargetID: targetID, Reason: target.GetStopReason()}
		case milvuspb.CompactionTargetState_COMPACTION_TARGET_STATE_ACTIVE:
		default:
			return nil, merr.WrapErrServiceInternal("unknown compaction target state")
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
	}
}
