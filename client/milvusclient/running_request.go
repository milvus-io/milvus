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
	"time"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/client/v3/entity"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

// ListRunningRequestsResult is the cluster-wide answer to ListRunningRequests.
type ListRunningRequestsResult struct {
	Requests []*entity.RunningRequest
	// NodeResults has one entry per proxy. A proxy with a non-nil Err did
	// not answer, and its requests are missing from Requests.
	NodeResults []entity.RequestNodeResult
}

// ListRunningRequests lists the Search, HybridSearch and Query requests the
// cluster is executing. It succeeds as long as one proxy answered; inspect
// NodeResults to tell a complete answer from a partial one.
func (c *Client) ListRunningRequests(ctx context.Context, option ListRunningRequestsOption, callOptions ...grpc.CallOption) (*ListRunningRequestsResult, error) {
	req := option.Request()

	var result *ListRunningRequestsResult
	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.ListRunningRequests(ctx, req, callOptions...)
		if err := merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		result = &ListRunningRequestsResult{
			Requests:    make([]*entity.RunningRequest, 0, len(resp.GetRequests())),
			NodeResults: nodeResultsFromProto(resp.GetNodeResults()),
		}
		for _, info := range resp.GetRequests() {
			result.Requests = append(result.Requests, runningRequestFromProto(info))
		}
		return nil
	})
	return result, err
}

// CancelRequestsResult is the cluster-wide answer to CancelRequests. Every
// requested id is in exactly one of Canceled, NotFound and Undetermined.
type CancelRequestsResult struct {
	// Canceled describes each canceled request as it was when it was told
	// to stop. Cancellation is asynchronous: the request stops at its next
	// cancellation check, usually within milliseconds, and until then
	// ListRunningRequests shows it in state "Canceling".
	Canceled []*entity.RunningRequest
	// NotFound lists the ids no proxy holds, which includes ids that were
	// already canceled or finished.
	NotFound []string
	// Undetermined lists the ids that a proxy which did not answer may still
	// be running.
	Undetermined []string
	NodeResults  []entity.RequestNodeResult
}

// CancelRequests cancels running requests by id. It succeeds as long as one
// proxy answered; an id a silent proxy may hold is reported in Undetermined
// rather than NotFound.
func (c *Client) CancelRequests(ctx context.Context, option CancelRequestsOption, callOptions ...grpc.CallOption) (*CancelRequestsResult, error) {
	req := option.Request()
	if len(req.GetRequestIds()) == 0 {
		return nil, merr.WrapErrParameterMissingMsg("request ids are required")
	}

	var result *CancelRequestsResult
	err := c.callService(func(milvusService milvuspb.MilvusServiceClient) error {
		resp, err := milvusService.CancelRequests(ctx, req, callOptions...)
		if err := merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		result = &CancelRequestsResult{
			Canceled:     make([]*entity.RunningRequest, 0, len(resp.GetCanceled())),
			NotFound:     append([]string(nil), resp.GetNotFound()...),
			Undetermined: append([]string(nil), resp.GetUndetermined()...),
			NodeResults:  nodeResultsFromProto(resp.GetNodeResults()),
		}
		for _, info := range resp.GetCanceled() {
			result.Canceled = append(result.Canceled, runningRequestFromProto(info))
		}
		return nil
	})
	return result, err
}

func runningRequestFromProto(info *milvuspb.RunningRequestInfo) *entity.RunningRequest {
	return &entity.RunningRequest{
		ID:             info.GetRequestId(),
		ProxyID:        info.GetProxyId(),
		Type:           info.GetType(),
		DBName:         info.GetDbName(),
		CollectionName: info.GetCollectionName(),
		User:           info.GetUser(),
		ClientAddr:     info.GetClientAddr(),
		NQ:             info.GetNq(),
		TopK:           info.GetTopk(),
		Expr:           info.GetExpr(),
		StartTime:      time.UnixMilli(info.GetStartTimeMs()),
		Queued:         time.Duration(info.GetQueuedMs()) * time.Millisecond,
		Elapsed:        time.Duration(info.GetElapsedMs()) * time.Millisecond,
		State:          info.GetState(),
		TaskIDs:        append([]int64(nil), info.GetTaskIds()...),
		TraceID:        info.GetTraceId(),
		Cancellable:    info.GetCancellable(),
	}
}

func nodeResultsFromProto(results []*milvuspb.RunningRequestNodeResult) []entity.RequestNodeResult {
	out := make([]entity.RequestNodeResult, 0, len(results))
	for _, r := range results {
		out = append(out, entity.RequestNodeResult{
			NodeID:        r.GetNodeId(),
			Err:           merr.Error(r.GetStatus()),
			Unimplemented: r.GetUnimplemented(),
		})
	}
	return out
}
