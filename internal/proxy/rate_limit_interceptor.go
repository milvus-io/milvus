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

package proxy

import (
	"context"
	"fmt"
	"reflect"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/types"
)

// RateLimitInterceptor returns a new unary server interceptors that performs request rate limiting.
func RateLimitInterceptor(limiter types.Limiter) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		collectionID, rt, n, err := getRequestInfo(req)
		if err != nil {
			return handler(ctx, req)
		}

		code := limiter.Check(collectionID, rt, n)
		if code != commonpb.ErrorCode_Success {
			rsp := getFailedResponse(req, rt, code, info.FullMethod)
			if rsp != nil {
				return rsp, nil
			}
		}
		return handler(ctx, req)
	}
}

// getRequestInfo returns collection name and rateType of request and return tokens needed.
func getRequestInfo(req interface{}) (int64, internalpb.RateType, int, error) {
	switch r := req.(type) {
	case *milvuspb.InsertRequest:
		collectionID, _ := globalMetaCache.GetCollectionID(context.TODO(), r.GetDbName(), r.GetCollectionName())
		return collectionID, internalpb.RateType_DMLInsert, proto.Size(r), nil
	case *milvuspb.UpsertRequest:
		collectionID, _ := globalMetaCache.GetCollectionID(context.TODO(), r.GetDbName(), r.GetCollectionName())
		return collectionID, internalpb.RateType_DMLUpsert, proto.Size(r), nil
	case *milvuspb.DeleteRequest:
		collectionID, _ := globalMetaCache.GetCollectionID(context.TODO(), r.GetDbName(), r.GetCollectionName())
		return collectionID, internalpb.RateType_DMLDelete, proto.Size(r), nil
	case *milvuspb.ImportRequest:
		collectionID, _ := globalMetaCache.GetCollectionID(context.TODO(), r.GetDbName(), r.GetCollectionName())
		return collectionID, internalpb.RateType_DMLBulkLoad, proto.Size(r), nil
	case *milvuspb.SearchRequest:
		collectionID, _ := globalMetaCache.GetCollectionID(context.TODO(), r.GetDbName(), r.GetCollectionName())
		return collectionID, internalpb.RateType_DQLSearch, int(r.GetNq()), nil
	case *milvuspb.QueryRequest:
		collectionID, _ := globalMetaCache.GetCollectionID(context.TODO(), r.GetDbName(), r.GetCollectionName())
		return collectionID, internalpb.RateType_DQLQuery, 1, nil // think of the query request's nq as 1
	case *milvuspb.CreateCollectionRequest:
		collectionID, _ := globalMetaCache.GetCollectionID(context.TODO(), r.GetDbName(), r.GetCollectionName())
		return collectionID, internalpb.RateType_DDLCollection, 1, nil
	case *milvuspb.DropCollectionRequest:
		collectionID, _ := globalMetaCache.GetCollectionID(context.TODO(), r.GetDbName(), r.GetCollectionName())
		return collectionID, internalpb.RateType_DDLCollection, 1, nil
	case *milvuspb.LoadCollectionRequest:
		collectionID, _ := globalMetaCache.GetCollectionID(context.TODO(), r.GetDbName(), r.GetCollectionName())
		return collectionID, internalpb.RateType_DDLCollection, 1, nil
	case *milvuspb.ReleaseCollectionRequest:
		collectionID, _ := globalMetaCache.GetCollectionID(context.TODO(), r.GetDbName(), r.GetCollectionName())
		return collectionID, internalpb.RateType_DDLCollection, 1, nil
	case *milvuspb.CreatePartitionRequest:
		collectionID, _ := globalMetaCache.GetCollectionID(context.TODO(), r.GetDbName(), r.GetCollectionName())
		return collectionID, internalpb.RateType_DDLPartition, 1, nil
	case *milvuspb.DropPartitionRequest:
		collectionID, _ := globalMetaCache.GetCollectionID(context.TODO(), r.GetDbName(), r.GetCollectionName())
		return collectionID, internalpb.RateType_DDLPartition, 1, nil
	case *milvuspb.LoadPartitionsRequest:
		collectionID, _ := globalMetaCache.GetCollectionID(context.TODO(), r.GetDbName(), r.GetCollectionName())
		return collectionID, internalpb.RateType_DDLPartition, 1, nil
	case *milvuspb.ReleasePartitionsRequest:
		collectionID, _ := globalMetaCache.GetCollectionID(context.TODO(), r.GetDbName(), r.GetCollectionName())
		return collectionID, internalpb.RateType_DDLPartition, 1, nil
	case *milvuspb.CreateIndexRequest:
		collectionID, _ := globalMetaCache.GetCollectionID(context.TODO(), r.GetDbName(), r.GetCollectionName())
		return collectionID, internalpb.RateType_DDLIndex, 1, nil
	case *milvuspb.DropIndexRequest:
		collectionID, _ := globalMetaCache.GetCollectionID(context.TODO(), r.GetDbName(), r.GetCollectionName())
		return collectionID, internalpb.RateType_DDLIndex, 1, nil
	case *milvuspb.FlushRequest:
		return 0, internalpb.RateType_DDLFlush, 1, nil
	case *milvuspb.ManualCompactionRequest:
		return 0, internalpb.RateType_DDLCompaction, 1, nil
		// TODO: support more request
	default:
		if req == nil {
			return 0, 0, 0, fmt.Errorf("null request")
		}
		return 0, 0, 0, fmt.Errorf("unsupported request type %s", reflect.TypeOf(req).Name())
	}
}

// failedStatus returns failed status.
func failedStatus(code commonpb.ErrorCode, reason string) *commonpb.Status {
	return &commonpb.Status{
		ErrorCode: code,
		Reason:    reason,
	}
}

// failedMutationResult returns failed mutation result.
func failedMutationResult(code commonpb.ErrorCode, reason string) *milvuspb.MutationResult {
	return &milvuspb.MutationResult{
		Status: failedStatus(code, reason),
	}
}

// failedBoolResponse returns failed boolean response.
func failedBoolResponse(code commonpb.ErrorCode, reason string) *milvuspb.BoolResponse {
	return &milvuspb.BoolResponse{
		Status: failedStatus(code, reason),
	}
}

func wrapQuotaError(rt internalpb.RateType, errCode commonpb.ErrorCode, fullMethod string) error {
	if errCode == commonpb.ErrorCode_RateLimit {
		return fmt.Errorf("request is rejected by grpc RateLimiter middleware, please retry later, req: %s", fullMethod)
	}

	// deny to write/read
	var op string
	switch rt {
	case internalpb.RateType_DMLInsert, internalpb.RateType_DMLUpsert, internalpb.RateType_DMLDelete, internalpb.RateType_DMLBulkLoad:
		op = "write"
	case internalpb.RateType_DQLSearch, internalpb.RateType_DQLQuery:
		op = "read"
	}
	return fmt.Errorf("deny to %s, reason: %s, req: %s", op, GetQuotaErrorString(errCode), fullMethod)
}

// getFailedResponse returns failed response.
func getFailedResponse(req interface{}, rt internalpb.RateType, errCode commonpb.ErrorCode, fullMethod string) interface{} {
	err := wrapQuotaError(rt, errCode, fullMethod)
	switch req.(type) {
	case *milvuspb.InsertRequest, *milvuspb.DeleteRequest:
		return failedMutationResult(errCode, err.Error())
	case *milvuspb.ImportRequest:
		return &milvuspb.ImportResponse{
			Status: failedStatus(errCode, err.Error()),
		}
	case *milvuspb.SearchRequest:
		return &milvuspb.SearchResults{
			Status: failedStatus(errCode, err.Error()),
		}
	case *milvuspb.QueryRequest:
		return &milvuspb.QueryResults{
			Status: failedStatus(errCode, err.Error()),
		}
	case *milvuspb.CreateCollectionRequest, *milvuspb.DropCollectionRequest,
		*milvuspb.LoadCollectionRequest, *milvuspb.ReleaseCollectionRequest,
		*milvuspb.CreatePartitionRequest, *milvuspb.DropPartitionRequest,
		*milvuspb.LoadPartitionsRequest, *milvuspb.ReleasePartitionsRequest,
		*milvuspb.CreateIndexRequest, *milvuspb.DropIndexRequest:
		return failedStatus(errCode, err.Error())
	case *milvuspb.FlushRequest:
		return &milvuspb.FlushResponse{
			Status: failedStatus(errCode, err.Error()),
		}
	case *milvuspb.ManualCompactionRequest:
		return &milvuspb.ManualCompactionResponse{
			Status: failedStatus(errCode, err.Error()),
		}
	}
	return nil
}
