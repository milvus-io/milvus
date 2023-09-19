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

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

// RateLimitInterceptor returns a new unary server interceptors that performs request rate limiting.
func RateLimitInterceptor(limiter types.Limiter) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		collectionID, rt, n, err := getRequestInfo(req)
		if err != nil {
			return handler(ctx, req)
		}

		err = limiter.Check(collectionID, rt, n)
		if err != nil {
			rsp := getFailedResponse(req, rt, err, info.FullMethod)
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

// failedMutationResult returns failed mutation result.
func failedMutationResult(err error) *milvuspb.MutationResult {
	return &milvuspb.MutationResult{
		Status: merr.Status(err),
	}
}

func wrapQuotaError(rt internalpb.RateType, err error, fullMethod string) error {
	if errors.Is(err, merr.ErrServiceRateLimit) {
		return errors.Wrapf(err, "request %s is rejected by grpc RateLimiter middleware, please retry later", fullMethod)
	}

	// deny to write/read
	var op string
	switch rt {
	case internalpb.RateType_DMLInsert, internalpb.RateType_DMLUpsert, internalpb.RateType_DMLDelete, internalpb.RateType_DMLBulkLoad:
		op = "write"
	case internalpb.RateType_DQLSearch, internalpb.RateType_DQLQuery:
		op = "read"
	}

	return merr.WrapErrServiceForceDeny(op, err, fullMethod)
}

// getFailedResponse returns failed response.
func getFailedResponse(req any, rt internalpb.RateType, err error, fullMethod string) any {
	err = wrapQuotaError(rt, err, fullMethod)
	switch req.(type) {
	case *milvuspb.InsertRequest, *milvuspb.DeleteRequest, *milvuspb.UpsertRequest:
		return failedMutationResult(err)
	case *milvuspb.ImportRequest:
		return &milvuspb.ImportResponse{
			Status: merr.Status(err),
		}
	case *milvuspb.SearchRequest:
		return &milvuspb.SearchResults{
			Status: merr.Status(err),
		}
	case *milvuspb.QueryRequest:
		return &milvuspb.QueryResults{
			Status: merr.Status(err),
		}
	case *milvuspb.CreateCollectionRequest, *milvuspb.DropCollectionRequest,
		*milvuspb.LoadCollectionRequest, *milvuspb.ReleaseCollectionRequest,
		*milvuspb.CreatePartitionRequest, *milvuspb.DropPartitionRequest,
		*milvuspb.LoadPartitionsRequest, *milvuspb.ReleasePartitionsRequest,
		*milvuspb.CreateIndexRequest, *milvuspb.DropIndexRequest:
		return merr.Status(err)
	case *milvuspb.FlushRequest:
		return &milvuspb.FlushResponse{
			Status: merr.Status(err),
		}
	case *milvuspb.ManualCompactionRequest:
		return &milvuspb.ManualCompactionResponse{
			Status: merr.Status(err),
		}
	}
	return nil
}
