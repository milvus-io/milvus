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
	"strconv"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// RateLimitInterceptor returns a new unary server interceptors that performs request rate limiting.
func RateLimitInterceptor(limiter types.Limiter) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		dbID, collectionIDToPartIDs, rt, n, err := getRequestInfo(req)
		if err != nil {
			return handler(ctx, req)
		}

		err = limiter.Check(dbID, collectionIDToPartIDs, rt, n)
		nodeID := strconv.FormatInt(paramtable.GetNodeID(), 10)
		metrics.ProxyRateLimitReqCount.WithLabelValues(nodeID, rt.String(), metrics.TotalLabel).Inc()
		if err != nil {
			metrics.ProxyRateLimitReqCount.WithLabelValues(nodeID, rt.String(), metrics.FailLabel).Inc()
			rsp := getFailedResponse(req, err)
			if rsp != nil {
				return rsp, nil
			}
		}
		metrics.ProxyRateLimitReqCount.WithLabelValues(nodeID, rt.String(), metrics.SuccessLabel).Inc()
		return handler(ctx, req)
	}
}

type reqPartName interface {
	GetDbName() string
	GetCollectionName() string
	GetPartitionName() string
}

type reqCollName interface {
	GetDbName() string
	GetCollectionName() string
}

func getCollectionAndPartitionIds(r reqPartName) (int64, map[int64][]int64) {
	db, _ := globalMetaCache.GetDatabaseInfo(context.TODO(), r.GetDbName())
	collectionID, _ := globalMetaCache.GetCollectionID(context.TODO(), r.GetDbName(), r.GetCollectionName())
	part, _ := globalMetaCache.GetPartitionInfo(context.TODO(), r.GetDbName(), r.GetCollectionName(), r.GetPartitionName())
	return db.dbID, map[int64][]int64{collectionID: {part.partitionID}}
}

func getCollectionID(r reqCollName) (int64, map[int64][]int64) {
	db, _ := globalMetaCache.GetDatabaseInfo(context.TODO(), r.GetDbName())
	collectionID, _ := globalMetaCache.GetCollectionID(context.TODO(), r.GetDbName(), r.GetCollectionName())
	return db.dbID, map[int64][]int64{collectionID: {}}
}

// getRequestInfo returns collection name and rateType of request and return tokens needed.
func getRequestInfo(req interface{}) (int64, map[int64][]int64, internalpb.RateType, int, error) {
	switch r := req.(type) {
	case *milvuspb.InsertRequest:
		dbID, collToPartIDs := getCollectionAndPartitionIds(req.(reqPartName))
		return dbID, collToPartIDs, internalpb.RateType_DMLInsert, proto.Size(r), nil
	case *milvuspb.UpsertRequest:
		dbID, collToPartIDs := getCollectionAndPartitionIds(req.(reqPartName))
		return dbID, collToPartIDs, internalpb.RateType_DMLUpsert, proto.Size(r), nil
	case *milvuspb.DeleteRequest:
		dbID, collToPartIDs := getCollectionAndPartitionIds(req.(reqPartName))
		return dbID, collToPartIDs, internalpb.RateType_DMLDelete, proto.Size(r), nil
	case *milvuspb.ImportRequest:
		dbID, collToPartIDs := getCollectionAndPartitionIds(req.(reqPartName))
		return dbID, collToPartIDs, internalpb.RateType_DMLBulkLoad, proto.Size(r), nil
	case *milvuspb.SearchRequest:
		dbID, collToPartIDs := getCollectionID(req.(reqCollName))
		return dbID, collToPartIDs, internalpb.RateType_DQLSearch, int(r.GetNq()), nil
	case *milvuspb.QueryRequest:
		dbID, collToPartIDs := getCollectionID(req.(reqCollName))
		return dbID, collToPartIDs, internalpb.RateType_DQLQuery, 1, nil // think of the query request's nq as 1
	case *milvuspb.CreateCollectionRequest:
		dbID, collToPartIDs := getCollectionID(req.(reqCollName))
		return dbID, collToPartIDs, internalpb.RateType_DDLCollection, 1, nil
	case *milvuspb.DropCollectionRequest:
		dbID, collToPartIDs := getCollectionID(req.(reqCollName))
		return dbID, collToPartIDs, internalpb.RateType_DDLCollection, 1, nil
	case *milvuspb.LoadCollectionRequest:
		dbID, collToPartIDs := getCollectionID(req.(reqCollName))
		return dbID, collToPartIDs, internalpb.RateType_DDLCollection, 1, nil
	case *milvuspb.ReleaseCollectionRequest:
		dbID, collToPartIDs := getCollectionID(req.(reqCollName))
		return dbID, collToPartIDs, internalpb.RateType_DDLCollection, 1, nil
	case *milvuspb.CreatePartitionRequest:
		dbID, collToPartIDs := getCollectionID(req.(reqCollName))
		return dbID, collToPartIDs, internalpb.RateType_DDLPartition, 1, nil
	case *milvuspb.DropPartitionRequest:
		dbID, collToPartIDs := getCollectionID(req.(reqCollName))
		return dbID, collToPartIDs, internalpb.RateType_DDLPartition, 1, nil
	case *milvuspb.LoadPartitionsRequest:
		dbID, collToPartIDs := getCollectionID(req.(reqCollName))
		return dbID, collToPartIDs, internalpb.RateType_DDLPartition, 1, nil
	case *milvuspb.ReleasePartitionsRequest:
		dbID, collToPartIDs := getCollectionID(req.(reqCollName))
		return dbID, collToPartIDs, internalpb.RateType_DDLPartition, 1, nil
	case *milvuspb.CreateIndexRequest:
		dbID, collToPartIDs := getCollectionID(req.(reqCollName))
		return dbID, collToPartIDs, internalpb.RateType_DDLIndex, 1, nil
	case *milvuspb.DropIndexRequest:
		dbID, collToPartIDs := getCollectionID(req.(reqCollName))
		return dbID, collToPartIDs, internalpb.RateType_DDLIndex, 1, nil
	case *milvuspb.FlushRequest:
		db, err := globalMetaCache.GetDatabaseInfo(context.TODO(), r.GetDbName())
		if err != nil {
			return 0, map[int64][]int64{}, 0, 0, err
		}

		collToPartIDs := make(map[int64][]int64, 0)
		for _, collectionName := range r.GetCollectionNames() {
			collectionID, _ := globalMetaCache.GetCollectionID(context.TODO(), r.GetDbName(), collectionName)
			collToPartIDs[collectionID] = []int64{}
		}
		return db.dbID, collToPartIDs, internalpb.RateType_DDLFlush, 1, nil
	case *milvuspb.ManualCompactionRequest:
		return 0, map[int64][]int64{}, internalpb.RateType_DDLCompaction, 1, nil
		// TODO: support more request
	default:
		if req == nil {
			return 0, map[int64][]int64{}, 0, 0, fmt.Errorf("null request")
		}
		return 0, map[int64][]int64{}, 0, 0, fmt.Errorf("unsupported request type %s", reflect.TypeOf(req).Name())
	}
}

// failedMutationResult returns failed mutation result.
func failedMutationResult(err error) *milvuspb.MutationResult {
	return &milvuspb.MutationResult{
		Status: merr.Status(err),
	}
}

// getFailedResponse returns failed response.
func getFailedResponse(req any, err error) any {
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
