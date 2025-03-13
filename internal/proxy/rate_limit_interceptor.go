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
	"strconv"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/requestutil"
)

// RateLimitInterceptor returns a new unary server interceptors that performs request rate limiting.
func RateLimitInterceptor(limiter types.Limiter) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		request, ok := req.(proto.Message)
		if !ok {
			return nil, merr.WrapErrParameterInvalidMsg("wrong req format when check limiter")
		}
		dbID, collectionIDToPartIDs, rt, n, err := GetRequestInfo(ctx, request)
		if err != nil {
			log.Warn("failed to get request info", zap.Error(err))
			return handler(ctx, req)
		}
		if rt == internalpb.RateType_DMLBulkLoad {
			if importReq, ok := req.(*milvuspb.ImportRequest); ok {
				if importutilv2.SkipDiskQuotaCheck(importReq.GetOptions()) {
					return handler(ctx, req)
				}
			}
		}
		err = limiter.Check(dbID, collectionIDToPartIDs, rt, n)
		nodeID := strconv.FormatInt(paramtable.GetNodeID(), 10)
		metrics.ProxyRateLimitReqCount.WithLabelValues(nodeID, rt.String(), metrics.TotalLabel).Inc()
		if err != nil {
			metrics.ProxyRateLimitReqCount.WithLabelValues(nodeID, rt.String(), metrics.FailLabel).Inc()
			rsp := GetFailedResponse(req, err)
			if rsp != nil {
				return rsp, nil
			}
			log.Warn("failed to get failed response, please check it!", zap.Error(err))
			return nil, err
		}
		metrics.ProxyRateLimitReqCount.WithLabelValues(nodeID, rt.String(), metrics.SuccessLabel).Inc()
		return handler(ctx, req)
	}
}

type reqPartName interface {
	requestutil.DBNameGetter
	requestutil.CollectionNameGetter
	requestutil.PartitionNameGetter
}

type reqPartNames interface {
	requestutil.DBNameGetter
	requestutil.CollectionNameGetter
	requestutil.PartitionNamesGetter
}

type reqCollName interface {
	requestutil.DBNameGetter
	requestutil.CollectionNameGetter
}

func getCollectionAndPartitionID(ctx context.Context, r reqPartName) (int64, map[int64][]int64, error) {
	db, err := globalMetaCache.GetDatabaseInfo(ctx, r.GetDbName())
	if err != nil {
		return 0, nil, err
	}
	collectionID, err := globalMetaCache.GetCollectionID(ctx, r.GetDbName(), r.GetCollectionName())
	if err != nil {
		return 0, nil, err
	}
	if r.GetPartitionName() == "" {
		collectionSchema, err := globalMetaCache.GetCollectionSchema(ctx, r.GetDbName(), r.GetCollectionName())
		if err != nil {
			return 0, nil, err
		}
		if collectionSchema.IsPartitionKeyCollection() {
			return db.dbID, map[int64][]int64{collectionID: {}}, nil
		}
	}
	part, err := globalMetaCache.GetPartitionInfo(ctx, r.GetDbName(), r.GetCollectionName(), r.GetPartitionName())
	if err != nil {
		return 0, nil, err
	}
	return db.dbID, map[int64][]int64{collectionID: {part.partitionID}}, nil
}

func getCollectionAndPartitionIDs(ctx context.Context, r reqPartNames) (int64, map[int64][]int64, error) {
	db, err := globalMetaCache.GetDatabaseInfo(ctx, r.GetDbName())
	if err != nil {
		return 0, nil, err
	}
	collectionID, err := globalMetaCache.GetCollectionID(ctx, r.GetDbName(), r.GetCollectionName())
	if err != nil {
		return 0, nil, err
	}
	parts := make([]int64, len(r.GetPartitionNames()))
	for i, s := range r.GetPartitionNames() {
		part, err := globalMetaCache.GetPartitionInfo(ctx, r.GetDbName(), r.GetCollectionName(), s)
		if err != nil {
			return 0, nil, err
		}
		parts[i] = part.partitionID
	}

	return db.dbID, map[int64][]int64{collectionID: parts}, nil
}

func getCollectionID(r reqCollName) (int64, map[int64][]int64) {
	db, _ := globalMetaCache.GetDatabaseInfo(context.TODO(), r.GetDbName())
	if db == nil {
		return util.InvalidDBID, map[int64][]int64{}
	}
	collectionID, _ := globalMetaCache.GetCollectionID(context.TODO(), r.GetDbName(), r.GetCollectionName())
	return db.dbID, map[int64][]int64{collectionID: {}}
}

// failedMutationResult returns failed mutation result.
func failedMutationResult(err error) *milvuspb.MutationResult {
	return &milvuspb.MutationResult{
		Status: merr.Status(err),
	}
}
