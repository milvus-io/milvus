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
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// These legacy requests have no protobuf privilege annotation. Reuse the
// existing collection privileges: segment diagnostics expose statistics, while
// replica diagnostics describe collection loading. Both belong to ReadOnly.
// Other unannotated methods keep their own public/result-filtered contracts.
func collectionMetadataPrivilegeRequest(req interface{}) interface{} {
	switch req := req.(type) {
	case *milvuspb.GetPersistentSegmentInfoRequest:
		return &milvuspb.GetCollectionStatisticsRequest{DbName: req.GetDbName(), CollectionName: req.GetCollectionName()}
	case *milvuspb.GetQuerySegmentInfoRequest:
		return &milvuspb.GetCollectionStatisticsRequest{DbName: req.GetDbName(), CollectionName: req.GetCollectionName()}
	case *milvuspb.GetReplicasRequest:
		return &milvuspb.GetLoadStateRequest{DbName: req.GetDbName(), CollectionName: req.GetCollectionName()}
	default:
		return req
	}
}

func replicaPrivilegeRequestByID(ctx context.Context, cache Cache, req *milvuspb.GetReplicasRequest) (*milvuspb.GetLoadStateRequest, error) {
	if req.GetCollectionID() <= 0 {
		return nil, merr.WrapErrParameterInvalidMsg("collection name or a positive collection ID is required")
	}
	if cache == nil {
		return nil, merr.WrapErrServiceUnavailable("collection metadata cache is not ready")
	}
	info, err := cache.GetCollectionInfo(ctx, GetCurDBNameFromRequestOrContext(ctx, req), "", req.GetCollectionID())
	if err != nil {
		// Identity resolution runs before authorization. An absent collection or
		// database must be indistinguishable from an existing, forbidden ID.
		// Keep operational failures intact so callers can still retry them.
		if errors.Is(err, merr.ErrCollectionNotFound) || errors.Is(err, merr.ErrDatabaseNotFound) {
			return nil, replicaPrivilegeDenied()
		}
		return nil, err
	}
	// IDs are cluster-wide: neither the connection database nor a caller-supplied
	// DbName establishes which database owns this collection. Older coordinators
	// can omit the real database; never authorize such an ID against a guessed DB.
	if info == nil || info.CollID != req.GetCollectionID() || info.DBName == "" || info.Schema == nil || info.Schema.GetName() == "" {
		return nil, merr.WrapErrServiceUnavailable("cannot resolve collection identity for authorization")
	}
	return &milvuspb.GetLoadStateRequest{DbName: info.DBName, CollectionName: info.Schema.GetName()}, nil
}

func replicaPrivilegeDenied() error {
	// Do not include the resolved database, collection name, or lookup error:
	// none of that identity has been authorized for the caller to see.
	return status.Error(codes.PermissionDenied, "GetReplicas: permission deny")
}

// Guard the Proxy methods as well as gRPC dispatch: in-process HTTP handlers
// call these methods directly. Internal coordinator RPCs use a separate service
// and do not enter these user-facing Proxy methods.
func (node *Proxy) authorizeCollectionMetadata(ctx context.Context, req interface{}) (context.Context, error) {
	nextCtx, err := PrivilegeInterceptorWithMetaCache(node.GetMetaCache)(ctx, req)
	if status.Code(err) == codes.PermissionDenied {
		return ctx, merr.WrapErrPrivilegeNotPermitted("%s", status.Convert(err).Message())
	}
	return nextCtx, err
}
