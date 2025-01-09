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

package httpserver

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	validator "github.com/go-playground/validator/v10"
	"github.com/samber/lo"
	"github.com/tidwall/gjson"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/hook"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util/crypto"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/requestutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type HandlersV2 struct {
	proxy     types.ProxyComponent
	checkAuth bool
}

func NewHandlersV2(proxyClient types.ProxyComponent) *HandlersV2 {
	return &HandlersV2{
		proxy:     proxyClient,
		checkAuth: proxy.Params.CommonCfg.AuthorizationEnabled.GetAsBool(),
	}
}

func (h *HandlersV2) RegisterRoutesToV2(router gin.IRouter) {
	router.POST(CollectionCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &DatabaseReq{} }, wrapperTraceLog(h.listCollections))))
	router.POST(CollectionCategory+HasAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.hasCollection))))
	// todo review the return data
	router.POST(CollectionCategory+DescribeAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.getCollectionDetails))))
	router.POST(CollectionCategory+StatsAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.getCollectionStats))))
	router.POST(CollectionCategory+LoadStateAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.getCollectionLoadState))))
	router.POST(CollectionCategory+CreateAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionReq{AutoID: DisableAutoID} }, wrapperTraceLog(h.createCollection))))
	router.POST(CollectionCategory+DropAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.dropCollection))))
	router.POST(CollectionCategory+RenameAction, timeoutMiddleware(wrapperPost(func() any { return &RenameCollectionReq{} }, wrapperTraceLog(h.renameCollection))))
	router.POST(CollectionCategory+LoadAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.loadCollection))))
	router.POST(CollectionCategory+RefreshLoadAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.refreshLoadCollection))))
	router.POST(CollectionCategory+ReleaseAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.releaseCollection))))
	router.POST(CollectionCategory+AlterPropertiesAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionReqWithProperties{} }, wrapperTraceLog(h.alterCollectionProperties))))
	router.POST(CollectionCategory+DropPropertiesAction, timeoutMiddleware(wrapperPost(func() any { return &DropCollectionPropertiesReq{} }, wrapperTraceLog(h.dropCollectionProperties))))

	router.POST(CollectionFieldCategory+AlterPropertiesAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionFieldReqWithParams{} }, wrapperTraceLog(h.alterCollectionFieldProperties))))

	router.POST(DataBaseCategory+CreateAction, timeoutMiddleware(wrapperPost(func() any { return &DatabaseReqWithProperties{} }, wrapperTraceLog(h.createDatabase))))
	router.POST(DataBaseCategory+DropAction, timeoutMiddleware(wrapperPost(func() any { return &DatabaseReqRequiredName{} }, wrapperTraceLog(h.dropDatabase))))
	router.POST(DataBaseCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &EmptyReq{} }, wrapperTraceLog(h.listDatabases))))
	router.POST(DataBaseCategory+DescribeAction, timeoutMiddleware(wrapperPost(func() any { return &DatabaseReqRequiredName{} }, wrapperTraceLog(h.describeDatabase))))
	router.POST(DataBaseCategory+AlterAction, timeoutMiddleware(wrapperPost(func() any { return &DatabaseReqWithProperties{} }, wrapperTraceLog(h.alterDatabase))))
	// Query
	router.POST(EntityCategory+QueryAction, restfulSizeMiddleware(timeoutMiddleware(wrapperPost(func() any {
		return &QueryReqV2{
			Limit:        100,
			OutputFields: []string{DefaultOutputFields},
		}
	}, wrapperTraceLog(h.query))), true))
	// Get
	router.POST(EntityCategory+GetAction, restfulSizeMiddleware(timeoutMiddleware(wrapperPost(func() any {
		return &CollectionIDReq{
			OutputFields: []string{DefaultOutputFields},
		}
	}, wrapperTraceLog(h.get))), true))
	// Delete
	router.POST(EntityCategory+DeleteAction, restfulSizeMiddleware(timeoutMiddleware(wrapperPost(func() any {
		return &CollectionFilterReq{}
	}, wrapperTraceLog(h.delete))), false))
	// Insert
	router.POST(EntityCategory+InsertAction, restfulSizeMiddleware(timeoutMiddleware(wrapperPost(func() any {
		return &CollectionDataReq{}
	}, wrapperTraceLog(h.insert))), false))
	// Upsert
	router.POST(EntityCategory+UpsertAction, restfulSizeMiddleware(timeoutMiddleware(wrapperPost(func() any {
		return &CollectionDataReq{}
	}, wrapperTraceLog(h.upsert))), false))
	// Search
	router.POST(EntityCategory+SearchAction, restfulSizeMiddleware(timeoutMiddleware(wrapperPost(func() any {
		return &SearchReqV2{
			Limit: 100,
		}
	}, wrapperTraceLog(h.search))), true))
	// advanced_search, backward compatible uri
	router.POST(EntityCategory+AdvancedSearchAction, restfulSizeMiddleware(timeoutMiddleware(wrapperPost(func() any {
		return &HybridSearchReq{
			Limit: 100,
		}
	}, wrapperTraceLog(h.advancedSearch))), true))
	// HybridSearch
	router.POST(EntityCategory+HybridSearchAction, restfulSizeMiddleware(timeoutMiddleware(wrapperPost(func() any {
		return &HybridSearchReq{
			Limit: 100,
		}
	}, wrapperTraceLog(h.advancedSearch))), true))

	router.POST(PartitionCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.listPartitions))))
	router.POST(PartitionCategory+HasAction, timeoutMiddleware(wrapperPost(func() any { return &PartitionReq{} }, wrapperTraceLog(h.hasPartitions))))
	router.POST(PartitionCategory+StatsAction, timeoutMiddleware(wrapperPost(func() any { return &PartitionReq{} }, wrapperTraceLog(h.statsPartition))))

	router.POST(PartitionCategory+CreateAction, timeoutMiddleware(wrapperPost(func() any { return &PartitionReq{} }, wrapperTraceLog(h.createPartition))))
	router.POST(PartitionCategory+DropAction, timeoutMiddleware(wrapperPost(func() any { return &PartitionReq{} }, wrapperTraceLog(h.dropPartition))))
	router.POST(PartitionCategory+LoadAction, timeoutMiddleware(wrapperPost(func() any { return &PartitionsReq{} }, wrapperTraceLog(h.loadPartitions))))
	router.POST(PartitionCategory+ReleaseAction, timeoutMiddleware(wrapperPost(func() any { return &PartitionsReq{} }, wrapperTraceLog(h.releasePartitions))))

	router.POST(UserCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &DatabaseReq{} }, wrapperTraceLog(h.listUsers))))
	router.POST(UserCategory+DescribeAction, timeoutMiddleware(wrapperPost(func() any { return &UserReq{} }, wrapperTraceLog(h.describeUser))))

	router.POST(UserCategory+CreateAction, timeoutMiddleware(wrapperPost(func() any { return &PasswordReq{} }, wrapperTraceLog(h.createUser))))
	router.POST(UserCategory+UpdatePasswordAction, timeoutMiddleware(wrapperPost(func() any { return &NewPasswordReq{} }, wrapperTraceLog(h.updateUser))))
	router.POST(UserCategory+DropAction, timeoutMiddleware(wrapperPost(func() any { return &UserReq{} }, wrapperTraceLog(h.dropUser))))
	router.POST(UserCategory+GrantRoleAction, timeoutMiddleware(wrapperPost(func() any { return &UserRoleReq{} }, wrapperTraceLog(h.addRoleToUser))))
	router.POST(UserCategory+RevokeRoleAction, timeoutMiddleware(wrapperPost(func() any { return &UserRoleReq{} }, wrapperTraceLog(h.removeRoleFromUser))))

	router.POST(RoleCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &DatabaseReq{} }, wrapperTraceLog(h.listRoles))))
	router.POST(RoleCategory+DescribeAction, timeoutMiddleware(wrapperPost(func() any { return &RoleReq{} }, wrapperTraceLog(h.describeRole))))

	router.POST(RoleCategory+CreateAction, timeoutMiddleware(wrapperPost(func() any { return &RoleReq{} }, wrapperTraceLog(h.createRole))))
	router.POST(RoleCategory+DropAction, timeoutMiddleware(wrapperPost(func() any { return &RoleReq{} }, wrapperTraceLog(h.dropRole))))
	router.POST(RoleCategory+GrantPrivilegeAction, timeoutMiddleware(wrapperPost(func() any { return &GrantReq{} }, wrapperTraceLog(h.addPrivilegeToRole))))
	router.POST(RoleCategory+RevokePrivilegeAction, timeoutMiddleware(wrapperPost(func() any { return &GrantReq{} }, wrapperTraceLog(h.removePrivilegeFromRole))))
	router.POST(RoleCategory+GrantPrivilegeActionV2, timeoutMiddleware(wrapperPost(func() any { return &GrantV2Req{} }, wrapperTraceLog(h.grantV2))))
	router.POST(RoleCategory+RevokePrivilegeActionV2, timeoutMiddleware(wrapperPost(func() any { return &GrantV2Req{} }, wrapperTraceLog(h.revokeV2))))

	// privilege group
	router.POST(PrivilegeGroupCategory+CreateAction, timeoutMiddleware(wrapperPost(func() any { return &PrivilegeGroupReq{} }, wrapperTraceLog(h.createPrivilegeGroup))))
	router.POST(PrivilegeGroupCategory+DropAction, timeoutMiddleware(wrapperPost(func() any { return &PrivilegeGroupReq{} }, wrapperTraceLog(h.dropPrivilegeGroup))))
	router.POST(PrivilegeGroupCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &DatabaseReq{} }, wrapperTraceLog(h.listPrivilegeGroups))))
	router.POST(PrivilegeGroupCategory+AddPrivilegesToGroupAction, timeoutMiddleware(wrapperPost(func() any { return &PrivilegeGroupReq{} }, wrapperTraceLog(h.addPrivilegesToGroup))))
	router.POST(PrivilegeGroupCategory+RemovePrivilegesFromGroupAction, timeoutMiddleware(wrapperPost(func() any { return &PrivilegeGroupReq{} }, wrapperTraceLog(h.removePrivilegesFromGroup))))

	router.POST(IndexCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.listIndexes))))
	router.POST(IndexCategory+DescribeAction, timeoutMiddleware(wrapperPost(func() any { return &IndexReq{} }, wrapperTraceLog(h.describeIndex))))

	router.POST(IndexCategory+CreateAction, timeoutMiddleware(wrapperPost(func() any { return &IndexParamReq{} }, wrapperTraceLog(h.createIndex))))
	// todo cannot drop index before release it ?
	router.POST(IndexCategory+DropAction, timeoutMiddleware(wrapperPost(func() any { return &IndexReq{} }, wrapperTraceLog(h.dropIndex))))
	router.POST(IndexCategory+AlterPropertiesAction, timeoutMiddleware(wrapperPost(func() any { return &IndexReqWithProperties{} }, wrapperTraceLog(h.alterIndexProperties))))
	router.POST(IndexCategory+DropPropertiesAction, timeoutMiddleware(wrapperPost(func() any { return &DropIndexPropertiesReq{} }, wrapperTraceLog(h.dropIndexProperties))))

	router.POST(AliasCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &OptionalCollectionNameReq{} }, wrapperTraceLog(h.listAlias))))
	router.POST(AliasCategory+DescribeAction, timeoutMiddleware(wrapperPost(func() any { return &AliasReq{} }, wrapperTraceLog(h.describeAlias))))

	router.POST(AliasCategory+CreateAction, timeoutMiddleware(wrapperPost(func() any { return &AliasCollectionReq{} }, wrapperTraceLog(h.createAlias))))
	router.POST(AliasCategory+DropAction, timeoutMiddleware(wrapperPost(func() any { return &AliasReq{} }, wrapperTraceLog(h.dropAlias))))
	router.POST(AliasCategory+AlterAction, timeoutMiddleware(wrapperPost(func() any { return &AliasCollectionReq{} }, wrapperTraceLog(h.alterAlias))))

	router.POST(ImportJobCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &OptionalCollectionNameReq{} }, wrapperTraceLog(h.listImportJob))))
	router.POST(ImportJobCategory+CreateAction, timeoutMiddleware(wrapperPost(func() any { return &ImportReq{} }, wrapperTraceLog(h.createImportJob))))
	router.POST(ImportJobCategory+GetProgressAction, timeoutMiddleware(wrapperPost(func() any { return &JobIDReq{} }, wrapperTraceLog(h.getImportJobProcess))))
	router.POST(ImportJobCategory+DescribeAction, timeoutMiddleware(wrapperPost(func() any { return &JobIDReq{} }, wrapperTraceLog(h.getImportJobProcess))))

	// resource group
	router.POST(ResourceGroupCategory+CreateAction, timeoutMiddleware(wrapperPost(func() any { return &ResourceGroupReq{} }, wrapperTraceLog(h.createResourceGroup))))
	router.POST(ResourceGroupCategory+DropAction, timeoutMiddleware(wrapperPost(func() any { return &ResourceGroupReq{} }, wrapperTraceLog(h.dropResourceGroup))))
	router.POST(ResourceGroupCategory+AlterAction, timeoutMiddleware(wrapperPost(func() any { return &UpdateResourceGroupReq{} }, wrapperTraceLog(h.updateResourceGroup))))
	router.POST(ResourceGroupCategory+DescribeAction, timeoutMiddleware(wrapperPost(func() any { return &ResourceGroupReq{} }, wrapperTraceLog(h.describeResourceGroup))))
	router.POST(ResourceGroupCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &EmptyReq{} }, wrapperTraceLog(h.listResourceGroups))))
	router.POST(ResourceGroupCategory+TransferReplicaAction, timeoutMiddleware(wrapperPost(func() any { return &TransferReplicaReq{} }, wrapperTraceLog(h.transferReplica))))
}

type (
	newReqFunc    func() any
	handlerFuncV2 func(ctx context.Context, c *gin.Context, req any, dbName string) (interface{}, error)
)

func wrapperPost(newReq newReqFunc, v2 handlerFuncV2) gin.HandlerFunc {
	return func(c *gin.Context) {
		req := newReq()
		if err := c.ShouldBindBodyWith(req, binding.JSON); err != nil {
			log.Warn("high level restful api, read parameters from request body fail", zap.Error(err),
				zap.Any("url", c.Request.URL.Path))
			if _, ok := err.(validator.ValidationErrors); ok {
				HTTPAbortReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrMissingRequiredParameters),
					HTTPReturnMessage: merr.ErrMissingRequiredParameters.Error() + ", error: " + err.Error(),
				})
			} else if err == io.EOF {
				HTTPAbortReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
					HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", the request body should be nil, however {} is valid",
				})
			} else {
				HTTPAbortReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
					HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", error: " + err.Error(),
				})
			}
			return
		}
		dbName := ""
		if req != nil {
			if getter, ok := req.(requestutil.DBNameGetter); ok {
				dbName = getter.GetDbName()
			}
			if dbName == "" {
				dbName = c.Request.Header.Get(HTTPHeaderDBName)
				if dbName == "" {
					dbName = DefaultDbName
				}
			}
		}
		username, _ := c.Get(ContextUsername)
		ctx, span := otel.Tracer(typeutil.ProxyRole).Start(getCtx(c), c.Request.URL.Path)
		defer span.End()
		ctx = proxy.NewContextWithMetadata(ctx, username.(string), dbName)
		traceID := span.SpanContext().TraceID().String()
		ctx = log.WithTraceID(ctx, traceID)
		c.Keys["traceID"] = traceID
		log.Ctx(ctx).Debug("high level restful api, read parameters from request body, then start to handle.",
			zap.Any("url", c.Request.URL.Path))
		v2(ctx, c, req, dbName)
	}
}

const (
	v2CtxKey = `milvus_restful_v2_ctxkey`
)

func getCtx(ctx *gin.Context) context.Context {
	v, ok := ctx.Get(v2CtxKey)
	if !ok {
		return ctx
	}
	return v.(context.Context)
}

// restfulSizeMiddleware is the middleware fetchs metrics stats from gin struct.
func restfulSizeMiddleware(handler gin.HandlerFunc, observeOutbound bool) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		h := metrics.WrapRestfulContext(ctx, ctx.Request.ContentLength)
		ctx.Set(v2CtxKey, h)
		handler(ctx)
		metrics.RecordRestfulMetrics(h, int64(ctx.Writer.Size()), observeOutbound)
	}
}

func wrapperTraceLog(v2 handlerFuncV2) handlerFuncV2 {
	return func(ctx context.Context, c *gin.Context, req any, dbName string) (interface{}, error) {
		switch proxy.Params.CommonCfg.TraceLogMode.GetAsInt() {
		case 1: // simple info
			fields := proxy.GetRequestBaseInfo(ctx, req, &grpc.UnaryServerInfo{
				FullMethod: c.Request.URL.Path,
			}, false)
			log.Ctx(ctx).Info("trace info: simple", fields...)
		case 2: // detail info
			fields := proxy.GetRequestBaseInfo(ctx, req, &grpc.UnaryServerInfo{
				FullMethod: c.Request.URL.Path,
			}, true)
			fields = append(fields, proxy.GetRequestFieldWithoutSensitiveInfo(req))
			log.Ctx(ctx).Info("trace info: detail", fields...)
		case 3: // detail info with request and response
			fields := proxy.GetRequestBaseInfo(ctx, req, &grpc.UnaryServerInfo{
				FullMethod: c.Request.URL.Path,
			}, true)
			fields = append(fields, proxy.GetRequestFieldWithoutSensitiveInfo(req))
			log.Ctx(ctx).Info("trace info: all request", fields...)
		}
		resp, err := v2(ctx, c, req, dbName)
		if proxy.Params.CommonCfg.TraceLogMode.GetAsInt() > 2 {
			if err != nil {
				log.Ctx(ctx).Info("trace info: all, error", zap.Error(err))
			} else {
				log.Ctx(ctx).Info("trace info: all, unknown")
			}
		}
		return resp, err
	}
}

func checkAuthorizationV2(ctx context.Context, c *gin.Context, ignoreErr bool, req interface{}) error {
	username, ok := c.Get(ContextUsername)
	if !ok || username.(string) == "" {
		if !ignoreErr {
			HTTPReturn(c, http.StatusUnauthorized, gin.H{HTTPReturnCode: merr.Code(merr.ErrNeedAuthenticate), HTTPReturnMessage: merr.ErrNeedAuthenticate.Error()})
		}
		hookutil.GetExtension().ReportRefused(ctx, req, WrapErrorToResponse(merr.ErrNeedAuthenticate), nil, c.FullPath())
		return merr.ErrNeedAuthenticate
	}
	_, authErr := proxy.PrivilegeInterceptor(ctx, req)
	if authErr != nil {
		if !ignoreErr {
			HTTPReturn(c, http.StatusForbidden, gin.H{HTTPReturnCode: merr.Code(authErr), HTTPReturnMessage: authErr.Error()})
		}
		hookutil.GetExtension().ReportRefused(ctx, req, WrapErrorToResponse(authErr), nil, c.FullPath())
		return authErr
	}

	return nil
}

func wrapperProxy(ctx context.Context, c *gin.Context, req any, checkAuth bool, ignoreErr bool, fullMethod string, handler func(reqCtx context.Context, req any) (any, error)) (interface{}, error) {
	return wrapperProxyWithLimit(ctx, c, req, checkAuth, ignoreErr, fullMethod, false, nil, handler)
}

func wrapperProxyWithLimit(ctx context.Context, c *gin.Context, req any, checkAuth bool, ignoreErr bool, fullMethod string, checkLimit bool, pxy types.ProxyComponent, handler func(reqCtx context.Context, req any) (any, error)) (interface{}, error) {
	if baseGetter, ok := req.(BaseGetter); ok {
		span := trace.SpanFromContext(ctx)
		span.AddEvent(baseGetter.GetBase().GetMsgType().String())
	}
	if checkAuth {
		err := checkAuthorizationV2(ctx, c, ignoreErr, req)
		if err != nil {
			return nil, err
		}
	}
	if checkLimit {
		_, err := CheckLimiter(ctx, req, pxy)
		if err != nil {
			log.Warn("high level restful api, fail to check limiter", zap.Error(err), zap.String("method", fullMethod))
			hookutil.GetExtension().ReportRefused(ctx, req, WrapErrorToResponse(merr.ErrHTTPRateLimit), nil, c.FullPath())
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrHTTPRateLimit),
				HTTPReturnMessage: merr.ErrHTTPRateLimit.Error() + ", error: " + err.Error(),
			})
			return nil, RestRequestInterceptorErr
		}
	}
	log.Ctx(ctx).Debug("high level restful api, try to do a grpc call")
	username, ok := c.Get(ContextUsername)
	if !ok {
		username = ""
	}

	response, err := proxy.HookInterceptor(context.WithValue(ctx, hook.GinParamsKey, c.Keys), req, username.(string), fullMethod, handler)
	if err == nil {
		status, ok := requestutil.GetStatusFromResponse(response)
		if ok {
			err = merr.Error(status)
		}
	}

	if err != nil {
		log.Ctx(ctx).Warn("high level restful api, grpc call failed", zap.Error(err))
		if !ignoreErr {
			HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		}
	}
	return response, err
}

func (h *HandlersV2) hasCollection(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(requestutil.CollectionNameGetter)
	collectionName := getter.GetCollectionName()
	_, err := proxy.GetCachedCollectionSchema(ctx, dbName, collectionName)
	has := true
	if err != nil {
		req := &milvuspb.HasCollectionRequest{
			DbName:         dbName,
			CollectionName: collectionName,
		}
		resp, err := wrapperProxy(ctx, c, req, false, false, "/milvus.proto.milvus.MilvusService/HasCollection", func(reqCtx context.Context, req any) (interface{}, error) {
			return h.proxy.HasCollection(reqCtx, req.(*milvuspb.HasCollectionRequest))
		})
		if err != nil {
			return nil, err
		}
		has = resp.(*milvuspb.BoolResponse).Value
	}
	HTTPReturn(c, http.StatusOK, wrapperReturnHas(has))
	return has, nil
}

func (h *HandlersV2) listCollections(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	req := &milvuspb.ShowCollectionsRequest{
		DbName: dbName,
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxy(ctx, c, req, false, false, "/milvus.proto.milvus.MilvusService/ShowCollections", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ShowCollections(reqCtx, req.(*milvuspb.ShowCollectionsRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnList(resp.(*milvuspb.ShowCollectionsResponse).CollectionNames))
	}
	return resp, err
}

func (h *HandlersV2) getCollectionDetails(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	collectionName := collectionGetter.GetCollectionName()
	req := &milvuspb.DescribeCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DescribeCollection", func(reqCtx context.Context, req any) (any, error) {
		return h.proxy.DescribeCollection(reqCtx, req.(*milvuspb.DescribeCollectionRequest))
	})
	if err != nil {
		return resp, err
	}
	coll := resp.(*milvuspb.DescribeCollectionResponse)
	primaryField, ok := getPrimaryField(coll.Schema)
	autoID := false
	if !ok {
		log.Ctx(ctx).Warn("high level restful api, get primary field from collection schema fail", zap.Any("collection schema", coll.Schema), zap.Any("request", anyReq))
	} else {
		autoID = primaryField.AutoID
	}
	errMessage := ""
	loadStateReq := &milvuspb.GetLoadStateRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	}
	stateResp, err := wrapperProxy(ctx, c, loadStateReq, h.checkAuth, true, "/milvus.proto.milvus.MilvusService/GetLoadState", func(reqCtx context.Context, req any) (any, error) {
		return h.proxy.GetLoadState(reqCtx, req.(*milvuspb.GetLoadStateRequest))
	})
	collLoadState := ""
	if err == nil {
		collLoadState = stateResp.(*milvuspb.GetLoadStateResponse).State.String()
	} else {
		errMessage += err.Error() + ";"
	}
	vectorField := ""
	for _, field := range coll.Schema.Fields {
		if typeutil.IsVectorType(field.DataType) {
			vectorField = field.Name
			break
		}
	}
	indexDesc := []gin.H{}
	descIndexReq := &milvuspb.DescribeIndexRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldName:      vectorField,
	}
	indexResp, err := wrapperProxy(ctx, c, descIndexReq, h.checkAuth, true, "/milvus.proto.milvus.MilvusService/DescribeIndex", func(reqCtx context.Context, req any) (any, error) {
		return h.proxy.DescribeIndex(reqCtx, req.(*milvuspb.DescribeIndexRequest))
	})
	if err == nil {
		indexDesc = printIndexes(indexResp.(*milvuspb.DescribeIndexResponse).IndexDescriptions)
	} else {
		errMessage += err.Error() + ";"
	}
	var aliases []string
	aliasReq := &milvuspb.ListAliasesRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	}
	aliasResp, err := wrapperProxy(ctx, c, aliasReq, h.checkAuth, true, "/milvus.proto.milvus.MilvusService/ListAliases", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ListAliases(reqCtx, req.(*milvuspb.ListAliasesRequest))
	})
	if err == nil {
		aliases = aliasResp.(*milvuspb.ListAliasesResponse).GetAliases()
	} else {
		errMessage += err.Error() + "."
	}
	if aliases == nil {
		aliases = []string{}
	}
	if coll.Properties == nil {
		coll.Properties = []*commonpb.KeyValuePair{}
	}
	HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: gin.H{
		HTTPCollectionName:    coll.CollectionName,
		HTTPCollectionID:      coll.CollectionID,
		HTTPReturnDescription: coll.Schema.Description,
		HTTPReturnFieldAutoID: autoID,
		"fields":              printFieldsV2(coll.Schema.Fields),
		"functions":           printFunctionDetails(coll.Schema.Functions),
		"aliases":             aliases,
		"indexes":             indexDesc,
		"load":                collLoadState,
		"shardsNum":           coll.ShardsNum,
		"partitionsNum":       coll.NumPartitions,
		"consistencyLevel":    commonpb.ConsistencyLevel_name[int32(coll.ConsistencyLevel)],
		"enableDynamicField":  coll.Schema.EnableDynamicField,
		"properties":          coll.Properties,
	}, HTTPReturnMessage: errMessage})
	return resp, nil
}

func (h *HandlersV2) getCollectionStats(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	req := &milvuspb.GetCollectionStatisticsRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/GetCollectionStatistics", func(reqCtx context.Context, req any) (any, error) {
		return h.proxy.GetCollectionStatistics(reqCtx, req.(*milvuspb.GetCollectionStatisticsRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnRowCount(resp.(*milvuspb.GetCollectionStatisticsResponse).Stats))
	}
	return resp, err
}

func (h *HandlersV2) getCollectionLoadState(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	req := &milvuspb.GetLoadStateRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/GetLoadState", func(reqCtx context.Context, req any) (any, error) {
		return h.proxy.GetLoadState(reqCtx, req.(*milvuspb.GetLoadStateRequest))
	})
	if err != nil {
		return resp, err
	}
	if resp.(*milvuspb.GetLoadStateResponse).State == commonpb.LoadState_LoadStateNotExist {
		err = merr.WrapErrCollectionNotFound(req.CollectionName)
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return resp, err
	} else if resp.(*milvuspb.GetLoadStateResponse).State == commonpb.LoadState_LoadStateNotLoad {
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: gin.H{
			HTTPReturnLoadState: resp.(*milvuspb.GetLoadStateResponse).State.String(),
		}})
		return resp, err
	}
	partitionsGetter, _ := anyReq.(requestutil.PartitionNamesGetter)
	progressReq := &milvuspb.GetLoadingProgressRequest{
		CollectionName: collectionGetter.GetCollectionName(),
		PartitionNames: partitionsGetter.GetPartitionNames(),
		DbName:         dbName,
	}
	progressResp, err := wrapperProxy(ctx, c, progressReq, h.checkAuth, true, "/milvus.proto.milvus.MilvusService/GetLoadingProgress", func(reqCtx context.Context, req any) (any, error) {
		return h.proxy.GetLoadingProgress(reqCtx, req.(*milvuspb.GetLoadingProgressRequest))
	})
	progress := int64(-1)
	errMessage := ""
	if err == nil {
		progress = progressResp.(*milvuspb.GetLoadingProgressResponse).Progress
	} else {
		errMessage += err.Error() + "."
	}
	state := commonpb.LoadState_LoadStateLoading.String()
	if progress >= 100 {
		state = commonpb.LoadState_LoadStateLoaded.String()
	}
	HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: gin.H{
		HTTPReturnLoadState:    state,
		HTTPReturnLoadProgress: progress,
	}, HTTPReturnMessage: errMessage})
	return resp, err
}

func (h *HandlersV2) dropCollection(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(requestutil.CollectionNameGetter)
	req := &milvuspb.DropCollectionRequest{
		DbName:         dbName,
		CollectionName: getter.GetCollectionName(),
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DropCollection", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DropCollection(reqCtx, req.(*milvuspb.DropCollectionRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) renameCollection(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*RenameCollectionReq)
	req := &milvuspb.RenameCollectionRequest{
		DbName:    dbName,
		OldName:   httpReq.CollectionName,
		NewName:   httpReq.NewCollectionName,
		NewDBName: httpReq.NewDbName,
	}
	c.Set(ContextRequest, req)
	if req.NewDBName == "" {
		req.NewDBName = dbName
	}
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/RenameCollection", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.RenameCollection(reqCtx, req.(*milvuspb.RenameCollectionRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) refreshLoadCollection(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(requestutil.CollectionNameGetter)
	req := &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: getter.GetCollectionName(),
		Refresh:        true,
	}
	return h.loadCollectionInternal(ctx, c, req, dbName)
}

func (h *HandlersV2) loadCollection(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(requestutil.CollectionNameGetter)
	req := &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: getter.GetCollectionName(),
	}
	return h.loadCollectionInternal(ctx, c, req, dbName)
}

func (h *HandlersV2) loadCollectionInternal(ctx context.Context, c *gin.Context, req *milvuspb.LoadCollectionRequest, dbName string) (interface{}, error) {
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/LoadCollection", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.LoadCollection(reqCtx, req.(*milvuspb.LoadCollectionRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) releaseCollection(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(requestutil.CollectionNameGetter)
	req := &milvuspb.ReleaseCollectionRequest{
		DbName:         dbName,
		CollectionName: getter.GetCollectionName(),
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/ReleaseCollection", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ReleaseCollection(reqCtx, req.(*milvuspb.ReleaseCollectionRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) alterCollectionProperties(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionReqWithProperties)
	req := &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
	}
	properties := make([]*commonpb.KeyValuePair, 0, len(httpReq.Properties))
	for key, value := range httpReq.Properties {
		properties = append(properties, &commonpb.KeyValuePair{Key: key, Value: fmt.Sprintf("%v", value)})
	}
	req.Properties = properties

	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/AlterCollection", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.AlterCollection(reqCtx, req.(*milvuspb.AlterCollectionRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) dropCollectionProperties(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*DropCollectionPropertiesReq)
	req := &milvuspb.AlterCollectionRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		DeleteKeys:     httpReq.DeleteKeys,
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/AlterCollection", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.AlterCollection(reqCtx, req.(*milvuspb.AlterCollectionRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) alterCollectionFieldProperties(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionFieldReqWithParams)
	req := &milvuspb.AlterCollectionFieldRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		FieldName:      httpReq.FieldName,
	}
	properties := make([]*commonpb.KeyValuePair, 0, len(httpReq.FieldParams))
	for key, value := range httpReq.FieldParams {
		properties = append(properties, &commonpb.KeyValuePair{Key: key, Value: fmt.Sprintf("%v", value)})
	}
	req.Properties = properties

	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/AlterCollectionField", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.AlterCollectionField(reqCtx, req.(*milvuspb.AlterCollectionFieldRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

// copy from internal/proxy/task_query.go
func matchCountRule(outputs []string) bool {
	return len(outputs) == 1 && strings.ToLower(strings.TrimSpace(outputs[0])) == "count(*)"
}

func (h *HandlersV2) query(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*QueryReqV2)
	req := &milvuspb.QueryRequest{
		DbName:                dbName,
		CollectionName:        httpReq.CollectionName,
		Expr:                  httpReq.Filter,
		OutputFields:          httpReq.OutputFields,
		PartitionNames:        httpReq.PartitionNames,
		QueryParams:           []*commonpb.KeyValuePair{},
		UseDefaultConsistency: true,
	}
	req.ExprTemplateValues = generateExpressionTemplate(httpReq.ExprParams)
	c.Set(ContextRequest, req)
	if httpReq.Offset > 0 {
		req.QueryParams = append(req.QueryParams, &commonpb.KeyValuePair{Key: ParamOffset, Value: strconv.FormatInt(int64(httpReq.Offset), 10)})
	}
	if httpReq.Limit > 0 && !matchCountRule(httpReq.OutputFields) {
		req.QueryParams = append(req.QueryParams, &commonpb.KeyValuePair{Key: ParamLimit, Value: strconv.FormatInt(int64(httpReq.Limit), 10)})
	}
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/Query", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.Query(reqCtx, req.(*milvuspb.QueryRequest))
	})
	if err == nil {
		queryResp := resp.(*milvuspb.QueryResults)
		allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
		outputData, err := buildQueryResp(int64(0), queryResp.OutputFields, queryResp.FieldsData, nil, nil, allowJS)
		if err != nil {
			log.Ctx(ctx).Warn("high level restful api, fail to deal with query result", zap.Any("response", resp), zap.Error(err))
			HTTPReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrInvalidSearchResult),
				HTTPReturnMessage: merr.ErrInvalidSearchResult.Error() + ", error: " + err.Error(),
			})
		} else {
			HTTPReturnStream(c, http.StatusOK, gin.H{
				HTTPReturnCode: merr.Code(nil),
				HTTPReturnData: outputData,
				HTTPReturnCost: proxy.GetCostValue(queryResp.GetStatus()),
			})
		}
	}
	return resp, err
}

func (h *HandlersV2) get(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionIDReq)
	collSchema, err := h.GetCollectionSchema(ctx, c, dbName, httpReq.CollectionName)
	if err != nil {
		return nil, err
	}
	body, _ := c.Get(gin.BodyBytesKey)
	filter, err := checkGetPrimaryKey(collSchema, gjson.Get(string(body.([]byte)), DefaultPrimaryFieldName))
	if err != nil {
		HTTPReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrCheckPrimaryKey),
			HTTPReturnMessage: merr.ErrCheckPrimaryKey.Error() + ", error: " + err.Error(),
		})
		return nil, err
	}
	req := &milvuspb.QueryRequest{
		DbName:                dbName,
		CollectionName:        httpReq.CollectionName,
		OutputFields:          httpReq.OutputFields,
		PartitionNames:        httpReq.PartitionNames,
		Expr:                  filter,
		UseDefaultConsistency: true,
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/Query", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.Query(reqCtx, req.(*milvuspb.QueryRequest))
	})
	if err == nil {
		queryResp := resp.(*milvuspb.QueryResults)
		allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
		outputData, err := buildQueryResp(int64(0), queryResp.OutputFields, queryResp.FieldsData, nil, nil, allowJS)
		if err != nil {
			log.Ctx(ctx).Warn("high level restful api, fail to deal with get result", zap.Any("response", resp), zap.Error(err))
			HTTPReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrInvalidSearchResult),
				HTTPReturnMessage: merr.ErrInvalidSearchResult.Error() + ", error: " + err.Error(),
			})
		} else {
			HTTPReturnStream(c, http.StatusOK, gin.H{
				HTTPReturnCode: merr.Code(nil),
				HTTPReturnData: outputData,
				HTTPReturnCost: proxy.GetCostValue(queryResp.GetStatus()),
			})
		}
	}
	return resp, err
}

func (h *HandlersV2) delete(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionFilterReq)
	collSchema, err := h.GetCollectionSchema(ctx, c, dbName, httpReq.CollectionName)
	if err != nil {
		return nil, err
	}
	req := &milvuspb.DeleteRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		PartitionName:  httpReq.PartitionName,
		Expr:           httpReq.Filter,
	}
	req.ExprTemplateValues = generateExpressionTemplate(httpReq.ExprParams)
	c.Set(ContextRequest, req)
	if req.Expr == "" {
		body, _ := c.Get(gin.BodyBytesKey)
		filter, err := checkGetPrimaryKey(collSchema, gjson.Get(string(body.([]byte)), DefaultPrimaryFieldName))
		if err != nil {
			HTTPReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrCheckPrimaryKey),
				HTTPReturnMessage: merr.ErrCheckPrimaryKey.Error() + ", error: " + err.Error(),
			})
			return nil, err
		}
		req.Expr = filter
	}
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/Delete", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.Delete(reqCtx, req.(*milvuspb.DeleteRequest))
	})
	if err == nil {
		deleteResp := resp.(*milvuspb.MutationResult)
		HTTPReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode: merr.Code(nil),
			HTTPReturnData: gin.H{"deleteCount": deleteResp.DeleteCnt},
		})
	}
	return resp, err
}

func (h *HandlersV2) insert(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionDataReq)
	req := &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		PartitionName:  httpReq.PartitionName,
		// PartitionName:  "_default",
	}
	c.Set(ContextRequest, req)

	collSchema, err := h.GetCollectionSchema(ctx, c, dbName, httpReq.CollectionName)
	if err != nil {
		return nil, err
	}
	body, _ := c.Get(gin.BodyBytesKey)
	var validDataMap map[string][]bool
	err, httpReq.Data, validDataMap = checkAndSetData(string(body.([]byte)), collSchema)
	if err != nil {
		log.Ctx(ctx).Warn("high level restful api, fail to deal with insert data", zap.Error(err), zap.String("body", string(body.([]byte))))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrInvalidInsertData),
			HTTPReturnMessage: merr.ErrInvalidInsertData.Error() + ", error: " + err.Error(),
		})
		return nil, err
	}

	req.NumRows = uint32(len(httpReq.Data))
	req.FieldsData, err = anyToColumns(httpReq.Data, validDataMap, collSchema, true)
	if err != nil {
		log.Ctx(ctx).Warn("high level restful api, fail to deal with insert data", zap.Any("data", httpReq.Data), zap.Error(err))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrInvalidInsertData),
			HTTPReturnMessage: merr.ErrInvalidInsertData.Error() + ", error: " + err.Error(),
		})
		return nil, err
	}
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/Insert", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.Insert(reqCtx, req.(*milvuspb.InsertRequest))
	})
	if err == nil {
		insertResp := resp.(*milvuspb.MutationResult)
		cost := proxy.GetCostValue(insertResp.GetStatus())
		switch insertResp.IDs.GetIdField().(type) {
		case *schemapb.IDs_IntId:
			allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
			if allowJS {
				HTTPReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode: merr.Code(nil),
					HTTPReturnData: gin.H{"insertCount": insertResp.InsertCnt, "insertIds": insertResp.IDs.IdField.(*schemapb.IDs_IntId).IntId.Data},
					HTTPReturnCost: cost,
				})
			} else {
				HTTPReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode: merr.Code(nil),
					HTTPReturnData: gin.H{"insertCount": insertResp.InsertCnt, "insertIds": formatInt64(insertResp.IDs.IdField.(*schemapb.IDs_IntId).IntId.Data)},
					HTTPReturnCost: cost,
				})
			}
		case *schemapb.IDs_StrId:
			HTTPReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode: merr.Code(nil),
				HTTPReturnData: gin.H{"insertCount": insertResp.InsertCnt, "insertIds": insertResp.IDs.IdField.(*schemapb.IDs_StrId).StrId.Data},
				HTTPReturnCost: cost,
			})
		default:
			HTTPReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrCheckPrimaryKey),
				HTTPReturnMessage: merr.ErrCheckPrimaryKey.Error() + ", error: unsupported primary key data type",
			})
		}
	}
	return resp, err
}

func (h *HandlersV2) upsert(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionDataReq)
	req := &milvuspb.UpsertRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		PartitionName:  httpReq.PartitionName,
		// PartitionName:  "_default",
	}
	c.Set(ContextRequest, req)

	collSchema, err := h.GetCollectionSchema(ctx, c, dbName, httpReq.CollectionName)
	if err != nil {
		return nil, err
	}
	body, _ := c.Get(gin.BodyBytesKey)
	var validDataMap map[string][]bool
	err, httpReq.Data, validDataMap = checkAndSetData(string(body.([]byte)), collSchema)
	if err != nil {
		log.Ctx(ctx).Warn("high level restful api, fail to deal with upsert data", zap.Any("body", body), zap.Error(err))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrInvalidInsertData),
			HTTPReturnMessage: merr.ErrInvalidInsertData.Error() + ", error: " + err.Error(),
		})
		return nil, err
	}

	req.NumRows = uint32(len(httpReq.Data))
	req.FieldsData, err = anyToColumns(httpReq.Data, validDataMap, collSchema, false)
	if err != nil {
		log.Ctx(ctx).Warn("high level restful api, fail to deal with upsert data", zap.Any("data", httpReq.Data), zap.Error(err))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrInvalidInsertData),
			HTTPReturnMessage: merr.ErrInvalidInsertData.Error() + ", error: " + err.Error(),
		})
		return nil, err
	}
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/Upsert", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.Upsert(reqCtx, req.(*milvuspb.UpsertRequest))
	})
	if err == nil {
		upsertResp := resp.(*milvuspb.MutationResult)
		cost := proxy.GetCostValue(upsertResp.GetStatus())
		switch upsertResp.IDs.GetIdField().(type) {
		case *schemapb.IDs_IntId:
			allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
			if allowJS {
				HTTPReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode: merr.Code(nil),
					HTTPReturnData: gin.H{"upsertCount": upsertResp.UpsertCnt, "upsertIds": upsertResp.IDs.IdField.(*schemapb.IDs_IntId).IntId.Data},
					HTTPReturnCost: cost,
				})
			} else {
				HTTPReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode: merr.Code(nil),
					HTTPReturnData: gin.H{"upsertCount": upsertResp.UpsertCnt, "upsertIds": formatInt64(upsertResp.IDs.IdField.(*schemapb.IDs_IntId).IntId.Data)},
					HTTPReturnCost: cost,
				})
			}
		case *schemapb.IDs_StrId:
			HTTPReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode: merr.Code(nil),
				HTTPReturnData: gin.H{"upsertCount": upsertResp.UpsertCnt, "upsertIds": upsertResp.IDs.IdField.(*schemapb.IDs_StrId).StrId.Data},
				HTTPReturnCost: cost,
			})
		default:
			HTTPReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrCheckPrimaryKey),
				HTTPReturnMessage: merr.ErrCheckPrimaryKey.Error() + ", error: unsupported primary key data type",
			})
		}
	}
	return resp, err
}

func generatePlaceholderGroup(ctx context.Context, body string, collSchema *schemapb.CollectionSchema, fieldName string) ([]byte, error) {
	var err error
	var vectorField *schemapb.FieldSchema
	if len(fieldName) == 0 {
		for _, field := range collSchema.Fields {
			if typeutil.IsVectorType(field.DataType) {
				if len(fieldName) == 0 {
					fieldName = field.Name
					vectorField = field
				} else {
					return nil, errors.New("search without annsField, but already found multiple vector fields: [" + fieldName + ", " + field.Name + ",,,]")
				}
			}
		}
	} else {
		for _, field := range collSchema.Fields {
			if field.Name == fieldName && typeutil.IsVectorType(field.DataType) {
				vectorField = field
				break
			}
		}
	}
	if vectorField == nil {
		return nil, errors.New("cannot find a vector field named: " + fieldName)
	}
	dim := int64(0)
	if !typeutil.IsSparseFloatVectorType(vectorField.DataType) {
		dim, _ = getDim(vectorField)
	}

	dataType := vectorField.DataType

	if vectorField.GetIsFunctionOutput() {
		for _, function := range collSchema.Functions {
			if function.Type == schemapb.FunctionType_BM25 {
				// TODO: currently only BM25 function is supported, thus guarantees one input field to one output field
				if function.OutputFieldNames[0] == vectorField.Name {
					dataType = schemapb.DataType_VarChar
				}
			}
		}
	}

	phv, err := convertQueries2Placeholder(body, dataType, dim)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(&commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			phv,
		},
	})
}

func generateSearchParams(reqSearchParams searchParams) []*commonpb.KeyValuePair {
	var searchParams []*commonpb.KeyValuePair
	if reqSearchParams.Params == nil {
		reqSearchParams.Params = make(map[string]any)
	}
	bs, _ := json.Marshal(reqSearchParams.Params)
	searchParams = append(searchParams, &commonpb.KeyValuePair{Key: Params, Value: string(bs)})
	searchParams = append(searchParams, &commonpb.KeyValuePair{Key: common.IgnoreGrowing, Value: strconv.FormatBool(reqSearchParams.IgnoreGrowing)})
	searchParams = append(searchParams, &commonpb.KeyValuePair{Key: common.HintsKey, Value: reqSearchParams.Hints})
	// need to exposure ParamRoundDecimal in req?
	searchParams = append(searchParams, &commonpb.KeyValuePair{Key: ParamRoundDecimal, Value: "-1"})
	return searchParams
}

func (h *HandlersV2) search(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*SearchReqV2)
	req := &milvuspb.SearchRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		Dsl:            httpReq.Filter,
		DslType:        commonpb.DslType_BoolExprV1,
		OutputFields:   httpReq.OutputFields,
		PartitionNames: httpReq.PartitionNames,
	}
	var err error
	req.ConsistencyLevel, req.UseDefaultConsistency, err = convertConsistencyLevel(httpReq.ConsistencyLevel)
	if err != nil {
		log.Ctx(ctx).Warn("high level restful api, search with consistency_level invalid", zap.Error(err))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(err),
			HTTPReturnMessage: "consistencyLevel can only be [Strong, Session, Bounded, Eventually, Customized], default: Bounded, err:" + err.Error(),
		})
		return nil, err
	}
	c.Set(ContextRequest, req)

	collSchema, err := h.GetCollectionSchema(ctx, c, dbName, httpReq.CollectionName)
	if err != nil {
		// has already throw http in GetCollectionSchema if fails to get schema
		return nil, err
	}

	searchParams := generateSearchParams(httpReq.SearchParams)
	searchParams = append(searchParams, &commonpb.KeyValuePair{Key: common.TopKKey, Value: strconv.FormatInt(int64(httpReq.Limit), 10)})
	searchParams = append(searchParams, &commonpb.KeyValuePair{Key: ParamOffset, Value: strconv.FormatInt(int64(httpReq.Offset), 10)})
	if httpReq.GroupByField != "" {
		searchParams = append(searchParams, &commonpb.KeyValuePair{Key: ParamGroupByField, Value: httpReq.GroupByField})
	}
	if httpReq.GroupByField != "" && httpReq.GroupSize > 0 {
		searchParams = append(searchParams, &commonpb.KeyValuePair{Key: ParamGroupSize, Value: strconv.FormatInt(int64(httpReq.GroupSize), 10)})
		searchParams = append(searchParams, &commonpb.KeyValuePair{Key: ParamStrictGroupSize, Value: strconv.FormatBool(httpReq.StrictGroupSize)})
	}
	searchParams = append(searchParams, &commonpb.KeyValuePair{Key: proxy.AnnsFieldKey, Value: httpReq.AnnsField})
	body, _ := c.Get(gin.BodyBytesKey)
	placeholderGroup, err := generatePlaceholderGroup(ctx, string(body.([]byte)), collSchema, httpReq.AnnsField)
	if err != nil {
		log.Ctx(ctx).Warn("high level restful api, search with vector invalid", zap.Error(err))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
			HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", error: " + err.Error(),
		})
		return nil, err
	}
	req.SearchParams = searchParams
	req.PlaceholderGroup = placeholderGroup
	req.ExprTemplateValues = generateExpressionTemplate(httpReq.ExprParams)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/Search", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.Search(reqCtx, req.(*milvuspb.SearchRequest))
	})
	if err == nil {
		searchResp := resp.(*milvuspb.SearchResults)
		cost := proxy.GetCostValue(searchResp.GetStatus())
		if searchResp.Results.TopK == int64(0) {
			HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: []interface{}{}, HTTPReturnCost: cost})
		} else {
			allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
			outputData, err := buildQueryResp(0, searchResp.Results.OutputFields, searchResp.Results.FieldsData, searchResp.Results.Ids, searchResp.Results.Scores, allowJS)
			if err != nil {
				log.Ctx(ctx).Warn("high level restful api, fail to deal with search result", zap.Any("result", searchResp.Results), zap.Error(err))
				HTTPReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrInvalidSearchResult),
					HTTPReturnMessage: merr.ErrInvalidSearchResult.Error() + ", error: " + err.Error(),
				})
			} else {
				HTTPReturnStream(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: outputData, HTTPReturnCost: cost})
			}
		}
	}
	return resp, err
}

func (h *HandlersV2) advancedSearch(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*HybridSearchReq)
	req := &milvuspb.HybridSearchRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		Requests:       []*milvuspb.SearchRequest{},
		OutputFields:   httpReq.OutputFields,
	}
	var err error
	req.ConsistencyLevel, req.UseDefaultConsistency, err = convertConsistencyLevel(httpReq.ConsistencyLevel)
	if err != nil {
		log.Ctx(ctx).Warn("high level restful api, search with consistency_level invalid", zap.Error(err))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(err),
			HTTPReturnMessage: "consistencyLevel can only be [Strong, Session, Bounded, Eventually, Customized], default: Bounded, err:" + err.Error(),
		})
		return nil, err
	}
	c.Set(ContextRequest, req)

	collSchema, err := h.GetCollectionSchema(ctx, c, dbName, httpReq.CollectionName)
	if err != nil {
		// has already throw http in GetCollectionSchema if fails to get schema
		return nil, err
	}
	body, _ := c.Get(gin.BodyBytesKey)
	searchArray := gjson.Get(string(body.([]byte)), "search").Array()
	for i, subReq := range httpReq.Search {
		searchParams := generateSearchParams(subReq.SearchParams)
		searchParams = append(searchParams, &commonpb.KeyValuePair{Key: common.TopKKey, Value: strconv.FormatInt(int64(subReq.Limit), 10)})
		searchParams = append(searchParams, &commonpb.KeyValuePair{Key: ParamOffset, Value: strconv.FormatInt(int64(subReq.Offset), 10)})
		searchParams = append(searchParams, &commonpb.KeyValuePair{Key: proxy.AnnsFieldKey, Value: subReq.AnnsField})
		placeholderGroup, err := generatePlaceholderGroup(ctx, searchArray[i].Raw, collSchema, subReq.AnnsField)
		if err != nil {
			log.Ctx(ctx).Warn("high level restful api, search with vector invalid", zap.Error(err))
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
				HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", error: " + err.Error(),
			})
			return nil, err
		}
		searchReq := &milvuspb.SearchRequest{
			DbName:           dbName,
			CollectionName:   httpReq.CollectionName,
			Dsl:              subReq.Filter,
			PlaceholderGroup: placeholderGroup,
			DslType:          commonpb.DslType_BoolExprV1,
			OutputFields:     httpReq.OutputFields,
			PartitionNames:   httpReq.PartitionNames,
			SearchParams:     searchParams,
		}
		searchReq.ExprTemplateValues = generateExpressionTemplate(subReq.ExprParams)
		req.Requests = append(req.Requests, searchReq)
	}
	bs, _ := json.Marshal(httpReq.Rerank.Params)
	req.RankParams = []*commonpb.KeyValuePair{
		{Key: proxy.RankTypeKey, Value: httpReq.Rerank.Strategy},
		{Key: proxy.RankParamsKey, Value: string(bs)},
		{Key: ParamLimit, Value: strconv.FormatInt(int64(httpReq.Limit), 10)},
		{Key: ParamRoundDecimal, Value: "-1"},
	}
	if httpReq.GroupByField != "" {
		req.RankParams = append(req.RankParams, &commonpb.KeyValuePair{Key: ParamGroupByField, Value: httpReq.GroupByField})
	}
	if httpReq.GroupByField != "" && httpReq.GroupSize > 0 {
		req.RankParams = append(req.RankParams, &commonpb.KeyValuePair{Key: ParamGroupSize, Value: strconv.FormatInt(int64(httpReq.GroupSize), 10)})
		req.RankParams = append(req.RankParams, &commonpb.KeyValuePair{Key: ParamStrictGroupSize, Value: strconv.FormatBool(httpReq.StrictGroupSize)})
	}
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/HybridSearch", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.HybridSearch(reqCtx, req.(*milvuspb.HybridSearchRequest))
	})
	if err == nil {
		searchResp := resp.(*milvuspb.SearchResults)
		cost := proxy.GetCostValue(searchResp.GetStatus())
		if searchResp.Results.TopK == int64(0) {
			HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: []interface{}{}, HTTPReturnCost: cost})
		} else {
			allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
			outputData, err := buildQueryResp(0, searchResp.Results.OutputFields, searchResp.Results.FieldsData, searchResp.Results.Ids, searchResp.Results.Scores, allowJS)
			if err != nil {
				log.Ctx(ctx).Warn("high level restful api, fail to deal with search result", zap.Any("result", searchResp.Results), zap.Error(err))
				HTTPReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrInvalidSearchResult),
					HTTPReturnMessage: merr.ErrInvalidSearchResult.Error() + ", error: " + err.Error(),
				})
			} else {
				HTTPReturnStream(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: outputData, HTTPReturnCost: cost})
			}
		}
	}
	return resp, err
}

func (h *HandlersV2) createCollection(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionReq)
	req := &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		Properties:     []*commonpb.KeyValuePair{},
	}
	c.Set(ContextRequest, req)

	var schema []byte
	var err error
	fieldNames := map[string]bool{}
	partitionsNum := int64(-1)
	if len(httpReq.Schema.Fields) == 0 {
		if len(httpReq.Schema.Functions) > 0 {
			err := merr.WrapErrParameterInvalid("schema", "functions",
				"functions are not supported for quickly create collection")
			log.Ctx(ctx).Warn("high level restful api, quickly create collection fail", zap.Error(err), zap.Any("request", anyReq))
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(err),
				HTTPReturnMessage: err.Error(),
			})
			return nil, err
		}

		if httpReq.Dimension == 0 {
			err := merr.WrapErrParameterInvalid("collectionName & dimension", "collectionName",
				"dimension is required for quickly create collection(default metric type: "+DefaultMetricType+")")
			log.Ctx(ctx).Warn("high level restful api, quickly create collection fail", zap.Error(err), zap.Any("request", anyReq))
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(err),
				HTTPReturnMessage: err.Error(),
			})
			return nil, err
		}
		idDataType := schemapb.DataType_Int64
		idParams := []*commonpb.KeyValuePair{}
		switch httpReq.IDType {
		case "VarChar", "Varchar":
			idDataType = schemapb.DataType_VarChar
			idParams = append(idParams, &commonpb.KeyValuePair{
				Key:   common.MaxLengthKey,
				Value: fmt.Sprintf("%v", httpReq.Params["max_length"]),
			})
			httpReq.IDType = "VarChar"
		case "", "Int64", "int64":
			httpReq.IDType = "Int64"
		default:
			err := merr.WrapErrParameterInvalid("Int64, Varchar", httpReq.IDType,
				"idType can only be [Int64, VarChar], default: Int64")
			log.Ctx(ctx).Warn("high level restful api, quickly create collection fail", zap.Error(err), zap.Any("request", anyReq))
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(err),
				HTTPReturnMessage: err.Error(),
			})
			return nil, err
		}
		if len(httpReq.PrimaryFieldName) == 0 {
			httpReq.PrimaryFieldName = DefaultPrimaryFieldName
		}
		if len(httpReq.VectorFieldName) == 0 {
			httpReq.VectorFieldName = DefaultVectorFieldName
		}
		enableDynamic := EnableDynamic
		if enStr, ok := httpReq.Params["enableDynamicField"]; ok {
			enableDynamic, err = strconv.ParseBool(fmt.Sprintf("%v", enStr))
			if err != nil {
				log.Ctx(ctx).Warn("high level restful api, parse enableDynamicField fail", zap.Error(err), zap.Any("request", anyReq))
				HTTPAbortReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(err),
					HTTPReturnMessage: "parse enableDynamicField fail, err:" + err.Error(),
				})
				return nil, err
			}
		}
		schema, err = proto.Marshal(&schemapb.CollectionSchema{
			Name: httpReq.CollectionName,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      common.StartOfUserFieldID,
					Name:         httpReq.PrimaryFieldName,
					IsPrimaryKey: true,
					DataType:     idDataType,
					AutoID:       httpReq.AutoID,
					TypeParams:   idParams,
				},
				{
					FieldID:      common.StartOfUserFieldID + 1,
					Name:         httpReq.VectorFieldName,
					IsPrimaryKey: false,
					DataType:     schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   Dim,
							Value: strconv.FormatInt(int64(httpReq.Dimension), 10),
						},
					},
					AutoID: DisableAutoID,
				},
			},
			EnableDynamicField: enableDynamic,
		})
	} else {
		collSchema := schemapb.CollectionSchema{
			Name:               httpReq.CollectionName,
			AutoID:             httpReq.Schema.AutoId,
			Fields:             []*schemapb.FieldSchema{},
			Functions:          []*schemapb.FunctionSchema{},
			EnableDynamicField: httpReq.Schema.EnableDynamicField,
		}

		allOutputFields := []string{}

		for _, function := range httpReq.Schema.Functions {
			functionTypeValue, ok := schemapb.FunctionType_value[function.FunctionType]
			if !ok {
				log.Ctx(ctx).Warn("function's data type is invalid(case sensitive).", zap.Any("function.DataType", function.FunctionType), zap.Any("function", function))
				err := merr.WrapErrParameterInvalid("FunctionType", function.FunctionType, "function data type is invalid(case sensitive)")
				HTTPAbortReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrParameterInvalid),
					HTTPReturnMessage: err.Error(),
				})
				return nil, err
			}
			functionType := schemapb.FunctionType(functionTypeValue)
			description := function.Description
			params := []*commonpb.KeyValuePair{}
			for key, value := range function.Params {
				params = append(params, &commonpb.KeyValuePair{Key: key, Value: fmt.Sprintf("%v", value)})
			}
			collSchema.Functions = append(collSchema.Functions, &schemapb.FunctionSchema{
				Name:             function.FunctionName,
				Description:      description,
				Type:             functionType,
				InputFieldNames:  function.InputFieldNames,
				OutputFieldNames: function.OutputFieldNames,
				Params:           params,
			})
			allOutputFields = append(allOutputFields, function.OutputFieldNames...)
		}

		for _, field := range httpReq.Schema.Fields {
			fieldDataType, ok := schemapb.DataType_value[field.DataType]
			if !ok {
				log.Ctx(ctx).Warn("field's data type is invalid(case sensitive).", zap.Any("fieldDataType", field.DataType), zap.Any("field", field))
				HTTPAbortReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrParameterInvalid),
					HTTPReturnMessage: merr.ErrParameterInvalid.Error() + ", data type " + field.DataType + " is invalid(case sensitive).",
				})
				return nil, merr.ErrParameterInvalid
			}
			dataType := schemapb.DataType(fieldDataType)
			fieldSchema := schemapb.FieldSchema{
				Name:            field.FieldName,
				IsPrimaryKey:    field.IsPrimary,
				IsPartitionKey:  field.IsPartitionKey,
				IsClusteringKey: field.IsClusteringKey,
				DataType:        dataType,
				TypeParams:      []*commonpb.KeyValuePair{},
				Nullable:        field.Nullable,
			}

			fieldSchema.DefaultValue, err = convertDefaultValue(field.DefaultValue, dataType)
			if err != nil {
				log.Ctx(ctx).Warn("convert defaultValue fail", zap.Any("defaultValue", field.DefaultValue))
				HTTPAbortReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(err),
					HTTPReturnMessage: "convert defaultValue fail, err:" + err.Error(),
				})
				return nil, err
			}
			if dataType == schemapb.DataType_Array {
				if _, ok := schemapb.DataType_value[field.ElementDataType]; !ok {
					log.Ctx(ctx).Warn("element's data type is invalid(case sensitive).", zap.Any("elementDataType", field.ElementDataType), zap.Any("field", field))
					HTTPAbortReturn(c, http.StatusOK, gin.H{
						HTTPReturnCode:    merr.Code(merr.ErrParameterInvalid),
						HTTPReturnMessage: merr.ErrParameterInvalid.Error() + ", element data type " + field.ElementDataType + " is invalid(case sensitive).",
					})
					return nil, merr.ErrParameterInvalid
				}
				fieldSchema.ElementType = schemapb.DataType(schemapb.DataType_value[field.ElementDataType])
			}
			if field.IsPrimary {
				fieldSchema.AutoID = httpReq.Schema.AutoId
			}
			if field.IsPartitionKey {
				partitionsNum = int64(64)
				if partitionsNumStr, ok := httpReq.Params["partitionsNum"]; ok {
					if partitions, err := strconv.ParseInt(fmt.Sprintf("%v", partitionsNumStr), 10, 64); err == nil {
						partitionsNum = partitions
					}
				}
			}
			for key, fieldParam := range field.ElementTypeParams {
				value, err := getElementTypeParams(fieldParam)
				if err != nil {
					return nil, err
				}
				fieldSchema.TypeParams = append(fieldSchema.TypeParams, &commonpb.KeyValuePair{Key: key, Value: value})
			}
			if lo.Contains(allOutputFields, field.FieldName) {
				fieldSchema.IsFunctionOutput = true
			}
			collSchema.Fields = append(collSchema.Fields, &fieldSchema)
			fieldNames[field.FieldName] = true
		}
		schema, err = proto.Marshal(&collSchema)
	}
	if err != nil {
		log.Ctx(ctx).Warn("high level restful api, marshal collection schema fail", zap.Error(err), zap.Any("request", anyReq))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrMarshalCollectionSchema),
			HTTPReturnMessage: merr.ErrMarshalCollectionSchema.Error() + ", error: " + err.Error(),
		})
		return nil, err
	}
	req.Schema = schema

	shardsNum := int32(ShardNumDefault)
	if shardsNumStr, ok := httpReq.Params["shardsNum"]; ok {
		if shards, err := strconv.ParseInt(fmt.Sprintf("%v", shardsNumStr), 10, 64); err == nil {
			shardsNum = int32(shards)
		}
	}
	req.ShardsNum = shardsNum

	consistencyLevel := commonpb.ConsistencyLevel_Bounded
	if _, ok := httpReq.Params["consistencyLevel"]; ok {
		if level, ok := commonpb.ConsistencyLevel_value[fmt.Sprintf("%s", httpReq.Params["consistencyLevel"])]; ok {
			consistencyLevel = commonpb.ConsistencyLevel(level)
		} else {
			err := merr.WrapErrParameterInvalid("Strong, Session, Bounded, Eventually, Customized", httpReq.Params["consistencyLevel"],
				"consistencyLevel can only be [Strong, Session, Bounded, Eventually, Customized], default: Bounded")
			log.Ctx(ctx).Warn("high level restful api, create collection fail", zap.Error(err), zap.Any("request", anyReq))
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(err),
				HTTPReturnMessage: err.Error(),
			})
			return nil, err
		}
	}
	req.ConsistencyLevel = consistencyLevel

	if partitionsNum > 0 {
		req.NumPartitions = partitionsNum
	}
	if _, ok := httpReq.Params["ttlSeconds"]; ok {
		req.Properties = append(req.Properties, &commonpb.KeyValuePair{
			Key:   common.CollectionTTLConfigKey,
			Value: fmt.Sprintf("%v", httpReq.Params["ttlSeconds"]),
		})
	}
	if _, ok := httpReq.Params["partitionKeyIsolation"]; ok {
		req.Properties = append(req.Properties, &commonpb.KeyValuePair{
			Key:   common.PartitionKeyIsolationKey,
			Value: fmt.Sprintf("%v", httpReq.Params["partitionKeyIsolation"]),
		})
	}
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/CreateCollection", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.CreateCollection(reqCtx, req.(*milvuspb.CreateCollectionRequest))
	})
	if err != nil {
		return resp, err
	}
	if len(httpReq.Schema.Fields) == 0 {
		if len(httpReq.MetricType) == 0 {
			httpReq.MetricType = DefaultMetricType
		}
		createIndexReq := &milvuspb.CreateIndexRequest{
			DbName:         dbName,
			CollectionName: httpReq.CollectionName,
			FieldName:      httpReq.VectorFieldName,
			IndexName:      httpReq.VectorFieldName,
			ExtraParams:    []*commonpb.KeyValuePair{{Key: common.MetricTypeKey, Value: httpReq.MetricType}},
		}
		statusResponse, err := wrapperProxyWithLimit(ctx, c, createIndexReq, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/CreateIndex", false, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
			return h.proxy.CreateIndex(ctx, req.(*milvuspb.CreateIndexRequest))
		})
		if err != nil {
			return statusResponse, err
		}
	} else {
		if len(httpReq.IndexParams) == 0 {
			HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
			return nil, nil
		}
		for _, indexParam := range httpReq.IndexParams {
			if _, ok := fieldNames[indexParam.FieldName]; !ok {
				HTTPAbortReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrMissingRequiredParameters),
					HTTPReturnMessage: merr.ErrMissingRequiredParameters.Error() + ", error: `" + indexParam.FieldName + "` hasn't defined in schema",
				})
				return nil, merr.ErrMissingRequiredParameters
			}
			createIndexReq := &milvuspb.CreateIndexRequest{
				DbName:         dbName,
				CollectionName: httpReq.CollectionName,
				FieldName:      indexParam.FieldName,
				IndexName:      indexParam.IndexName,
				ExtraParams:    []*commonpb.KeyValuePair{{Key: common.MetricTypeKey, Value: indexParam.MetricType}},
			}
			createIndexReq.ExtraParams, err = convertToExtraParams(indexParam)
			if err != nil {
				// will not happen
				log.Ctx(ctx).Warn("high level restful api, convertToExtraParams fail", zap.Error(err), zap.Any("request", anyReq))
				HTTPAbortReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(err),
					HTTPReturnMessage: err.Error(),
				})
				return resp, err
			}
			statusResponse, err := wrapperProxyWithLimit(ctx, c, createIndexReq, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/CreateIndex", false, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
				return h.proxy.CreateIndex(ctx, req.(*milvuspb.CreateIndexRequest))
			})
			if err != nil {
				return statusResponse, err
			}
		}
	}
	loadReq := &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
	}
	statusResponse, err := wrapperProxyWithLimit(ctx, c, loadReq, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/LoadCollection", false, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.LoadCollection(ctx, req.(*milvuspb.LoadCollectionRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return statusResponse, err
}

func (h *HandlersV2) createDatabase(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*DatabaseReqWithProperties)
	req := &milvuspb.CreateDatabaseRequest{
		DbName: dbName,
	}
	properties := make([]*commonpb.KeyValuePair, 0, len(httpReq.Properties))
	for key, value := range httpReq.Properties {
		properties = append(properties, &commonpb.KeyValuePair{Key: key, Value: fmt.Sprintf("%v", value)})
	}
	req.Properties = properties

	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/CreateDatabase", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.CreateDatabase(reqCtx, req.(*milvuspb.CreateDatabaseRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) dropDatabase(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	req := &milvuspb.DropDatabaseRequest{
		DbName: dbName,
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DropDatabase", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DropDatabase(reqCtx, req.(*milvuspb.DropDatabaseRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

// todo: use a more flexible way to handle the number of input parameters of req
func (h *HandlersV2) listDatabases(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	req := &milvuspb.ListDatabasesRequest{}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxy(ctx, c, req, false, false, "/milvus.proto.milvus.MilvusService/ListDatabases", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ListDatabases(reqCtx, req.(*milvuspb.ListDatabasesRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnList(resp.(*milvuspb.ListDatabasesResponse).DbNames))
	}
	return resp, err
}

func (h *HandlersV2) describeDatabase(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	req := &milvuspb.DescribeDatabaseRequest{
		DbName: dbName,
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DescribeDatabase", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DescribeDatabase(reqCtx, req.(*milvuspb.DescribeDatabaseRequest))
	})
	if err != nil {
		return nil, err
	}
	info, _ := resp.(*milvuspb.DescribeDatabaseResponse)
	if info.Properties == nil {
		info.Properties = []*commonpb.KeyValuePair{}
	}
	dataBaseInfo := map[string]any{
		HTTPDbName:     info.DbName,
		HTTPDbID:       info.DbID,
		HTTPProperties: info.Properties,
	}
	HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: dataBaseInfo})
	return resp, err
}

func (h *HandlersV2) alterDatabase(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*DatabaseReqWithProperties)
	req := &milvuspb.AlterDatabaseRequest{
		DbName: dbName,
	}
	properties := make([]*commonpb.KeyValuePair, 0, len(httpReq.Properties))
	for key, value := range httpReq.Properties {
		properties = append(properties, &commonpb.KeyValuePair{Key: key, Value: fmt.Sprintf("%v", value)})
	}
	req.Properties = properties

	c.Set(ContextRequest, req)
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/AlterDatabase", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.AlterDatabase(reqCtx, req.(*milvuspb.AlterDatabaseRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) listPartitions(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	req := &milvuspb.ShowPartitionsRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/ShowPartitions", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ShowPartitions(reqCtx, req.(*milvuspb.ShowPartitionsRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnList(resp.(*milvuspb.ShowPartitionsResponse).PartitionNames))
	}
	return resp, err
}

func (h *HandlersV2) hasPartitions(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	partitionGetter, _ := anyReq.(requestutil.PartitionNameGetter)
	req := &milvuspb.HasPartitionRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
		PartitionName:  partitionGetter.GetPartitionName(),
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/HasPartition", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.HasPartition(reqCtx, req.(*milvuspb.HasPartitionRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnHas(resp.(*milvuspb.BoolResponse).Value))
	}
	return resp, err
}

// data coord will collect partitions' row_count
// proxy grpc call only support partition not partitions
func (h *HandlersV2) statsPartition(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	partitionGetter, _ := anyReq.(requestutil.PartitionNameGetter)
	req := &milvuspb.GetPartitionStatisticsRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
		PartitionName:  partitionGetter.GetPartitionName(),
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/GetPartitionStatistics", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.GetPartitionStatistics(reqCtx, req.(*milvuspb.GetPartitionStatisticsRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnRowCount(resp.(*milvuspb.GetPartitionStatisticsResponse).Stats))
	}
	return resp, err
}

func (h *HandlersV2) createPartition(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	partitionGetter, _ := anyReq.(requestutil.PartitionNameGetter)
	req := &milvuspb.CreatePartitionRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
		PartitionName:  partitionGetter.GetPartitionName(),
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/CreatePartition", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.CreatePartition(reqCtx, req.(*milvuspb.CreatePartitionRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) dropPartition(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	partitionGetter, _ := anyReq.(requestutil.PartitionNameGetter)
	req := &milvuspb.DropPartitionRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
		PartitionName:  partitionGetter.GetPartitionName(),
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DropPartition", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DropPartition(reqCtx, req.(*milvuspb.DropPartitionRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) loadPartitions(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*PartitionsReq)
	req := &milvuspb.LoadPartitionsRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		PartitionNames: httpReq.PartitionNames,
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/LoadPartitions", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.LoadPartitions(reqCtx, req.(*milvuspb.LoadPartitionsRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) releasePartitions(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*PartitionsReq)
	req := &milvuspb.ReleasePartitionsRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		PartitionNames: httpReq.PartitionNames,
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/ReleasePartitions", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ReleasePartitions(reqCtx, req.(*milvuspb.ReleasePartitionsRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) listUsers(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	req := &milvuspb.ListCredUsersRequest{}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/ListCredUsers", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ListCredUsers(reqCtx, req.(*milvuspb.ListCredUsersRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnList(resp.(*milvuspb.ListCredUsersResponse).Usernames))
	}
	return resp, err
}

func (h *HandlersV2) describeUser(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	userNameGetter, _ := anyReq.(UserNameGetter)
	userName := userNameGetter.GetUserName()
	req := &milvuspb.SelectUserRequest{
		User: &milvuspb.UserEntity{
			Name: userName,
		},
		IncludeRoleInfo: true,
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/SelectUser", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.SelectUser(reqCtx, req.(*milvuspb.SelectUserRequest))
	})
	if err == nil {
		roleNames := []string{}
		for _, userRole := range resp.(*milvuspb.SelectUserResponse).Results {
			if userRole.User.Name == userName {
				for _, role := range userRole.Roles {
					roleNames = append(roleNames, role.Name)
				}
			}
		}
		HTTPReturn(c, http.StatusOK, wrapperReturnList(roleNames))
	}
	return resp, err
}

func (h *HandlersV2) createUser(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*PasswordReq)
	req := &milvuspb.CreateCredentialRequest{
		Username: httpReq.UserName,
		Password: crypto.Base64Encode(httpReq.Password),
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/CreateCredential", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.CreateCredential(reqCtx, req.(*milvuspb.CreateCredentialRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) updateUser(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*NewPasswordReq)
	req := &milvuspb.UpdateCredentialRequest{
		Username:    httpReq.UserName,
		OldPassword: crypto.Base64Encode(httpReq.Password),
		NewPassword: crypto.Base64Encode(httpReq.NewPassword),
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/UpdateCredential", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.UpdateCredential(reqCtx, req.(*milvuspb.UpdateCredentialRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) dropUser(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(UserNameGetter)
	req := &milvuspb.DeleteCredentialRequest{
		Username: getter.GetUserName(),
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DeleteCredential", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DeleteCredential(reqCtx, req.(*milvuspb.DeleteCredentialRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) operateRoleToUser(ctx context.Context, c *gin.Context, userName, roleName string, operateType milvuspb.OperateUserRoleType) (interface{}, error) {
	req := &milvuspb.OperateUserRoleRequest{
		Username: userName,
		RoleName: roleName,
		Type:     operateType,
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/OperateUserRole", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.OperateUserRole(reqCtx, req.(*milvuspb.OperateUserRoleRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) addRoleToUser(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	return h.operateRoleToUser(ctx, c, anyReq.(*UserRoleReq).UserName, anyReq.(*UserRoleReq).RoleName, milvuspb.OperateUserRoleType_AddUserToRole)
}

func (h *HandlersV2) removeRoleFromUser(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	return h.operateRoleToUser(ctx, c, anyReq.(*UserRoleReq).UserName, anyReq.(*UserRoleReq).RoleName, milvuspb.OperateUserRoleType_RemoveUserFromRole)
}

func (h *HandlersV2) listRoles(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	req := &milvuspb.SelectRoleRequest{}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/SelectRole", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.SelectRole(reqCtx, req.(*milvuspb.SelectRoleRequest))
	})
	if err == nil {
		roleNames := []string{}
		for _, role := range resp.(*milvuspb.SelectRoleResponse).Results {
			roleNames = append(roleNames, role.Role.Name)
		}
		HTTPReturn(c, http.StatusOK, wrapperReturnList(roleNames))
	}
	return resp, err
}

func (h *HandlersV2) describeRole(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(RoleNameGetter)
	req := &milvuspb.SelectGrantRequest{
		Entity: &milvuspb.GrantEntity{Role: &milvuspb.RoleEntity{Name: getter.GetRoleName()}, DbName: dbName},
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/SelectGrant", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.SelectGrant(reqCtx, req.(*milvuspb.SelectGrantRequest))
	})
	if err == nil {
		privileges := [](map[string]string){}
		for _, grant := range resp.(*milvuspb.SelectGrantResponse).Entities {
			privilege := map[string]string{
				HTTPReturnObjectType: grant.Object.Name,
				HTTPReturnObjectName: grant.ObjectName,
				HTTPReturnPrivilege:  grant.Grantor.Privilege.Name,
				HTTPReturnDbName:     grant.DbName,
				HTTPReturnGrantor:    grant.Grantor.User.Name,
			}
			privileges = append(privileges, privilege)
		}
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: privileges})
	}
	return resp, err
}

func (h *HandlersV2) createRole(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(RoleNameGetter)
	req := &milvuspb.CreateRoleRequest{
		Entity: &milvuspb.RoleEntity{Name: getter.GetRoleName()},
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/CreateRole", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.CreateRole(reqCtx, req.(*milvuspb.CreateRoleRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) dropRole(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(RoleNameGetter)
	req := &milvuspb.DropRoleRequest{
		RoleName: getter.GetRoleName(),
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DropRole", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DropRole(reqCtx, req.(*milvuspb.DropRoleRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) operatePrivilegeToRole(ctx context.Context, c *gin.Context, httpReq *GrantReq, operateType milvuspb.OperatePrivilegeType, dbName string) (interface{}, error) {
	req := &milvuspb.OperatePrivilegeRequest{
		Entity: &milvuspb.GrantEntity{
			Role:       &milvuspb.RoleEntity{Name: httpReq.RoleName},
			Object:     &milvuspb.ObjectEntity{Name: httpReq.ObjectType},
			ObjectName: httpReq.ObjectName,
			DbName:     dbName,
			Grantor: &milvuspb.GrantorEntity{
				Privilege: &milvuspb.PrivilegeEntity{Name: httpReq.Privilege},
			},
		},
		Type: operateType,
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/OperatePrivilege", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.OperatePrivilege(reqCtx, req.(*milvuspb.OperatePrivilegeRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) operatePrivilegeToRoleV2(ctx context.Context, c *gin.Context, httpReq *GrantV2Req, operateType milvuspb.OperatePrivilegeType) (interface{}, error) {
	req := &milvuspb.OperatePrivilegeV2Request{
		Role: &milvuspb.RoleEntity{Name: httpReq.RoleName},
		Grantor: &milvuspb.GrantorEntity{
			Privilege: &milvuspb.PrivilegeEntity{Name: httpReq.Privilege},
		},
		Type:           operateType,
		DbName:         httpReq.DbName,
		CollectionName: httpReq.CollectionName,
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/OperatePrivilege", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.OperatePrivilegeV2(reqCtx, req.(*milvuspb.OperatePrivilegeV2Request))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) grantV2(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	return h.operatePrivilegeToRoleV2(ctx, c, anyReq.(*GrantV2Req), milvuspb.OperatePrivilegeType_Grant)
}

func (h *HandlersV2) revokeV2(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	return h.operatePrivilegeToRoleV2(ctx, c, anyReq.(*GrantV2Req), milvuspb.OperatePrivilegeType_Revoke)
}

func (h *HandlersV2) addPrivilegeToRole(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	return h.operatePrivilegeToRole(ctx, c, anyReq.(*GrantReq), milvuspb.OperatePrivilegeType_Grant, dbName)
}

func (h *HandlersV2) removePrivilegeFromRole(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	return h.operatePrivilegeToRole(ctx, c, anyReq.(*GrantReq), milvuspb.OperatePrivilegeType_Revoke, dbName)
}

func (h *HandlersV2) createPrivilegeGroup(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*PrivilegeGroupReq)
	req := &milvuspb.CreatePrivilegeGroupRequest{
		GroupName: httpReq.PrivilegeGroupName,
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/CreatePrivilegeGroup", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.CreatePrivilegeGroup(reqCtx, req.(*milvuspb.CreatePrivilegeGroupRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) dropPrivilegeGroup(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*PrivilegeGroupReq)
	req := &milvuspb.DropPrivilegeGroupRequest{
		GroupName: httpReq.PrivilegeGroupName,
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DropPrivilegeGroup", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DropPrivilegeGroup(reqCtx, req.(*milvuspb.DropPrivilegeGroupRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) listPrivilegeGroups(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	req := &milvuspb.ListPrivilegeGroupsRequest{}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/ListPrivilegeGroups", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ListPrivilegeGroups(reqCtx, req.(*milvuspb.ListPrivilegeGroupsRequest))
	})
	if err == nil {
		privGroups := make([]map[string]interface{}, 0)
		for _, group := range resp.(*milvuspb.ListPrivilegeGroupsResponse).PrivilegeGroups {
			privileges := make([]string, len(group.Privileges))
			for i, privilege := range group.Privileges {
				privileges[i] = privilege.Name
			}
			groupInfo := map[string]interface{}{
				HTTPReturnPrivilegeGroupName: group.GroupName,
				HTTPReturnPrivileges:         strings.Join(privileges, ","),
			}
			privGroups = append(privGroups, groupInfo)
		}
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: gin.H{
			HTTPReturnPrivilegeGroups: privGroups,
		}})
	}
	return resp, err
}

func (h *HandlersV2) addPrivilegesToGroup(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	return h.operatePrivilegeGroup(ctx, c, anyReq, dbName, milvuspb.OperatePrivilegeGroupType_AddPrivilegesToGroup)
}

func (h *HandlersV2) removePrivilegesFromGroup(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	return h.operatePrivilegeGroup(ctx, c, anyReq, dbName, milvuspb.OperatePrivilegeGroupType_RemovePrivilegesFromGroup)
}

func (h *HandlersV2) operatePrivilegeGroup(ctx context.Context, c *gin.Context, anyReq any, dbName string, operateType milvuspb.OperatePrivilegeGroupType) (interface{}, error) {
	httpReq := anyReq.(*PrivilegeGroupReq)
	req := &milvuspb.OperatePrivilegeGroupRequest{
		GroupName: httpReq.PrivilegeGroupName,
		Privileges: lo.Map(httpReq.Privileges, func(p string, _ int) *milvuspb.PrivilegeEntity {
			return &milvuspb.PrivilegeEntity{Name: p}
		}),
		Type: operateType,
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/OperatePrivilegeGroup", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.OperatePrivilegeGroup(reqCtx, req.(*milvuspb.OperatePrivilegeGroupRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) listIndexes(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	indexNames := []string{}
	req := &milvuspb.DescribeIndexRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DescribeIndex", func(reqCtx context.Context, req any) (any, error) {
		resp, err := h.proxy.DescribeIndex(reqCtx, req.(*milvuspb.DescribeIndexRequest))
		if errors.Is(err, merr.ErrIndexNotFound) {
			return &milvuspb.DescribeIndexResponse{
				IndexDescriptions: []*milvuspb.IndexDescription{},
			}, nil
		}
		if resp != nil && errors.Is(merr.Error(resp.Status), merr.ErrIndexNotFound) {
			return &milvuspb.DescribeIndexResponse{
				IndexDescriptions: []*milvuspb.IndexDescription{},
			}, nil
		}
		return resp, err
	})
	if err != nil {
		return resp, err
	}
	for _, index := range resp.(*milvuspb.DescribeIndexResponse).IndexDescriptions {
		indexNames = append(indexNames, index.IndexName)
	}
	HTTPReturn(c, http.StatusOK, wrapperReturnList(indexNames))
	return resp, err
}

func (h *HandlersV2) describeIndex(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	indexGetter, _ := anyReq.(IndexNameGetter)
	req := &milvuspb.DescribeIndexRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
		IndexName:      indexGetter.GetIndexName(),
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DescribeIndex", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DescribeIndex(reqCtx, req.(*milvuspb.DescribeIndexRequest))
	})
	if err == nil {
		indexInfos := [](map[string]any){}
		for _, indexDescription := range resp.(*milvuspb.DescribeIndexResponse).IndexDescriptions {
			metricType := ""
			indexType := ""
			for _, pair := range indexDescription.Params {
				if pair.Key == common.MetricTypeKey {
					metricType = pair.Value
				} else if pair.Key == common.IndexTypeKey {
					indexType = pair.Value
				}
			}
			indexInfo := map[string]any{
				HTTPIndexName:              indexDescription.IndexName,
				HTTPIndexField:             indexDescription.FieldName,
				HTTPReturnIndexType:        indexType,
				HTTPReturnIndexMetricType:  metricType,
				HTTPReturnIndexTotalRows:   indexDescription.TotalRows,
				HTTPReturnIndexPendingRows: indexDescription.PendingIndexRows,
				HTTPReturnIndexIndexedRows: indexDescription.IndexedRows,
				HTTPReturnIndexState:       indexDescription.State.String(),
				HTTPReturnIndexFailReason:  indexDescription.IndexStateFailReason,
			}
			indexInfos = append(indexInfos, indexInfo)
		}
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: indexInfos})
	}
	return resp, err
}

func (h *HandlersV2) createIndex(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*IndexParamReq)
	for _, indexParam := range httpReq.IndexParams {
		req := &milvuspb.CreateIndexRequest{
			DbName:         dbName,
			CollectionName: httpReq.CollectionName,
			FieldName:      indexParam.FieldName,
			IndexName:      indexParam.IndexName,
			ExtraParams: []*commonpb.KeyValuePair{
				{Key: common.MetricTypeKey, Value: indexParam.MetricType},
			},
		}
		c.Set(ContextRequest, req)

		var err error
		req.ExtraParams, err = convertToExtraParams(indexParam)
		if err != nil {
			// will not happen
			log.Ctx(ctx).Warn("high level restful api, convertToExtraParams fail", zap.Error(err), zap.Any("request", anyReq))
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(err),
				HTTPReturnMessage: err.Error(),
			})
			return nil, err
		}
		resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/CreateIndex", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
			return h.proxy.CreateIndex(reqCtx, req.(*milvuspb.CreateIndexRequest))
		})
		if err != nil {
			return resp, err
		}
	}
	HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	return httpReq.IndexParams, nil
}

func (h *HandlersV2) dropIndex(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	indexGetter, _ := anyReq.(IndexNameGetter)
	req := &milvuspb.DropIndexRequest{
		DbName:         dbName,
		CollectionName: collGetter.GetCollectionName(),
		IndexName:      indexGetter.GetIndexName(),
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DropIndex", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DropIndex(reqCtx, req.(*milvuspb.DropIndexRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) alterIndexProperties(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*IndexReqWithProperties)
	req := &milvuspb.AlterIndexRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		IndexName:      httpReq.IndexName,
	}
	extraParams := make([]*commonpb.KeyValuePair, 0, len(httpReq.Properties))
	for key, value := range httpReq.Properties {
		extraParams = append(extraParams, &commonpb.KeyValuePair{Key: key, Value: fmt.Sprintf("%v", value)})
	}
	req.ExtraParams = extraParams

	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/AlterIndex", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.AlterIndex(reqCtx, req.(*milvuspb.AlterIndexRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) dropIndexProperties(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*DropIndexPropertiesReq)
	req := &milvuspb.AlterIndexRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		DeleteKeys:     httpReq.DeleteKeys,
	}
	c.Set(ContextRequest, req)
	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/AlterIndex", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.AlterIndex(reqCtx, req.(*milvuspb.AlterIndexRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) listAlias(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	req := &milvuspb.ListAliasesRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/ListAliases", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ListAliases(reqCtx, req.(*milvuspb.ListAliasesRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnList(resp.(*milvuspb.ListAliasesResponse).Aliases))
	}
	return resp, err
}

func (h *HandlersV2) describeAlias(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(AliasNameGetter)
	req := &milvuspb.DescribeAliasRequest{
		DbName: dbName,
		Alias:  getter.GetAliasName(),
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DescribeAlias", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DescribeAlias(reqCtx, req.(*milvuspb.DescribeAliasRequest))
	})
	if err == nil {
		response := resp.(*milvuspb.DescribeAliasResponse)
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: gin.H{
			HTTPDbName:         response.DbName,
			HTTPCollectionName: response.Collection,
			HTTPAliasName:      response.Alias,
		}})
	}
	return resp, err
}

func (h *HandlersV2) createAlias(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	aliasGetter, _ := anyReq.(AliasNameGetter)
	req := &milvuspb.CreateAliasRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
		Alias:          aliasGetter.GetAliasName(),
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/CreateAlias", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.CreateAlias(reqCtx, req.(*milvuspb.CreateAliasRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) dropAlias(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(AliasNameGetter)
	req := &milvuspb.DropAliasRequest{
		DbName: dbName,
		Alias:  getter.GetAliasName(),
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DropAlias", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DropAlias(reqCtx, req.(*milvuspb.DropAliasRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) alterAlias(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	aliasGetter, _ := anyReq.(AliasNameGetter)
	req := &milvuspb.AlterAliasRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
		Alias:          aliasGetter.GetAliasName(),
	}
	c.Set(ContextRequest, req)

	resp, err := wrapperProxyWithLimit(ctx, c, req, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/AlterAlias", true, h.proxy, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.AlterAlias(reqCtx, req.(*milvuspb.AlterAliasRequest))
	})
	if err == nil {
		HTTPReturn(c, http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) listImportJob(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	var collectionName string
	if collectionGetter, ok := anyReq.(requestutil.CollectionNameGetter); ok {
		collectionName = collectionGetter.GetCollectionName()
	}
	req := &internalpb.ListImportsRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	}
	c.Set(ContextRequest, req)

	if h.checkAuth {
		err := checkAuthorizationV2(ctx, c, false, &milvuspb.ListImportsAuthPlaceholder{
			DbName:         dbName,
			CollectionName: collectionName,
		})
		if err != nil {
			return nil, err
		}
	}
	resp, err := wrapperProxy(ctx, c, req, false, false, "/milvus.proto.milvus.MilvusService/ListImports", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ListImports(reqCtx, req.(*internalpb.ListImportsRequest))
	})
	if err == nil {
		returnData := make(map[string]interface{})
		records := make([]map[string]interface{}, 0)
		response := resp.(*internalpb.ListImportsResponse)
		for i, jobID := range response.GetJobIDs() {
			jobDetail := make(map[string]interface{})
			jobDetail["jobId"] = jobID
			jobDetail["collectionName"] = response.GetCollectionNames()[i]
			jobDetail["state"] = response.GetStates()[i].String()
			jobDetail["progress"] = response.GetProgresses()[i]
			reason := response.GetReasons()[i]
			if reason != "" {
				jobDetail["reason"] = reason
			}
			records = append(records, jobDetail)
		}
		returnData["records"] = records
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: returnData})
	}
	return resp, err
}

func (h *HandlersV2) createImportJob(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	var (
		collectionGetter = anyReq.(requestutil.CollectionNameGetter)
		partitionGetter  = anyReq.(requestutil.PartitionNameGetter)
		filesGetter      = anyReq.(FilesGetter)
		optionsGetter    = anyReq.(OptionsGetter)
	)
	req := &internalpb.ImportRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
		PartitionName:  partitionGetter.GetPartitionName(),
		Files: lo.Map(filesGetter.GetFiles(), func(paths []string, _ int) *internalpb.ImportFile {
			return &internalpb.ImportFile{Paths: paths}
		}),
		Options: funcutil.Map2KeyValuePair(optionsGetter.GetOptions()),
	}
	c.Set(ContextRequest, req)

	if h.checkAuth {
		err := checkAuthorizationV2(ctx, c, false, &milvuspb.ImportAuthPlaceholder{
			DbName:         dbName,
			CollectionName: collectionGetter.GetCollectionName(),
			PartitionName:  partitionGetter.GetPartitionName(),
		})
		if err != nil {
			return nil, err
		}
	}
	resp, err := wrapperProxy(ctx, c, req, false, false, "/milvus.proto.milvus.MilvusService/ImportV2", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ImportV2(reqCtx, req.(*internalpb.ImportRequest))
	})
	if err == nil {
		returnData := make(map[string]interface{})
		returnData["jobId"] = resp.(*internalpb.ImportResponse).GetJobID()
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: returnData})
	}
	return resp, err
}

func (h *HandlersV2) getImportJobProcess(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	jobIDGetter := anyReq.(JobIDGetter)
	req := &internalpb.GetImportProgressRequest{
		DbName: dbName,
		JobID:  jobIDGetter.GetJobID(),
	}
	c.Set(ContextRequest, req)

	if h.checkAuth {
		err := checkAuthorizationV2(ctx, c, false, &milvuspb.GetImportProgressAuthPlaceholder{
			DbName: dbName,
		})
		if err != nil {
			return nil, err
		}
	}
	resp, err := wrapperProxy(ctx, c, req, false, false, "/milvus.proto.milvus.MilvusService/GetImportProgress", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.GetImportProgress(reqCtx, req.(*internalpb.GetImportProgressRequest))
	})
	if err == nil {
		response := resp.(*internalpb.GetImportProgressResponse)
		returnData := make(map[string]interface{})
		returnData["jobId"] = jobIDGetter.GetJobID()
		returnData["collectionName"] = response.GetCollectionName()
		returnData["completeTime"] = response.GetCompleteTime()
		returnData["state"] = response.GetState().String()
		returnData["progress"] = response.GetProgress()
		returnData["importedRows"] = response.GetImportedRows()
		returnData["totalRows"] = response.GetTotalRows()
		reason := response.GetReason()
		if reason != "" {
			returnData["reason"] = reason
		}
		details := make([]map[string]interface{}, 0)
		totalFileSize := int64(0)
		for _, taskProgress := range response.GetTaskProgresses() {
			detail := make(map[string]interface{})
			detail["fileName"] = taskProgress.GetFileName()
			detail["fileSize"] = taskProgress.GetFileSize()
			detail["progress"] = taskProgress.GetProgress()
			detail["completeTime"] = taskProgress.GetCompleteTime()
			detail["state"] = taskProgress.GetState()
			detail["importedRows"] = taskProgress.GetImportedRows()
			detail["totalRows"] = taskProgress.GetTotalRows()
			reason = taskProgress.GetReason()
			if reason != "" {
				detail["reason"] = reason
			}
			details = append(details, detail)
			totalFileSize += taskProgress.GetFileSize()
		}
		returnData["fileSize"] = totalFileSize
		returnData["details"] = details
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(nil), HTTPReturnData: returnData})
	}
	return resp, err
}

func (h *HandlersV2) GetCollectionSchema(ctx context.Context, c *gin.Context, dbName, collectionName string) (*schemapb.CollectionSchema, error) {
	collSchema, err := proxy.GetCachedCollectionSchema(ctx, dbName, collectionName)
	if err == nil {
		return collSchema.CollectionSchema, nil
	}
	descReq := &milvuspb.DescribeCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	}
	descResp, err := wrapperProxy(ctx, c, descReq, h.checkAuth, false, "/milvus.proto.milvus.MilvusService/DescribeCollection", func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DescribeCollection(reqCtx, req.(*milvuspb.DescribeCollectionRequest))
	})
	if err != nil {
		return nil, err
	}
	response, _ := descResp.(*milvuspb.DescribeCollectionResponse)
	return response.Schema, nil
}
