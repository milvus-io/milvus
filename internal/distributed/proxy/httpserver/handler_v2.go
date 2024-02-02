package httpserver

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/validator/v10"
	"github.com/golang/protobuf/proto"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/requestutil"
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
	router.POST(CollectionCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &DatabaseReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.listCollections)))))
	router.POST(CollectionCategory+HasAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.hasCollection)))))
	// todo review the return data
	router.POST(CollectionCategory+DescribeAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.getCollectionDetails)))))
	router.POST(CollectionCategory+StatsAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.getCollectionStats)))))
	router.POST(CollectionCategory+LoadStateAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.getCollectionLoadState)))))
	router.POST(CollectionCategory+CreateAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.createCollection)))))
	router.POST(CollectionCategory+DropAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.dropCollection)))))
	router.POST(CollectionCategory+RenameAction, timeoutMiddleware(wrapperPost(func() any { return &RenameCollectionReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.renameCollection)))))
	router.POST(CollectionCategory+LoadAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.loadCollection)))))
	router.POST(CollectionCategory+ReleaseAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.releaseCollection)))))

	router.POST(EntityCategory+QueryAction, timeoutMiddleware(wrapperPost(func() any {
		return &QueryReqV2{
			Limit:        100,
			OutputFields: []string{DefaultOutputFields},
		}
	}, wrapperTraceLog(h.wrapperCheckDatabase(h.query)))))
	router.POST(EntityCategory+GetAction, timeoutMiddleware(wrapperPost(func() any {
		return &CollectionIDOutputReq{
			OutputFields: []string{DefaultOutputFields},
		}
	}, wrapperTraceLog(h.wrapperCheckDatabase(h.get)))))
	router.POST(EntityCategory+DeleteAction, timeoutMiddleware(wrapperPost(func() any {
		return &CollectionIDFilterReq{}
	}, wrapperTraceLog(h.wrapperCheckDatabase(h.delete)))))
	router.POST(EntityCategory+InsertAction, timeoutMiddleware(wrapperPost(func() any {
		return &CollectionDataReq{}
	}, wrapperTraceLog(h.wrapperCheckDatabase(h.insert)))))
	router.POST(EntityCategory+UpsertAction, timeoutMiddleware(wrapperPost(func() any {
		return &CollectionDataReq{}
	}, wrapperTraceLog(h.wrapperCheckDatabase(h.upsert)))))
	router.POST(EntityCategory+SearchAction, timeoutMiddleware(wrapperPost(func() any {
		return &SearchReqV2{
			Limit: 100,
		}
	}, wrapperTraceLog(h.wrapperCheckDatabase(h.search)))))

	router.POST(PartitionCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.listPartitions)))))
	router.POST(PartitionCategory+HasAction, timeoutMiddleware(wrapperPost(func() any { return &PartitionReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.hasPartitions)))))
	router.POST(PartitionCategory+StatsAction, timeoutMiddleware(wrapperPost(func() any { return &PartitionReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.statsPartition)))))

	router.POST(PartitionCategory+CreateAction, timeoutMiddleware(wrapperPost(func() any { return &PartitionReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.createPartition)))))
	router.POST(PartitionCategory+DropAction, timeoutMiddleware(wrapperPost(func() any { return &PartitionReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.dropPartition)))))
	router.POST(PartitionCategory+LoadAction, timeoutMiddleware(wrapperPost(func() any { return &PartitionsReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.loadPartitions)))))
	router.POST(PartitionCategory+ReleaseAction, timeoutMiddleware(wrapperPost(func() any { return &PartitionsReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.releasePartitions)))))

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

	router.POST(IndexCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.listIndexes)))))
	router.POST(IndexCategory+DescribeAction, timeoutMiddleware(wrapperPost(func() any { return &IndexReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.describeIndex)))))

	router.POST(IndexCategory+CreateAction, timeoutMiddleware(wrapperPost(func() any { return &IndexParamReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.createIndex)))))
	// todo cannot drop index before release it ?
	router.POST(IndexCategory+DropAction, timeoutMiddleware(wrapperPost(func() any { return &IndexReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.dropIndex)))))

	router.POST(AliasCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &DatabaseReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.listAlias)))))
	router.POST(AliasCategory+DescribeAction, timeoutMiddleware(wrapperPost(func() any { return &AliasReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.describeAlias)))))

	router.POST(AliasCategory+CreateAction, timeoutMiddleware(wrapperPost(func() any { return &AliasCollectionReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.createAlias)))))
	router.POST(AliasCategory+DropAction, timeoutMiddleware(wrapperPost(func() any { return &AliasReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.dropAlias)))))
	router.POST(AliasCategory+AlterAction, timeoutMiddleware(wrapperPost(func() any { return &AliasCollectionReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.alterAlias)))))

	router.POST(ImportJobCategory+ListAction, timeoutMiddleware(wrapperPost(func() any { return &CollectionNameReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.listImportJob)))))
	router.POST(ImportJobCategory+CreateAction, timeoutMiddleware(wrapperPost(func() any { return &DataFilesReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.createImportJob)))))
	router.POST(ImportJobCategory+GetProgressAction, timeoutMiddleware(wrapperPost(func() any { return &TaskIDReq{} }, wrapperTraceLog(h.wrapperCheckDatabase(h.getImportJobProcess)))))
}

type (
	newReqFunc    func() any
	handlerFuncV2 func(ctx context.Context, c *gin.Context, req any, dbName string) (interface{}, error)
)

func wrapperPost(newReq newReqFunc, v2 handlerFuncV2) gin.HandlerFunc {
	return func(c *gin.Context) {
		req := newReq()
		if err := c.ShouldBindBodyWith(req, binding.JSON); err != nil {
			log.Warn("high level restful api, read parameters from request body fail", zap.Any("request", req), zap.Error(err))
			if _, ok := err.(validator.ValidationErrors); ok {
				c.AbortWithStatusJSON(http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrMissingRequiredParameters),
					HTTPReturnMessage: merr.ErrMissingRequiredParameters.Error() + ", error: " + err.Error(),
				})
			} else if err == io.EOF {
				c.AbortWithStatusJSON(http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
					HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", the request body should be nil, however {} is valid",
				})
			} else {
				c.AbortWithStatusJSON(http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
					HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", error: " + err.Error(),
				})
			}
			return
		}
		log.Debug("high level restful api, read parameters from request body", zap.Any("request", req))
		dbName := ""
		if getter, ok := req.(requestutil.DBNameGetter); ok {
			dbName = getter.GetDbName()
		}
		if dbName == "" {
			dbName = DefaultDbName
		}
		username, _ := c.Get(ContextUsername)
		ctx := proxy.NewContextWithMetadata(c, username.(string), dbName)
		traceID := strconv.FormatInt(time.Now().UnixNano(), 10)
		ctx = log.WithTraceID(ctx, traceID)
		c.Keys["traceID"] = traceID
		v2(ctx, c, req, dbName)
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
				log.Ctx(ctx).Info("trace info: all, unknown", zap.Any("resp", resp))
			}
		}
		return resp, err
	}
}

func wrapperProxy(ctx context.Context, c *gin.Context, req any, checkAuth bool, ignoreErr bool, handler func(reqCtx context.Context, req any) (any, error)) (interface{}, error) {
	if checkAuth {
		err := checkAuthorization(ctx, c, req)
		if err != nil {
			return nil, err
		}
	}
	// todo delete the message
	log.Ctx(ctx).Debug("high level restful api, try to do a grpc call", zap.Any("request", req))
	response, err := handler(ctx, req)
	if err == nil {
		status, ok := requestutil.GetStatusFromResponse(response)
		if ok {
			err = merr.Error(status)
		}
	}
	if err != nil {
		log.Ctx(ctx).Warn("high level restful api, grpc call failed", zap.Error(err), zap.Any("request", req))
		if !ignoreErr {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		}
	}
	return response, err
}

func (h *HandlersV2) wrapperCheckDatabase(v2 handlerFuncV2) handlerFuncV2 {
	return func(ctx context.Context, c *gin.Context, req any, dbName string) (interface{}, error) {
		if dbName == DefaultDbName || proxy.CheckDatabase(ctx, dbName) {
			return v2(ctx, c, req, dbName)
		}
		resp, err := wrapperProxy(ctx, c, req, false, false, func(reqCtx context.Context, req any) (interface{}, error) {
			return h.proxy.ListDatabases(reqCtx, &milvuspb.ListDatabasesRequest{})
		})
		if err != nil {
			return resp, err
		}
		for _, db := range resp.(*milvuspb.ListDatabasesResponse).DbNames {
			if db == dbName {
				return v2(ctx, c, req, dbName)
			}
		}
		log.Ctx(ctx).Warn("high level restful api, non-exist database", zap.String("database", dbName), zap.Any("request", req))
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrDatabaseNotFound),
			HTTPReturnMessage: merr.ErrDatabaseNotFound.Error() + ", database: " + dbName,
		})
		return nil, merr.ErrDatabaseNotFound
	}
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
		resp, err := wrapperProxy(ctx, c, req, false, false, func(reqCtx context.Context, req any) (interface{}, error) {
			return h.proxy.HasCollection(reqCtx, req.(*milvuspb.HasCollectionRequest))
		})
		if err != nil {
			return nil, err
		}
		has = resp.(*milvuspb.BoolResponse).Value
	}
	c.JSON(http.StatusOK, wrapperReturnHas(has))
	return has, nil
}

func (h *HandlersV2) listCollections(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	req := &milvuspb.ShowCollectionsRequest{
		DbName: dbName,
	}
	resp, err := wrapperProxy(ctx, c, req, false, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ShowCollections(reqCtx, req.(*milvuspb.ShowCollectionsRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnList(resp.(*milvuspb.ShowCollectionsResponse).CollectionNames))
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
	resp, err := wrapperProxy(ctx, c, req, false, false, func(reqCtx context.Context, req any) (any, error) {
		return h.proxy.DescribeCollection(reqCtx, req.(*milvuspb.DescribeCollectionRequest))
	})
	if err != nil {
		return resp, err
	}
	coll := resp.(*milvuspb.DescribeCollectionResponse)
	primaryField, ok := getPrimaryField(coll.Schema)
	autoID := false
	if !ok {
		log.Ctx(ctx).Warn("high level restful api, get primary field from collection schema fail", zap.Any("collection schema", coll.Schema))
	} else {
		autoID = primaryField.AutoID
	}
	loadStateReq := &milvuspb.GetLoadStateRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	}
	stateResp, err := wrapperProxy(ctx, c, loadStateReq, h.checkAuth, true, func(reqCtx context.Context, req any) (any, error) {
		return h.proxy.GetLoadState(reqCtx, req.(*milvuspb.GetLoadStateRequest))
	})
	collLoadState := ""
	if err == nil {
		collLoadState = stateResp.(*milvuspb.GetLoadStateResponse).State.String()
	}
	vectorField := ""
	for _, field := range coll.Schema.Fields {
		if field.DataType == schemapb.DataType_BinaryVector || field.DataType == schemapb.DataType_FloatVector {
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
	indexResp, err := wrapperProxy(ctx, c, descIndexReq, false, true, func(reqCtx context.Context, req any) (any, error) {
		return h.proxy.DescribeIndex(reqCtx, req.(*milvuspb.DescribeIndexRequest))
	})
	if err == nil {
		indexDesc = printIndexes(indexResp.(*milvuspb.DescribeIndexResponse).IndexDescriptions)
	}
	c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{
		HTTPCollectionName:    coll.CollectionName,
		HTTPReturnDescription: coll.Schema.Description,
		HTTPReturnFieldAutoID: autoID,
		"fields":              printFields(coll.Schema.Fields),
		"indexes":             indexDesc,
		"load":                collLoadState,
		"shardsNum":           coll.ShardsNum,
		"enableDynamicField":  coll.Schema.EnableDynamicField,
	}})
	return resp, nil
}

func (h *HandlersV2) getCollectionStats(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	req := &milvuspb.GetCollectionStatisticsRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (any, error) {
		return h.proxy.GetCollectionStatistics(reqCtx, req.(*milvuspb.GetCollectionStatisticsRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnRowCount(resp.(*milvuspb.GetCollectionStatisticsResponse).Stats))
	}
	return resp, err
}

func (h *HandlersV2) getCollectionLoadState(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	req := &milvuspb.GetLoadStateRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (any, error) {
		return h.proxy.GetLoadState(reqCtx, req.(*milvuspb.GetLoadStateRequest))
	})
	if err != nil {
		return resp, err
	}
	if resp.(*milvuspb.GetLoadStateResponse).State == commonpb.LoadState_LoadStateNotExist {
		err = merr.WrapErrCollectionNotFound(req.CollectionName)
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return resp, err
	} else if resp.(*milvuspb.GetLoadStateResponse).State == commonpb.LoadState_LoadStateNotLoad {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{
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
	progressResp, err := wrapperProxy(ctx, c, progressReq, h.checkAuth, true, func(reqCtx context.Context, req any) (any, error) {
		return h.proxy.GetLoadingProgress(reqCtx, req.(*milvuspb.GetLoadingProgressRequest))
	})
	progress := int64(-1)
	if err == nil {
		progress = progressResp.(*milvuspb.GetLoadingProgressResponse).Progress
	}
	state := commonpb.LoadState_LoadStateLoading.String()
	if progress >= 100 {
		state = commonpb.LoadState_LoadStateLoaded.String()
	}
	c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{
		HTTPReturnLoadState:    state,
		HTTPReturnLoadProgress: progress,
	}})
	return resp, err
}

func (h *HandlersV2) dropCollection(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(requestutil.CollectionNameGetter)
	req := &milvuspb.DropCollectionRequest{
		DbName:         dbName,
		CollectionName: getter.GetCollectionName(),
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DropCollection(reqCtx, req.(*milvuspb.DropCollectionRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) renameCollection(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*RenameCollectionReq)
	req := &milvuspb.RenameCollectionRequest{
		DbName:    dbName,
		OldName:   httpReq.CollectionName,
		NewName:   httpReq.NewCollectionName,
		NewDBName: dbName,
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.RenameCollection(reqCtx, req.(*milvuspb.RenameCollectionRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) loadCollection(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(requestutil.CollectionNameGetter)
	req := &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: getter.GetCollectionName(),
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.LoadCollection(reqCtx, req.(*milvuspb.LoadCollectionRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) releaseCollection(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(requestutil.CollectionNameGetter)
	req := &milvuspb.ReleaseCollectionRequest{
		DbName:         dbName,
		CollectionName: getter.GetCollectionName(),
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ReleaseCollection(reqCtx, req.(*milvuspb.ReleaseCollectionRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) query(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*QueryReqV2)
	req := &milvuspb.QueryRequest{
		DbName:             dbName,
		CollectionName:     httpReq.CollectionName,
		Expr:               httpReq.Filter,
		OutputFields:       httpReq.OutputFields,
		PartitionNames:     httpReq.PartitionNames,
		GuaranteeTimestamp: BoundedTimestamp,
		QueryParams:        []*commonpb.KeyValuePair{},
	}
	if httpReq.Offset > 0 {
		req.QueryParams = append(req.QueryParams, &commonpb.KeyValuePair{Key: ParamOffset, Value: strconv.FormatInt(int64(httpReq.Offset), 10)})
	}
	if httpReq.Limit > 0 {
		req.QueryParams = append(req.QueryParams, &commonpb.KeyValuePair{Key: ParamLimit, Value: strconv.FormatInt(int64(httpReq.Limit), 10)})
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.Query(reqCtx, req.(*milvuspb.QueryRequest))
	})
	if err == nil {
		queryResp := resp.(*milvuspb.QueryResults)
		allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
		outputData, err := buildQueryResp(int64(0), queryResp.OutputFields, queryResp.FieldsData, nil, nil, allowJS)
		if err != nil {
			log.Ctx(ctx).Warn("high level restful api, fail to deal with query result", zap.Any("response", resp), zap.Error(err))
			c.JSON(http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrInvalidSearchResult),
				HTTPReturnMessage: merr.ErrInvalidSearchResult.Error() + ", error: " + err.Error(),
			})
		} else {
			c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: outputData})
		}
	}
	return resp, err
}

func (h *HandlersV2) get(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionIDOutputReq)
	collSchema, err := h.GetCollectionSchema(ctx, c, dbName, httpReq.CollectionName)
	if err != nil {
		return nil, err
	}
	body, _ := c.Get(gin.BodyBytesKey)
	filter, err := checkGetPrimaryKey(collSchema, gjson.Get(string(body.([]byte)), DefaultPrimaryFieldName))
	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrCheckPrimaryKey),
			HTTPReturnMessage: merr.ErrCheckPrimaryKey.Error() + ", error: " + err.Error(),
		})
		return nil, err
	}
	req := &milvuspb.QueryRequest{
		DbName:             dbName,
		CollectionName:     httpReq.CollectionName,
		OutputFields:       httpReq.OutputFields,
		PartitionNames:     httpReq.PartitionNames,
		GuaranteeTimestamp: BoundedTimestamp,
		Expr:               filter,
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.Query(reqCtx, req.(*milvuspb.QueryRequest))
	})
	if err == nil {
		queryResp := resp.(*milvuspb.QueryResults)
		allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
		outputData, err := buildQueryResp(int64(0), queryResp.OutputFields, queryResp.FieldsData, nil, nil, allowJS)
		if err != nil {
			log.Ctx(ctx).Warn("high level restful api, fail to deal with get result", zap.Any("response", resp), zap.Error(err))
			c.JSON(http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrInvalidSearchResult),
				HTTPReturnMessage: merr.ErrInvalidSearchResult.Error() + ", error: " + err.Error(),
			})
		} else {
			c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: outputData})
		}
	}
	return resp, err
}

func (h *HandlersV2) delete(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionIDFilterReq)
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
	if req.Expr == "" {
		body, _ := c.Get(gin.BodyBytesKey)
		filter, err := checkGetPrimaryKey(collSchema, gjson.Get(string(body.([]byte)), DefaultPrimaryFieldName))
		if err != nil {
			c.JSON(http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrCheckPrimaryKey),
				HTTPReturnMessage: merr.ErrCheckPrimaryKey.Error() + ", error: " + err.Error(),
			})
			return nil, err
		}
		req.Expr = filter
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.Delete(reqCtx, req.(*milvuspb.DeleteRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) insert(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionDataReq)
	collSchema, err := h.GetCollectionSchema(ctx, c, dbName, httpReq.CollectionName)
	if err != nil {
		return nil, err
	}
	body, _ := c.Get(gin.BodyBytesKey)
	err, httpReq.Data = checkAndSetData(string(body.([]byte)), collSchema)
	if err != nil {
		log.Ctx(ctx).Warn("high level restful api, fail to deal with insert data", zap.Any("body", body), zap.Error(err))
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrInvalidInsertData),
			HTTPReturnMessage: merr.ErrInvalidInsertData.Error() + ", error: " + err.Error(),
		})
		return nil, err
	}
	req := &milvuspb.InsertRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		PartitionName:  httpReq.PartitionName,
		// PartitionName:  "_default",
		NumRows: uint32(len(httpReq.Data)),
	}
	req.FieldsData, err = anyToColumns(httpReq.Data, collSchema)
	if err != nil {
		log.Ctx(ctx).Warn("high level restful api, fail to deal with insert data", zap.Any("data", httpReq.Data), zap.Error(err))
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrInvalidInsertData),
			HTTPReturnMessage: merr.ErrInvalidInsertData.Error() + ", error: " + err.Error(),
		})
		return nil, err
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.Insert(reqCtx, req.(*milvuspb.InsertRequest))
	})
	if err == nil {
		insertResp := resp.(*milvuspb.MutationResult)
		switch insertResp.IDs.GetIdField().(type) {
		case *schemapb.IDs_IntId:
			allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
			if allowJS {
				c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{"insertCount": insertResp.InsertCnt, "insertIds": insertResp.IDs.IdField.(*schemapb.IDs_IntId).IntId.Data}})
			} else {
				c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{"insertCount": insertResp.InsertCnt, "insertIds": formatInt64(insertResp.IDs.IdField.(*schemapb.IDs_IntId).IntId.Data)}})
			}
		case *schemapb.IDs_StrId:
			c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{"insertCount": insertResp.InsertCnt, "insertIds": insertResp.IDs.IdField.(*schemapb.IDs_StrId).StrId.Data}})
		default:
			c.JSON(http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrCheckPrimaryKey),
				HTTPReturnMessage: merr.ErrCheckPrimaryKey.Error() + ", error: unsupported primary key data type",
			})
		}
	}
	return resp, err
}

func (h *HandlersV2) upsert(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionDataReq)
	collSchema, err := h.GetCollectionSchema(ctx, c, dbName, httpReq.CollectionName)
	if err != nil {
		return nil, err
	}
	if collSchema.AutoID {
		err := merr.WrapErrParameterInvalid("autoID: false", "autoID: true", "cannot upsert an autoID collection")
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return nil, err
	}
	body, _ := c.Get(gin.BodyBytesKey)
	err, httpReq.Data = checkAndSetData(string(body.([]byte)), collSchema)
	if err != nil {
		log.Ctx(ctx).Warn("high level restful api, fail to deal with upsert data", zap.Any("body", body), zap.Error(err))
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrInvalidInsertData),
			HTTPReturnMessage: merr.ErrInvalidInsertData.Error() + ", error: " + err.Error(),
		})
		return nil, err
	}
	req := &milvuspb.UpsertRequest{
		DbName:         dbName,
		CollectionName: httpReq.CollectionName,
		PartitionName:  httpReq.PartitionName,
		// PartitionName:  "_default",
		NumRows: uint32(len(httpReq.Data)),
	}
	req.FieldsData, err = anyToColumns(httpReq.Data, collSchema)
	if err != nil {
		log.Ctx(ctx).Warn("high level restful api, fail to deal with upsert data", zap.Any("data", httpReq.Data), zap.Error(err))
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrInvalidInsertData),
			HTTPReturnMessage: merr.ErrInvalidInsertData.Error() + ", error: " + err.Error(),
		})
		return nil, err
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.Upsert(reqCtx, req.(*milvuspb.UpsertRequest))
	})
	if err == nil {
		upsertResp := resp.(*milvuspb.MutationResult)
		switch upsertResp.IDs.GetIdField().(type) {
		case *schemapb.IDs_IntId:
			allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
			if allowJS {
				c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{"upsertCount": upsertResp.UpsertCnt, "upsertIds": upsertResp.IDs.IdField.(*schemapb.IDs_IntId).IntId.Data}})
			} else {
				c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{"upsertCount": upsertResp.UpsertCnt, "upsertIds": formatInt64(upsertResp.IDs.IdField.(*schemapb.IDs_IntId).IntId.Data)}})
			}
		case *schemapb.IDs_StrId:
			c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{"upsertCount": upsertResp.UpsertCnt, "upsertIds": upsertResp.IDs.IdField.(*schemapb.IDs_StrId).StrId.Data}})
		default:
			c.JSON(http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrCheckPrimaryKey),
				HTTPReturnMessage: merr.ErrCheckPrimaryKey.Error() + ", error: unsupported primary key data type",
			})
		}
	}
	return resp, err
}

func (h *HandlersV2) search(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*SearchReqV2)
	params := map[string]interface{}{ // auto generated mapping
		"level": int(commonpb.ConsistencyLevel_Bounded),
	}
	if httpReq.Params != nil {
		radius, radiusOk := httpReq.Params[ParamRadius]
		rangeFilter, rangeFilterOk := httpReq.Params[ParamRangeFilter]
		if rangeFilterOk {
			if !radiusOk {
				log.Ctx(ctx).Warn("high level restful api, search params invalid, because only " + ParamRangeFilter)
				c.AbortWithStatusJSON(http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
					HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", error: invalid search params",
				})
				return nil, merr.ErrIncorrectParameterFormat
			}
			params[ParamRangeFilter] = rangeFilter
		}
		if radiusOk {
			params[ParamRadius] = radius
		}
	}
	bs, _ := json.Marshal(params)
	searchParams := []*commonpb.KeyValuePair{
		{Key: common.TopKKey, Value: strconv.FormatInt(int64(httpReq.Limit), 10)},
		{Key: Params, Value: string(bs)},
		{Key: ParamRoundDecimal, Value: "-1"},
		{Key: ParamOffset, Value: strconv.FormatInt(int64(httpReq.Offset), 10)},
	}
	req := &milvuspb.SearchRequest{
		DbName:           dbName,
		CollectionName:   httpReq.CollectionName,
		Dsl:              httpReq.Filter,
		PlaceholderGroup: vector2PlaceholderGroupBytes(httpReq.Vector),
		DslType:          commonpb.DslType_BoolExprV1,
		OutputFields:     httpReq.OutputFields,
		// PartitionNames:     httpReq.PartitionNames,
		SearchParams:       searchParams,
		GuaranteeTimestamp: BoundedTimestamp,
		Nq:                 int64(1),
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.Search(reqCtx, req.(*milvuspb.SearchRequest))
	})
	if err == nil {
		searchResp := resp.(*milvuspb.SearchResults)
		if searchResp.Results.TopK == int64(0) {
			c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: []interface{}{}})
		} else {
			allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
			outputData, err := buildQueryResp(searchResp.Results.TopK, searchResp.Results.OutputFields, searchResp.Results.FieldsData, searchResp.Results.Ids, searchResp.Results.Scores, allowJS)
			if err != nil {
				log.Ctx(ctx).Warn("high level restful api, fail to deal with search result", zap.Any("result", searchResp.Results), zap.Error(err))
				c.JSON(http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrInvalidSearchResult),
					HTTPReturnMessage: merr.ErrInvalidSearchResult.Error() + ", error: " + err.Error(),
				})
			} else {
				c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: outputData})
			}
		}
	}
	return resp, err
}

func (h *HandlersV2) createCollection(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*CollectionReq)
	var schema []byte
	vectorFieldNum := 0
	valid := true
	var err error
	if httpReq.Schema.Fields == nil || len(httpReq.Schema.Fields) == 0 {
		schema, err = proto.Marshal(&schemapb.CollectionSchema{
			Name:   httpReq.CollectionName,
			AutoID: EnableAutoID,
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:      common.StartOfUserFieldID,
					Name:         PrimaryFieldName,
					IsPrimaryKey: true,
					DataType:     schemapb.DataType_Int64,
					AutoID:       EnableAutoID,
				},
				{
					FieldID:      common.StartOfUserFieldID + 1,
					Name:         VectorFieldName,
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
			EnableDynamicField: EnableDynamic,
		})
	} else {
		collSchema := schemapb.CollectionSchema{
			Name:               httpReq.CollectionName,
			AutoID:             EnableAutoID,
			Fields:             []*schemapb.FieldSchema{},
			EnableDynamicField: EnableDynamic,
		}
		allFields := map[string]bool{}
		for _, field := range httpReq.Schema.Fields {
			dataType := schemapb.DataType(schemapb.DataType_value[field.DataType])
			if dataType == schemapb.DataType_BinaryVector || dataType == schemapb.DataType_FloatVector || dataType == schemapb.DataType_Float16Vector {
				allFields[field.FieldName] = true
				vectorFieldNum++
			} else {
				allFields[field.FieldName] = false
			}
			fieldSchema := schemapb.FieldSchema{
				Name:         field.FieldName,
				IsPrimaryKey: field.IsPrimary,
				DataType:     dataType,
				TypeParams:   []*commonpb.KeyValuePair{},
			}
			if field.IsPrimary {
				fieldSchema.AutoID = httpReq.Schema.AutoId
			}
			for key, fieldParam := range field.ElementTypeParams {
				fieldSchema.TypeParams = append(fieldSchema.TypeParams, &commonpb.KeyValuePair{Key: key, Value: fieldParam})
			}
			allFields[field.FieldName] = true
			collSchema.Fields = append(collSchema.Fields, &fieldSchema)
		}
		for _, indexParam := range httpReq.IndexParams {
			vectorField, ok := allFields[indexParam.FieldName]
			if ok {
				if !vectorField {
					valid = false // create index for scaler field is not supported
				} else {
					vectorFieldNum--
				}
			} else {
				c.AbortWithStatusJSON(http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrMissingRequiredParameters),
					HTTPReturnMessage: merr.ErrMissingRequiredParameters.Error() + ", error: `" + indexParam.FieldName + "` hasn't defined in schema",
				})
				return nil, merr.ErrMissingRequiredParameters
			}
		}
		schema, err = proto.Marshal(&collSchema)
	}
	if err != nil {
		log.Ctx(ctx).Warn("high level restful api, marshal collection schema fail", zap.Any("request", httpReq), zap.Error(err))
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrMarshalCollectionSchema),
			HTTPReturnMessage: merr.ErrMarshalCollectionSchema.Error() + ", error: " + err.Error(),
		})
		return nil, err
	}
	req := &milvuspb.CreateCollectionRequest{
		DbName:           dbName,
		CollectionName:   httpReq.CollectionName,
		Schema:           schema,
		ShardsNum:        ShardNumDefault,
		ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.CreateCollection(reqCtx, req.(*milvuspb.CreateCollectionRequest))
	})
	if err != nil {
		return resp, err
	}
	if !valid || vectorFieldNum > 0 {
		c.JSON(http.StatusOK, wrapperReturnDefault())
		return resp, err
	}
	if httpReq.Schema.Fields == nil || len(httpReq.Schema.Fields) == 0 {
		createIndexReq := &milvuspb.CreateIndexRequest{
			DbName:         dbName,
			CollectionName: httpReq.CollectionName,
			FieldName:      VectorFieldName,
			IndexName:      VectorFieldName,
			ExtraParams:    []*commonpb.KeyValuePair{{Key: common.MetricTypeKey, Value: httpReq.MetricsType}},
		}
		statusResponse, err := wrapperProxy(ctx, c, createIndexReq, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
			return h.proxy.CreateIndex(ctx, req.(*milvuspb.CreateIndexRequest))
		})
		if err != nil {
			return statusResponse, err
		}
	} else {
		for _, indexParam := range httpReq.IndexParams {
			createIndexReq := &milvuspb.CreateIndexRequest{
				DbName:         dbName,
				CollectionName: httpReq.CollectionName,
				FieldName:      indexParam.FieldName,
				IndexName:      indexParam.IndexName,
				ExtraParams:    []*commonpb.KeyValuePair{{Key: common.MetricTypeKey, Value: indexParam.MetricsType}},
			}
			statusResponse, err := wrapperProxy(ctx, c, createIndexReq, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
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
	statusResponse, err := wrapperProxy(ctx, c, loadReq, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.LoadCollection(ctx, req.(*milvuspb.LoadCollectionRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnDefault())
	}
	return statusResponse, err
}

func (h *HandlersV2) listPartitions(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	req := &milvuspb.ShowPartitionsRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ShowPartitions(reqCtx, req.(*milvuspb.ShowPartitionsRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnList(resp.(*milvuspb.ShowPartitionsResponse).PartitionNames))
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
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.HasPartition(reqCtx, req.(*milvuspb.HasPartitionRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnHas(resp.(*milvuspb.BoolResponse).Value))
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
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.GetPartitionStatistics(reqCtx, req.(*milvuspb.GetPartitionStatisticsRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnRowCount(resp.(*milvuspb.GetPartitionStatisticsResponse).Stats))
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
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.CreatePartition(reqCtx, req.(*milvuspb.CreatePartitionRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnDefault())
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
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DropPartition(reqCtx, req.(*milvuspb.DropPartitionRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnDefault())
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
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.LoadPartitions(reqCtx, req.(*milvuspb.LoadPartitionsRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnDefault())
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
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ReleasePartitions(reqCtx, req.(*milvuspb.ReleasePartitionsRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) listUsers(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	req := &milvuspb.ListCredUsersRequest{}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ListCredUsers(reqCtx, req.(*milvuspb.ListCredUsersRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnList(resp.(*milvuspb.ListCredUsersResponse).Usernames))
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
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
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
		c.JSON(http.StatusOK, wrapperReturnList(roleNames))
	}
	return resp, err
}

func (h *HandlersV2) createUser(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*PasswordReq)
	req := &milvuspb.CreateCredentialRequest{
		Username: httpReq.UserName,
		Password: httpReq.Password,
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.CreateCredential(reqCtx, req.(*milvuspb.CreateCredentialRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) updateUser(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	httpReq := anyReq.(*NewPasswordReq)
	req := &milvuspb.UpdateCredentialRequest{
		Username:    httpReq.UserName,
		OldPassword: httpReq.Password,
		NewPassword: httpReq.NewPassword,
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.UpdateCredential(reqCtx, req.(*milvuspb.UpdateCredentialRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) dropUser(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(UserNameGetter)
	req := &milvuspb.DeleteCredentialRequest{
		Username: getter.GetUserName(),
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DeleteCredential(reqCtx, req.(*milvuspb.DeleteCredentialRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) operateRoleToUser(ctx context.Context, c *gin.Context, userName, roleName string, operateType milvuspb.OperateUserRoleType) (interface{}, error) {
	req := &milvuspb.OperateUserRoleRequest{
		Username: userName,
		RoleName: roleName,
		Type:     operateType,
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.OperateUserRole(reqCtx, req.(*milvuspb.OperateUserRoleRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnDefault())
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
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.SelectRole(reqCtx, req.(*milvuspb.SelectRoleRequest))
	})
	if err == nil {
		roleNames := []string{}
		for _, role := range resp.(*milvuspb.SelectRoleResponse).Results {
			roleNames = append(roleNames, role.Role.Name)
		}
		c.JSON(http.StatusOK, wrapperReturnList(roleNames))
	}
	return resp, err
}

func (h *HandlersV2) describeRole(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(RoleNameGetter)
	req := &milvuspb.SelectGrantRequest{
		Entity: &milvuspb.GrantEntity{Role: &milvuspb.RoleEntity{Name: getter.GetRoleName()}},
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
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
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: privileges})
	}
	return resp, err
}

func (h *HandlersV2) createRole(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(RoleNameGetter)
	req := &milvuspb.CreateRoleRequest{
		Entity: &milvuspb.RoleEntity{Name: getter.GetRoleName()},
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.CreateRole(reqCtx, req.(*milvuspb.CreateRoleRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) dropRole(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(RoleNameGetter)
	req := &milvuspb.DropRoleRequest{
		RoleName: getter.GetRoleName(),
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DropRole(reqCtx, req.(*milvuspb.DropRoleRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnDefault())
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
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.OperatePrivilege(reqCtx, req.(*milvuspb.OperatePrivilegeRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) addPrivilegeToRole(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	return h.operatePrivilegeToRole(ctx, c, anyReq.(*GrantReq), milvuspb.OperatePrivilegeType_Grant, dbName)
}

func (h *HandlersV2) removePrivilegeFromRole(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	return h.operatePrivilegeToRole(ctx, c, anyReq.(*GrantReq), milvuspb.OperatePrivilegeType_Revoke, dbName)
}

func (h *HandlersV2) listIndexes(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	indexNames := []string{}
	req := &milvuspb.DescribeIndexRequest{
		DbName:         dbName,
		CollectionName: collectionGetter.GetCollectionName(),
	}
	resp, err := wrapperProxy(ctx, c, req, false, false, func(reqCtx context.Context, req any) (any, error) {
		return h.proxy.DescribeIndex(reqCtx, req.(*milvuspb.DescribeIndexRequest))
	})
	if err != nil {
		return resp, err
	}
	for _, index := range resp.(*milvuspb.DescribeIndexResponse).IndexDescriptions {
		indexNames = append(indexNames, index.IndexName)
	}
	c.JSON(http.StatusOK, wrapperReturnList(indexNames))
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
	resp, err := wrapperProxy(ctx, c, req, false, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DescribeIndex(reqCtx, req.(*milvuspb.DescribeIndexRequest))
	})
	if err == nil {
		indexInfos := [](map[string]any){}
		for _, indexDescription := range resp.(*milvuspb.DescribeIndexResponse).IndexDescriptions {
			metricsType := ""
			indexType := ""
			for _, pair := range indexDescription.Params {
				if pair.Key == common.MetricTypeKey {
					metricsType = pair.Value
				} else if pair.Key == common.IndexTypeKey {
					indexType = pair.Value
				}
			}
			indexInfo := map[string]any{
				HTTPIndexName:              indexDescription.IndexName,
				HTTPIndexField:             indexDescription.FieldName,
				HTTPReturnIndexType:        indexType,
				HTTPReturnIndexMetricsType: metricsType,
				HTTPReturnIndexTotalRows:   indexDescription.TotalRows,
				HTTPReturnIndexPendingRows: indexDescription.PendingIndexRows,
				HTTPReturnIndexIndexedRows: indexDescription.IndexedRows,
				HTTPReturnIndexState:       indexDescription.State.String(),
				HTTPReturnIndexFailReason:  indexDescription.IndexStateFailReason,
			}
			indexInfos = append(indexInfos, indexInfo)
		}
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: indexInfos})
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
				{Key: common.MetricTypeKey, Value: indexParam.MetricsType},
			},
		}
		if indexParam.IndexType != "" {
			req.ExtraParams = append(req.ExtraParams, &commonpb.KeyValuePair{Key: common.IndexTypeKey, Value: indexParam.IndexType})
		}
		resp, err := wrapperProxy(ctx, c, req, false, false, func(reqCtx context.Context, req any) (interface{}, error) {
			return h.proxy.CreateIndex(reqCtx, req.(*milvuspb.CreateIndexRequest))
		})
		if err != nil {
			return resp, err
		}
	}
	c.JSON(http.StatusOK, wrapperReturnDefault())
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
	resp, err := wrapperProxy(ctx, c, req, false, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DropIndex(reqCtx, req.(*milvuspb.DropIndexRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) listAlias(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	req := &milvuspb.ListAliasesRequest{
		DbName: dbName,
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ListAliases(reqCtx, req.(*milvuspb.ListAliasesRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnList(resp.(*milvuspb.ListAliasesResponse).Aliases))
	}
	return resp, err
}

func (h *HandlersV2) describeAlias(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(AliasNameGetter)
	req := &milvuspb.DescribeAliasRequest{
		DbName: dbName,
		Alias:  getter.GetAliasName(),
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DescribeAlias(reqCtx, req.(*milvuspb.DescribeAliasRequest))
	})
	if err == nil {
		response := resp.(*milvuspb.DescribeAliasResponse)
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{
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
	resp, err := wrapperProxy(ctx, c, req, false, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.CreateAlias(reqCtx, req.(*milvuspb.CreateAliasRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) dropAlias(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	getter, _ := anyReq.(AliasNameGetter)
	req := &milvuspb.DropAliasRequest{
		DbName: dbName,
		Alias:  getter.GetAliasName(),
	}
	resp, err := wrapperProxy(ctx, c, req, false, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DropAlias(reqCtx, req.(*milvuspb.DropAliasRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnDefault())
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
	resp, err := wrapperProxy(ctx, c, req, false, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.AlterAlias(reqCtx, req.(*milvuspb.AlterAliasRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, wrapperReturnDefault())
	}
	return resp, err
}

func (h *HandlersV2) listImportJob(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	limitGetter, _ := anyReq.(LimitGetter)
	req := &milvuspb.ListImportTasksRequest{
		CollectionName: collectionGetter.GetCollectionName(),
		Limit:          int64(limitGetter.GetLimit()),
		DbName:         dbName,
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.ListImportTasks(reqCtx, req.(*milvuspb.ListImportTasksRequest))
	})
	if err == nil {
		returnData := []map[string]interface{}{}
		for _, job := range resp.(*milvuspb.ListImportTasksResponse).Tasks {
			taskDetail := map[string]interface{}{
				"taskID":          job.Id,
				"state":           job.State.String(),
				"dbName":          dbName,
				"collectionName":  collectionGetter.GetCollectionName(),
				"createTimestamp": strconv.FormatInt(job.CreateTs, 10),
			}
			for _, info := range job.Infos {
				switch info.Key {
				case "collection":
					taskDetail["collectionName"] = info.Value
				case "partition":
					taskDetail["partitionName"] = info.Value
				case "persist_cost":
					taskDetail["persistCost"] = info.Value
				case "progress_percent":
					taskDetail["progressPercent"] = info.Value
				case "failed_reason":
					if info.Value != "" {
						taskDetail[HTTPReturnIndexFailReason] = info.Value
					}
				}
			}
			returnData = append(returnData, taskDetail)
		}
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: returnData})
	}
	return resp, err
}

func (h *HandlersV2) createImportJob(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	collectionGetter, _ := anyReq.(requestutil.CollectionNameGetter)
	fileNamesGetter, _ := anyReq.(FileNamesGetter)
	req := &milvuspb.ImportRequest{
		CollectionName: collectionGetter.GetCollectionName(),
		DbName:         dbName,
		Files:          fileNamesGetter.GetFileNames(),
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.Import(reqCtx, req.(*milvuspb.ImportRequest))
	})
	if err == nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: resp.(*milvuspb.ImportResponse).Tasks})
	}
	return resp, err
}

func (h *HandlersV2) getImportJobProcess(ctx context.Context, c *gin.Context, anyReq any, dbName string) (interface{}, error) {
	taskIDGetter, _ := anyReq.(TaskIDGetter)
	req := &milvuspb.GetImportStateRequest{
		Task: taskIDGetter.GetTaskID(),
	}
	resp, err := wrapperProxy(ctx, c, req, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.GetImportState(reqCtx, req.(*milvuspb.GetImportStateRequest))
	})
	if err == nil {
		response := resp.(*milvuspb.GetImportStateResponse)
		returnData := map[string]interface{}{
			"taskID":          response.Id,
			"state":           response.State.String(),
			"dbName":          dbName,
			"createTimestamp": strconv.FormatInt(response.CreateTs, 10),
		}
		for _, info := range response.Infos {
			switch info.Key {
			case "collection":
				returnData["collectionName"] = info.Value
			case "partition":
				returnData["partitionName"] = info.Value
			case "persist_cost":
				returnData["persistCost"] = info.Value
			case "progress_percent":
				returnData["progressPercent"] = info.Value
			case "failed_reason":
				if info.Value != "" {
					returnData[HTTPReturnIndexFailReason] = info.Value
				}
			}
		}
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: returnData})
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
	descResp, err := wrapperProxy(ctx, c, descReq, h.checkAuth, false, func(reqCtx context.Context, req any) (interface{}, error) {
		return h.proxy.DescribeCollection(reqCtx, req.(*milvuspb.DescribeCollectionRequest))
	})
	if err != nil {
		return nil, err
	}
	response, _ := descResp.(*milvuspb.DescribeCollectionResponse)
	return response.Schema, nil
}
