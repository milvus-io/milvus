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
	"net/http"
	"strconv"

	"github.com/cockroachdb/errors"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metric"
	"github.com/milvus-io/milvus/pkg/util/requestutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var RestRequestInterceptorErr = errors.New("interceptor error placeholder")

func checkAuthorization(ctx context.Context, c *gin.Context, req interface{}) error {
	username, ok := c.Get(ContextUsername)
	if !ok || username.(string) == "" {
		HTTPReturn(c, http.StatusUnauthorized, gin.H{HTTPReturnCode: merr.Code(merr.ErrNeedAuthenticate), HTTPReturnMessage: merr.ErrNeedAuthenticate.Error()})
		return RestRequestInterceptorErr
	}
	_, authErr := proxy.PrivilegeInterceptor(ctx, req)
	if authErr != nil {
		HTTPReturn(c, http.StatusForbidden, gin.H{HTTPReturnCode: merr.Code(authErr), HTTPReturnMessage: authErr.Error()})
		return RestRequestInterceptorErr
	}

	return nil
}

type RestRequestInterceptor func(ctx context.Context, ginCtx *gin.Context, req any, handler func(reqCtx context.Context, req any) (any, error)) (any, error)

// HandlersV1 handles http requests
type HandlersV1 struct {
	proxy        types.ProxyComponent
	interceptors []RestRequestInterceptor
}

// NewHandlers creates a new HandlersV1
func NewHandlersV1(proxyComponent types.ProxyComponent) *HandlersV1 {
	h := &HandlersV1{
		proxy:        proxyComponent,
		interceptors: []RestRequestInterceptor{},
	}
	if proxy.Params.CommonCfg.AuthorizationEnabled.GetAsBool() {
		h.interceptors = append(h.interceptors,
			// authorization
			func(ctx context.Context, ginCtx *gin.Context, req any, handler func(reqCtx context.Context, req any) (any, error)) (any, error) {
				err := checkAuthorization(ctx, ginCtx, req)
				if err != nil {
					return nil, err
				}
				return handler(ctx, req)
			})
	}
	h.interceptors = append(h.interceptors,
		// check database
		func(ctx context.Context, ginCtx *gin.Context, req any, handler func(reqCtx context.Context, req any) (any, error)) (any, error) {
			value, ok := requestutil.GetDbNameFromRequest(req)
			if !ok {
				return handler(ctx, req)
			}
			err := h.checkDatabase(ctx, ginCtx, value.(string))
			if err != nil {
				return nil, err
			}
			return handler(ctx, req)
		})
	h.interceptors = append(h.interceptors,
		// trace request
		func(ctx context.Context, ginCtx *gin.Context, req any, handler func(reqCtx context.Context, req any) (any, error)) (any, error) {
			return proxy.TraceLogInterceptor(ctx, req, &grpc.UnaryServerInfo{
				FullMethod: ginCtx.Request.URL.Path,
			}, handler)
		})
	return h
}

func (h *HandlersV1) checkDatabase(ctx context.Context, c *gin.Context, dbName string) error {
	if dbName == DefaultDbName {
		return nil
	}
	if proxy.CheckDatabase(ctx, dbName) {
		return nil
	}
	response, err := h.proxy.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{})
	if err == nil {
		err = merr.Error(response.GetStatus())
	}
	if err != nil {
		HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return RestRequestInterceptorErr
	}
	for _, db := range response.DbNames {
		if db == dbName {
			return nil
		}
	}
	HTTPAbortReturn(c, http.StatusOK, gin.H{
		HTTPReturnCode:    merr.Code(merr.ErrDatabaseNotFound),
		HTTPReturnMessage: merr.ErrDatabaseNotFound.Error() + ", database: " + dbName,
	})
	return RestRequestInterceptorErr
}

func (h *HandlersV1) describeCollection(ctx context.Context, c *gin.Context, dbName string, collectionName string) (*schemapb.CollectionSchema, error) {
	collSchema, err := proxy.GetCachedCollectionSchema(ctx, dbName, collectionName)
	if err == nil {
		return collSchema.CollectionSchema, nil
	}
	req := milvuspb.DescribeCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	}
	response, err := h.proxy.DescribeCollection(ctx, &req)
	if err == nil {
		err = merr.Error(response.GetStatus())
	}
	if err != nil {
		HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return nil, err
	}
	primaryField, ok := getPrimaryField(response.Schema)
	if ok && primaryField.AutoID && !primaryField.AutoID {
		log.Warn("primary filed autoID VS schema autoID", zap.String("collectionName", collectionName), zap.Bool("primary Field", primaryField.AutoID), zap.Bool("schema", primaryField.AutoID))
		response.Schema.AutoID = EnableAutoID
	}
	return response.Schema, nil
}

func (h *HandlersV1) hasCollection(ctx context.Context, c *gin.Context, dbName string, collectionName string) (bool, error) {
	req := milvuspb.HasCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	}
	response, err := h.proxy.HasCollection(ctx, &req)
	if err == nil {
		err = merr.Error(response.GetStatus())
	}
	if err != nil {
		HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return false, err
	}
	return response.Value, nil
}

func (h *HandlersV1) RegisterRoutesToV1(router gin.IRouter) {
	router.GET(VectorCollectionsPath, h.listCollections)
	router.POST(VectorCollectionsCreatePath, h.createCollection)
	router.GET(VectorCollectionsDescribePath, h.getCollectionDetails)
	router.POST(VectorCollectionsDropPath, h.dropCollection)
	router.POST(VectorQueryPath, h.query)
	router.POST(VectorGetPath, h.get)
	router.POST(VectorDeletePath, h.delete)
	router.POST(VectorInsertPath, h.insert)
	router.POST(VectorUpsertPath, h.upsert)
	router.POST(VectorSearchPath, h.search)
}

func (h *HandlersV1) executeRestRequestInterceptor(ctx context.Context,
	ginCtx *gin.Context,
	req any, handler func(reqCtx context.Context, req any) (any, error),
) (any, error) {
	f := handler
	for i := len(h.interceptors) - 1; i >= 0; i-- {
		f = func(j int, handlerFunc func(reqCtx context.Context, req any) (any, error)) func(reqCtx context.Context, req any) (any, error) {
			return func(reqCtx context.Context, req any) (any, error) {
				return h.interceptors[j](reqCtx, ginCtx, req, handlerFunc)
			}
		}(i, f)
	}
	return f(ctx, req)
}

func (h *HandlersV1) listCollections(c *gin.Context) {
	dbName := c.DefaultQuery(HTTPDbName, DefaultDbName)
	req := &milvuspb.ShowCollectionsRequest{
		DbName: dbName,
	}
	c.Set(ContextRequest, req)
	username, _ := c.Get(ContextUsername)
	ctx := proxy.NewContextWithMetadata(c, username.(string), req.DbName)

	resp, err := h.executeRestRequestInterceptor(ctx, c, req, func(reqCtx context.Context, req any) (any, error) {
		return h.proxy.ShowCollections(reqCtx, req.(*milvuspb.ShowCollectionsRequest))
	})
	if err == RestRequestInterceptorErr {
		return
	}
	if err == nil {
		err = merr.Error(resp.(*milvuspb.ShowCollectionsResponse).GetStatus())
	}
	if err != nil {
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return
	}
	response := resp.(*milvuspb.ShowCollectionsResponse)
	var collections []string
	if response.CollectionNames != nil {
		collections = response.CollectionNames
	} else {
		collections = []string{}
	}
	HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: collections})
}

func (h *HandlersV1) createCollection(c *gin.Context) {
	httpReq := CreateCollectionReq{
		DbName:             DefaultDbName,
		MetricType:         metric.L2,
		PrimaryField:       DefaultPrimaryFieldName,
		VectorField:        DefaultVectorFieldName,
		EnableDynamicField: EnableDynamic,
	}
	if err := c.ShouldBindWith(&httpReq, binding.JSON); err != nil {
		log.Warn("high level restful api, the parameter of create collection is incorrect", zap.Any("request", httpReq), zap.Error(err))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
			HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", error: " + err.Error(),
		})
		return
	}
	if httpReq.CollectionName == "" || httpReq.Dimension == 0 {
		log.Warn("high level restful api, create collection require parameters: [collectionName, dimension], but miss", zap.Any("request", httpReq))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrMissingRequiredParameters),
			HTTPReturnMessage: merr.ErrMissingRequiredParameters.Error() + ", required parameters: [collectionName, dimension]",
		})
		return
	}
	req := &milvuspb.CreateCollectionRequest{
		DbName:           httpReq.DbName,
		CollectionName:   httpReq.CollectionName,
		ShardsNum:        ShardNumDefault,
		ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
	}
	c.Set(ContextRequest, req)

	schema, err := proto.Marshal(&schemapb.CollectionSchema{
		Name:        httpReq.CollectionName,
		Description: httpReq.Description,
		AutoID:      EnableAutoID,
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      common.StartOfUserFieldID,
				Name:         httpReq.PrimaryField,
				IsPrimaryKey: true,
				DataType:     schemapb.DataType_Int64,
				AutoID:       EnableAutoID,
			}, {
				FieldID:      common.StartOfUserFieldID + 1,
				Name:         httpReq.VectorField,
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
		EnableDynamicField: httpReq.EnableDynamicField,
	})
	if err != nil {
		log.Warn("high level restful api, marshal collection schema fail", zap.Any("request", httpReq), zap.Error(err))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrMarshalCollectionSchema),
			HTTPReturnMessage: merr.ErrMarshalCollectionSchema.Error() + ", error: " + err.Error(),
		})
		return
	}
	req.Schema = schema
	username, _ := c.Get(ContextUsername)
	ctx := proxy.NewContextWithMetadata(c, username.(string), req.DbName)
	response, err := h.executeRestRequestInterceptor(ctx, c, req, func(reqCtx context.Context, req any) (any, error) {
		return h.proxy.CreateCollection(reqCtx, req.(*milvuspb.CreateCollectionRequest))
	})
	if err == RestRequestInterceptorErr {
		return
	}
	if err == nil {
		err = merr.Error(response.(*commonpb.Status))
	}
	if err != nil {
		HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return
	}

	statusResponse, err := h.proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		DbName:         httpReq.DbName,
		CollectionName: httpReq.CollectionName,
		FieldName:      httpReq.VectorField,
		IndexName:      DefaultIndexName,
		ExtraParams:    []*commonpb.KeyValuePair{{Key: common.MetricTypeKey, Value: httpReq.MetricType}},
	})
	if err == nil {
		err = merr.Error(statusResponse)
	}
	if err != nil {
		HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return
	}
	statusResponse, err = h.proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         httpReq.DbName,
		CollectionName: httpReq.CollectionName,
	})
	if err == nil {
		err = merr.Error(statusResponse)
	}
	if err != nil {
		HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return
	}
	HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{}})
}

func (h *HandlersV1) getCollectionDetails(c *gin.Context) {
	collectionName := c.Query(HTTPCollectionName)
	if collectionName == "" {
		log.Warn("high level restful api, desc collection require parameter: [collectionName], but miss")
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrMissingRequiredParameters),
			HTTPReturnMessage: merr.ErrMissingRequiredParameters.Error() + ", required parameters: [collectionName]",
		})
		return
	}
	dbName := c.DefaultQuery(HTTPDbName, DefaultDbName)
	username, _ := c.Get(ContextUsername)
	ctx := proxy.NewContextWithMetadata(c, username.(string), dbName)

	req := &milvuspb.DescribeCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	}
	c.Set(ContextRequest, req)

	response, err := h.executeRestRequestInterceptor(ctx, c, req, func(reqCtx context.Context, req any) (any, error) {
		return h.proxy.DescribeCollection(reqCtx, req.(*milvuspb.DescribeCollectionRequest))
	})

	if err == nil {
		err = merr.Error(response.(*milvuspb.DescribeCollectionResponse).GetStatus())
	}
	if err != nil {
		HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return
	}
	coll := response.(*milvuspb.DescribeCollectionResponse)
	primaryField, ok := getPrimaryField(coll.Schema)
	if ok && primaryField.AutoID && !primaryField.AutoID {
		log.Warn("primary filed autoID VS schema autoID", zap.String("collectionName", collectionName), zap.Bool("primary Field", primaryField.AutoID), zap.Bool("schema", primaryField.AutoID))
		coll.Schema.AutoID = EnableAutoID
	}

	stateResp, err := h.proxy.GetLoadState(ctx, &milvuspb.GetLoadStateRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	collLoadState := ""
	if err == nil {
		err = merr.Error(stateResp.GetStatus())
	}
	if err != nil {
		log.Warn("get collection load state fail",
			zap.String("collection", collectionName),
			zap.Error(err),
		)
	} else {
		collLoadState = stateResp.State.String()
	}
	vectorField := ""
	for _, field := range coll.Schema.Fields {
		if typeutil.IsVectorType(field.DataType) {
			vectorField = field.Name
			break
		}
	}
	indexResp, err := h.proxy.DescribeIndex(ctx, &milvuspb.DescribeIndexRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldName:      vectorField,
	})
	if err == nil {
		err = merr.Error(indexResp.GetStatus())
	}
	var indexDesc []gin.H
	if err != nil {
		indexDesc = []gin.H{}
		log.Warn("get indexes description fail",
			zap.String("collection", collectionName),
			zap.String("vectorField", vectorField),
			zap.Error(err),
		)
	} else {
		indexDesc = printIndexes(indexResp.IndexDescriptions)
	}
	HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{
		HTTPCollectionName:    coll.CollectionName,
		HTTPReturnDescription: coll.Schema.Description,
		"fields":              printFields(coll.Schema.Fields),
		"indexes":             indexDesc,
		"load":                collLoadState,
		"shardsNum":           coll.ShardsNum,
		"enableDynamicField":  coll.Schema.EnableDynamicField,
	}})
}

func (h *HandlersV1) dropCollection(c *gin.Context) {
	httpReq := DropCollectionReq{
		DbName: DefaultDbName,
	}
	if err := c.ShouldBindWith(&httpReq, binding.JSON); err != nil {
		log.Warn("high level restful api, the parameter of drop collection is incorrect", zap.Any("request", httpReq), zap.Error(err))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
			HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", error: " + err.Error(),
		})
		return
	}
	if httpReq.CollectionName == "" {
		log.Warn("high level restful api, drop collection require parameter: [collectionName], but miss")
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrMissingRequiredParameters),
			HTTPReturnMessage: merr.ErrMissingRequiredParameters.Error() + ", required parameters: [collectionName]",
		})
		return
	}
	req := &milvuspb.DropCollectionRequest{
		DbName:         httpReq.DbName,
		CollectionName: httpReq.CollectionName,
	}
	c.Set(ContextRequest, req)
	username, _ := c.Get(ContextUsername)
	ctx := proxy.NewContextWithMetadata(c, username.(string), req.DbName)
	response, err := h.executeRestRequestInterceptor(ctx, c, req, func(reqCtx context.Context, req any) (any, error) {
		has, err := h.hasCollection(ctx, c, httpReq.DbName, httpReq.CollectionName)
		if err != nil {
			return nil, RestRequestInterceptorErr
		}
		if !has {
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrCollectionNotFound),
				HTTPReturnMessage: merr.ErrCollectionNotFound.Error() + ", database: " + httpReq.DbName + ", collection: " + httpReq.CollectionName,
			})
			return nil, RestRequestInterceptorErr
		}
		return h.proxy.DropCollection(reqCtx, req.(*milvuspb.DropCollectionRequest))
	})
	if err == RestRequestInterceptorErr {
		return
	}
	if err == nil {
		err = merr.Error(response.(*commonpb.Status))
	}
	if err != nil {
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
	} else {
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{}})
	}
}

func (h *HandlersV1) query(c *gin.Context) {
	httpReq := QueryReq{
		DbName:       DefaultDbName,
		Limit:        100,
		OutputFields: []string{DefaultOutputFields},
	}
	if err := c.ShouldBindWith(&httpReq, binding.JSON); err != nil {
		log.Warn("high level restful api, the parameter of query is incorrect", zap.Any("request", httpReq), zap.Error(err))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
			HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", error: " + err.Error(),
		})
		return
	}
	if httpReq.CollectionName == "" || httpReq.Filter == "" {
		log.Warn("high level restful api, query require parameter: [collectionName, filter], but miss")
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrMissingRequiredParameters),
			HTTPReturnMessage: merr.ErrMissingRequiredParameters.Error() + ", required parameters: [collectionName, filter]",
		})
		return
	}
	req := &milvuspb.QueryRequest{
		DbName:             httpReq.DbName,
		CollectionName:     httpReq.CollectionName,
		Expr:               httpReq.Filter,
		OutputFields:       httpReq.OutputFields,
		GuaranteeTimestamp: BoundedTimestamp,
		QueryParams:        []*commonpb.KeyValuePair{},
	}
	c.Set(ContextRequest, req)
	if httpReq.Offset > 0 {
		req.QueryParams = append(req.QueryParams, &commonpb.KeyValuePair{Key: ParamOffset, Value: strconv.FormatInt(int64(httpReq.Offset), 10)})
	}
	if httpReq.Limit > 0 {
		req.QueryParams = append(req.QueryParams, &commonpb.KeyValuePair{Key: ParamLimit, Value: strconv.FormatInt(int64(httpReq.Limit), 10)})
	}
	username, _ := c.Get(ContextUsername)
	ctx := proxy.NewContextWithMetadata(c, username.(string), req.DbName)
	response, err := h.executeRestRequestInterceptor(ctx, c, req, func(reqCtx context.Context, req any) (any, error) {
		if _, err := CheckLimiter(ctx, &req, h.proxy); err != nil {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(err),
				HTTPReturnMessage: err.Error() + ", error: " + err.Error(),
			})
			return nil, err
		}
		return h.proxy.Query(reqCtx, req.(*milvuspb.QueryRequest))
	})
	if err == RestRequestInterceptorErr {
		return
	}
	if err == nil {
		err = merr.Error(response.(*milvuspb.QueryResults).GetStatus())
	}
	if err != nil {
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
	} else {
		queryResp := response.(*milvuspb.QueryResults)
		allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
		outputData, err := buildQueryResp(int64(0), queryResp.OutputFields, queryResp.FieldsData, nil, nil, allowJS, 0)
		if err != nil {
			log.Warn("high level restful api, fail to deal with query result", zap.Any("response", response), zap.Error(err))
			HTTPReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrInvalidSearchResult),
				HTTPReturnMessage: merr.ErrInvalidSearchResult.Error() + ", error: " + err.Error(),
			})
		} else {
			HTTPReturnStream(c, http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: outputData})
		}
	}
}

func (h *HandlersV1) get(c *gin.Context) {
	httpReq := GetReq{
		DbName:       DefaultDbName,
		OutputFields: []string{DefaultOutputFields},
	}
	if err := c.ShouldBindBodyWith(&httpReq, binding.JSON); err != nil {
		log.Warn("high level restful api, the parameter of get is incorrect", zap.Any("request", httpReq), zap.Error(err))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
			HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", error: " + err.Error(),
		})
		return
	}
	if httpReq.CollectionName == "" || httpReq.ID == nil {
		log.Warn("high level restful api, get require parameter: [collectionName, id], but miss")
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrMissingRequiredParameters),
			HTTPReturnMessage: merr.ErrMissingRequiredParameters.Error() + ", required parameters: [collectionName, id]",
		})
		return
	}
	req := &milvuspb.QueryRequest{
		DbName:             httpReq.DbName,
		CollectionName:     httpReq.CollectionName,
		OutputFields:       httpReq.OutputFields,
		GuaranteeTimestamp: BoundedTimestamp,
	}
	c.Set(ContextRequest, req)
	username, _ := c.Get(ContextUsername)
	ctx := proxy.NewContextWithMetadata(c, username.(string), req.DbName)
	response, err := h.executeRestRequestInterceptor(ctx, c, req, func(reqCtx context.Context, req any) (any, error) {
		collSchema, err := h.describeCollection(ctx, c, httpReq.DbName, httpReq.CollectionName)
		if err != nil || collSchema == nil {
			return nil, RestRequestInterceptorErr
		}
		body, _ := c.Get(gin.BodyBytesKey)
		filter, err := checkGetPrimaryKey(collSchema, gjson.Get(string(body.([]byte)), DefaultPrimaryFieldName))
		if err != nil {
			HTTPReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrCheckPrimaryKey),
				HTTPReturnMessage: merr.ErrCheckPrimaryKey.Error() + ", error: " + err.Error(),
			})
			return nil, RestRequestInterceptorErr
		}
		queryReq := req.(*milvuspb.QueryRequest)
		if _, err := CheckLimiter(ctx, &req, h.proxy); err != nil {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(err),
				HTTPReturnMessage: err.Error() + ", error: " + err.Error(),
			})
			return nil, err
		}
		queryReq.Expr = filter
		return h.proxy.Query(reqCtx, queryReq)
	})
	if err == RestRequestInterceptorErr {
		return
	}
	if err == nil {
		err = merr.Error(response.(*milvuspb.QueryResults).GetStatus())
	}
	if err != nil {
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
	} else {
		queryResp := response.(*milvuspb.QueryResults)
		allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
		outputData, err := buildQueryResp(int64(0), queryResp.OutputFields, queryResp.FieldsData, nil, nil, allowJS, 0)
		if err != nil {
			log.Warn("high level restful api, fail to deal with get result", zap.Any("response", response), zap.Error(err))
			HTTPReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrInvalidSearchResult),
				HTTPReturnMessage: merr.ErrInvalidSearchResult.Error() + ", error: " + err.Error(),
			})
		} else {
			HTTPReturnStream(c, http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: outputData})
		}
	}
}

func (h *HandlersV1) delete(c *gin.Context) {
	httpReq := DeleteReq{
		DbName: DefaultDbName,
	}
	if err := c.ShouldBindBodyWith(&httpReq, binding.JSON); err != nil {
		log.Warn("high level restful api, the parameter of delete is incorrect", zap.Any("request", httpReq), zap.Error(err))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
			HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", error: " + err.Error(),
		})
		return
	}
	if httpReq.CollectionName == "" || (httpReq.ID == nil && httpReq.Filter == "") {
		log.Warn("high level restful api, delete require parameter: [collectionName, id/filter], but miss")
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrMissingRequiredParameters),
			HTTPReturnMessage: merr.ErrMissingRequiredParameters.Error() + ", required parameters: [collectionName, id/filter]",
		})
		return
	}
	req := &milvuspb.DeleteRequest{
		DbName:         httpReq.DbName,
		CollectionName: httpReq.CollectionName,
	}
	c.Set(ContextRequest, req)
	username, _ := c.Get(ContextUsername)
	ctx := proxy.NewContextWithMetadata(c, username.(string), req.DbName)
	response, err := h.executeRestRequestInterceptor(ctx, c, req, func(reqCtx context.Context, req any) (any, error) {
		collSchema, err := h.describeCollection(ctx, c, httpReq.DbName, httpReq.CollectionName)
		if err != nil || collSchema == nil {
			return nil, RestRequestInterceptorErr
		}
		deleteReq := req.(*milvuspb.DeleteRequest)
		deleteReq.Expr = httpReq.Filter
		if deleteReq.Expr == "" {
			body, _ := c.Get(gin.BodyBytesKey)
			filter, err := checkGetPrimaryKey(collSchema, gjson.Get(string(body.([]byte)), DefaultPrimaryFieldName))
			if err != nil {
				HTTPReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrCheckPrimaryKey),
					HTTPReturnMessage: merr.ErrCheckPrimaryKey.Error() + ", error: " + err.Error(),
				})
				return nil, RestRequestInterceptorErr
			}
			deleteReq.Expr = filter
		}
		if _, err := CheckLimiter(ctx, &req, h.proxy); err != nil {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(err),
				HTTPReturnMessage: err.Error() + ", error: " + err.Error(),
			})
			return nil, err
		}
		return h.proxy.Delete(ctx, deleteReq)
	})
	if err == RestRequestInterceptorErr {
		return
	}
	if err == nil {
		err = merr.Error(response.(*milvuspb.MutationResult).GetStatus())
	}
	if err != nil {
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
	} else {
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{}})
	}
}

func (h *HandlersV1) insert(c *gin.Context) {
	httpReq := InsertReq{
		DbName: DefaultDbName,
	}
	if err := c.ShouldBindBodyWith(&httpReq, binding.JSON); err != nil {
		singleInsertReq := SingleInsertReq{
			DbName: DefaultDbName,
		}
		if err = c.ShouldBindBodyWith(&singleInsertReq, binding.JSON); err != nil {
			log.Warn("high level restful api, the parameter of insert is incorrect", zap.Any("request", httpReq), zap.Error(err))
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
				HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", error: " + err.Error(),
			})
			return
		}
		httpReq.DbName = singleInsertReq.DbName
		httpReq.CollectionName = singleInsertReq.CollectionName
		httpReq.Data = []map[string]interface{}{singleInsertReq.Data}
	}
	if httpReq.CollectionName == "" || httpReq.Data == nil {
		log.Warn("high level restful api, insert require parameter: [collectionName, data], but miss")
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrMissingRequiredParameters),
			HTTPReturnMessage: merr.ErrMissingRequiredParameters.Error() + ", required parameters: [collectionName, data]",
		})
		return
	}
	req := &milvuspb.InsertRequest{
		DbName:         httpReq.DbName,
		CollectionName: httpReq.CollectionName,
		NumRows:        uint32(len(httpReq.Data)),
	}
	c.Set(ContextRequest, req)
	username, _ := c.Get(ContextUsername)
	ctx := proxy.NewContextWithMetadata(c, username.(string), req.DbName)
	response, err := h.executeRestRequestInterceptor(ctx, c, req, func(reqCtx context.Context, req any) (any, error) {
		collSchema, err := h.describeCollection(ctx, c, httpReq.DbName, httpReq.CollectionName)
		if err != nil || collSchema == nil {
			return nil, RestRequestInterceptorErr
		}
		body, _ := c.Get(gin.BodyBytesKey)
		err, httpReq.Data, _ = checkAndSetData(string(body.([]byte)), collSchema)
		if err != nil {
			log.Warn("high level restful api, fail to deal with insert data", zap.Any("body", body), zap.Error(err))
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrInvalidInsertData),
				HTTPReturnMessage: merr.ErrInvalidInsertData.Error() + ", error: " + err.Error(),
			})
			return nil, RestRequestInterceptorErr
		}
		insertReq := req.(*milvuspb.InsertRequest)
		insertReq.FieldsData, err = anyToColumns(httpReq.Data, nil, collSchema, true)
		if err != nil {
			log.Warn("high level restful api, fail to deal with insert data", zap.Any("data", httpReq.Data), zap.Error(err))
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrInvalidInsertData),
				HTTPReturnMessage: merr.ErrInvalidInsertData.Error() + ", error: " + err.Error(),
			})
			return nil, RestRequestInterceptorErr
		}
		if _, err := CheckLimiter(ctx, &req, h.proxy); err != nil {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(err),
				HTTPReturnMessage: err.Error() + ", error: " + err.Error(),
			})
			return nil, err
		}
		return h.proxy.Insert(ctx, insertReq)
	})
	if err == RestRequestInterceptorErr {
		return
	}
	if err == nil {
		err = merr.Error(response.(*milvuspb.MutationResult).GetStatus())
	}
	if err != nil {
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
	} else {
		insertResp := response.(*milvuspb.MutationResult)
		switch insertResp.IDs.GetIdField().(type) {
		case *schemapb.IDs_IntId:
			allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
			if allowJS {
				HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{"insertCount": insertResp.InsertCnt, "insertIds": insertResp.IDs.IdField.(*schemapb.IDs_IntId).IntId.Data}})
			} else {
				HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{"insertCount": insertResp.InsertCnt, "insertIds": formatInt64(insertResp.IDs.IdField.(*schemapb.IDs_IntId).IntId.Data)}})
			}
		case *schemapb.IDs_StrId:
			HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{"insertCount": insertResp.InsertCnt, "insertIds": insertResp.IDs.IdField.(*schemapb.IDs_StrId).StrId.Data}})
		default:
			HTTPReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrCheckPrimaryKey),
				HTTPReturnMessage: merr.ErrCheckPrimaryKey.Error() + ", error: unsupported primary key data type",
			})
		}
	}
}

func (h *HandlersV1) upsert(c *gin.Context) {
	httpReq := UpsertReq{
		DbName: DefaultDbName,
	}
	if err := c.ShouldBindBodyWith(&httpReq, binding.JSON); err != nil {
		singleUpsertReq := SingleUpsertReq{
			DbName: DefaultDbName,
		}
		if err = c.ShouldBindBodyWith(&singleUpsertReq, binding.JSON); err != nil {
			log.Warn("high level restful api, the parameter of upsert is incorrect", zap.Any("request", httpReq), zap.Error(err))
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
				HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", error: " + err.Error(),
			})
			return
		}
		httpReq.DbName = singleUpsertReq.DbName
		httpReq.CollectionName = singleUpsertReq.CollectionName
		httpReq.Data = []map[string]interface{}{singleUpsertReq.Data}
	}
	if httpReq.CollectionName == "" || httpReq.Data == nil {
		log.Warn("high level restful api, upsert require parameter: [collectionName, data], but miss")
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrMissingRequiredParameters),
			HTTPReturnMessage: merr.ErrMissingRequiredParameters.Error() + ", required parameters: [collectionName, data]",
		})
		return
	}
	req := &milvuspb.UpsertRequest{
		DbName:         httpReq.DbName,
		CollectionName: httpReq.CollectionName,
		NumRows:        uint32(len(httpReq.Data)),
	}
	c.Set(ContextRequest, req)
	username, _ := c.Get(ContextUsername)
	ctx := proxy.NewContextWithMetadata(c, username.(string), req.DbName)
	response, err := h.executeRestRequestInterceptor(ctx, c, req, func(reqCtx context.Context, req any) (any, error) {
		collSchema, err := h.describeCollection(ctx, c, httpReq.DbName, httpReq.CollectionName)
		if err != nil || collSchema == nil {
			return nil, RestRequestInterceptorErr
		}
		for _, fieldSchema := range collSchema.Fields {
			if fieldSchema.IsPrimaryKey && fieldSchema.AutoID {
				err := merr.WrapErrParameterInvalid("autoID: false", "autoID: true", "cannot upsert an autoID collection")
				HTTPAbortReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
				return nil, RestRequestInterceptorErr
			}
		}
		body, _ := c.Get(gin.BodyBytesKey)
		err, httpReq.Data, _ = checkAndSetData(string(body.([]byte)), collSchema)
		if err != nil {
			log.Warn("high level restful api, fail to deal with upsert data", zap.Any("body", body), zap.Error(err))
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrInvalidInsertData),
				HTTPReturnMessage: merr.ErrInvalidInsertData.Error() + ", error: " + err.Error(),
			})
			return nil, RestRequestInterceptorErr
		}
		upsertReq := req.(*milvuspb.UpsertRequest)
		upsertReq.FieldsData, err = anyToColumns(httpReq.Data, nil, collSchema, false)
		if err != nil {
			log.Warn("high level restful api, fail to deal with upsert data", zap.Any("data", httpReq.Data), zap.Error(err))
			HTTPAbortReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrInvalidInsertData),
				HTTPReturnMessage: merr.ErrInvalidInsertData.Error() + ", error: " + err.Error(),
			})
			return nil, RestRequestInterceptorErr
		}
		if _, err := CheckLimiter(ctx, &req, h.proxy); err != nil {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(err),
				HTTPReturnMessage: err.Error() + ", error: " + err.Error(),
			})
			return nil, err
		}
		return h.proxy.Upsert(ctx, upsertReq)
	})
	if err == RestRequestInterceptorErr {
		return
	}
	if err == nil {
		err = merr.Error(response.(*milvuspb.MutationResult).GetStatus())
	}
	if err != nil {
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
	} else {
		upsertResp := response.(*milvuspb.MutationResult)
		switch upsertResp.IDs.GetIdField().(type) {
		case *schemapb.IDs_IntId:
			allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
			if allowJS {
				HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{"upsertCount": upsertResp.UpsertCnt, "upsertIds": upsertResp.IDs.IdField.(*schemapb.IDs_IntId).IntId.Data}})
			} else {
				HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{"upsertCount": upsertResp.UpsertCnt, "upsertIds": formatInt64(upsertResp.IDs.IdField.(*schemapb.IDs_IntId).IntId.Data)}})
			}
		case *schemapb.IDs_StrId:
			HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{"upsertCount": upsertResp.UpsertCnt, "upsertIds": upsertResp.IDs.IdField.(*schemapb.IDs_StrId).StrId.Data}})
		default:
			HTTPReturn(c, http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrCheckPrimaryKey),
				HTTPReturnMessage: merr.ErrCheckPrimaryKey.Error() + ", error: unsupported primary key data type",
			})
		}
	}
}

func (h *HandlersV1) search(c *gin.Context) {
	httpReq := SearchReq{
		DbName: DefaultDbName,
		Limit:  100,
	}
	if err := c.ShouldBindWith(&httpReq, binding.JSON); err != nil {
		log.Warn("high level restful api, the parameter of search is incorrect", zap.Any("request", httpReq), zap.Error(err))
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
			HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", error: " + err.Error(),
		})
		return
	}
	if httpReq.CollectionName == "" || httpReq.Vector == nil {
		log.Warn("high level restful api, search require parameter: [collectionName, vector], but miss")
		HTTPAbortReturn(c, http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrMissingRequiredParameters),
			HTTPReturnMessage: merr.ErrMissingRequiredParameters.Error() + ", required parameters: [collectionName, vector]",
		})
		return
	}
	req := &milvuspb.SearchRequest{
		DbName:             httpReq.DbName,
		CollectionName:     httpReq.CollectionName,
		Dsl:                httpReq.Filter,
		PlaceholderGroup:   vectors2PlaceholderGroupBytes([][]float32{httpReq.Vector}),
		DslType:            commonpb.DslType_BoolExprV1,
		OutputFields:       httpReq.OutputFields,
		GuaranteeTimestamp: BoundedTimestamp,
		Nq:                 int64(1),
	}
	c.Set(ContextRequest, req)

	params := map[string]interface{}{ // auto generated mapping
		"level": int(commonpb.ConsistencyLevel_Bounded),
	}
	if httpReq.Params != nil {
		radius, radiusOk := httpReq.Params[ParamRadius]
		rangeFilter, rangeFilterOk := httpReq.Params[ParamRangeFilter]
		if rangeFilterOk {
			if !radiusOk {
				log.Warn("high level restful api, search params invalid, because only " + ParamRangeFilter)
				HTTPAbortReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
					HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", error: invalid search params",
				})
				return
			}
			params[ParamRangeFilter] = rangeFilter
		}
		if radiusOk {
			params[ParamRadius] = radius
		}
	}
	bs, _ := json.Marshal(params)
	req.SearchParams = []*commonpb.KeyValuePair{
		{Key: common.TopKKey, Value: strconv.FormatInt(int64(httpReq.Limit), 10)},
		{Key: Params, Value: string(bs)},
		{Key: ParamRoundDecimal, Value: "-1"},
		{Key: ParamOffset, Value: strconv.FormatInt(int64(httpReq.Offset), 10)},
	}

	username, _ := c.Get(ContextUsername)
	ctx := proxy.NewContextWithMetadata(c, username.(string), req.DbName)
	response, err := h.executeRestRequestInterceptor(ctx, c, req, func(reqCtx context.Context, req any) (any, error) {
		if _, err := CheckLimiter(ctx, &req, h.proxy); err != nil {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(err),
				HTTPReturnMessage: err.Error() + ", error: " + err.Error(),
			})
			return nil, err
		}
		return h.proxy.Search(ctx, req.(*milvuspb.SearchRequest))
	})
	if err == RestRequestInterceptorErr {
		return
	}
	if err == nil {
		err = merr.Error(response.(*milvuspb.SearchResults).GetStatus())
	}
	if err != nil {
		HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
	} else {
		searchResp := response.(*milvuspb.SearchResults)
		if searchResp.Results.TopK == int64(0) {
			HTTPReturn(c, http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: []interface{}{}})
		} else {
			allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
			outputData, err := buildQueryResp(searchResp.Results.TopK, searchResp.Results.OutputFields, searchResp.Results.FieldsData, searchResp.Results.Ids, searchResp.Results.Scores, allowJS, 0)
			if err != nil {
				log.Warn("high level restful api, fail to deal with search result", zap.Any("result", searchResp.Results), zap.Error(err))
				HTTPReturn(c, http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrInvalidSearchResult),
					HTTPReturnMessage: merr.ErrInvalidSearchResult.Error() + ", error: " + err.Error(),
				})
			} else {
				HTTPReturnStream(c, http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: outputData})
			}
		}
	}
}
