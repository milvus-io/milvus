package httpserver

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/golang/protobuf/proto"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

func checkAuthorization(ctx context.Context, c *gin.Context, req interface{}) error {
	if proxy.Params.CommonCfg.AuthorizationEnabled.GetAsBool() {
		username, ok := c.Get(ContextUsername)
		if !ok || username.(string) == "" {
			c.JSON(http.StatusUnauthorized, gin.H{HTTPReturnCode: merr.Code(merr.ErrNeedAuthenticate), HTTPReturnMessage: merr.ErrNeedAuthenticate.Error()})
			return merr.ErrNeedAuthenticate
		}
		_, authErr := proxy.PrivilegeInterceptor(ctx, req)
		if authErr != nil {
			c.JSON(http.StatusForbidden, gin.H{HTTPReturnCode: merr.Code(authErr), HTTPReturnMessage: authErr.Error()})
			return authErr
		}
	}
	return nil
}

func (h *Handlers) checkDatabase(ctx context.Context, c *gin.Context, dbName string) bool {
	if dbName == DefaultDbName {
		return true
	}
	response, err := h.proxy.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{})
	if err == nil {
		err = merr.Error(response.GetStatus())
	}
	if err != nil {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return false
	}
	for _, db := range response.DbNames {
		if db == dbName {
			return true
		}
	}
	c.AbortWithStatusJSON(http.StatusOK, gin.H{
		HTTPReturnCode:    merr.Code(merr.ErrDatabaseNotFound),
		HTTPReturnMessage: merr.ErrDatabaseNotFound.Error() + ", database: " + dbName,
	})
	return false
}

func (h *Handlers) describeCollection(ctx context.Context, c *gin.Context, dbName string, collectionName string, needAuth bool) (*milvuspb.DescribeCollectionResponse, error) {
	req := milvuspb.DescribeCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	}
	if needAuth {
		if err := checkAuthorization(ctx, c, &req); err != nil {
			return nil, err
		}
	}
	response, err := h.proxy.DescribeCollection(ctx, &req)
	if err == nil {
		err = merr.Error(response.GetStatus())
	}
	if err != nil {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return nil, err
	}
	primaryField, ok := getPrimaryField(response.Schema)
	if ok && primaryField.AutoID && !response.Schema.AutoID {
		log.Warn("primary filed autoID VS schema autoID", zap.String("collectionName", collectionName), zap.Bool("primary Field", primaryField.AutoID), zap.Bool("schema", response.Schema.AutoID))
		response.Schema.AutoID = EnableAutoID
	}
	return response, nil
}

func (h *Handlers) hasCollection(ctx context.Context, c *gin.Context, dbName string, collectionName string) (bool, error) {
	req := milvuspb.HasCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	}
	response, err := h.proxy.HasCollection(ctx, &req)
	if err == nil {
		err = merr.Error(response.GetStatus())
	}
	if err != nil {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return false, err
	}
	return response.Value, nil
}

func (h *Handlers) RegisterRoutesToV1(router gin.IRouter) {
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

func (h *Handlers) listCollections(c *gin.Context) {
	dbName := c.DefaultQuery(HTTPDbName, DefaultDbName)
	req := milvuspb.ShowCollectionsRequest{
		DbName: dbName,
	}
	username, _ := c.Get(ContextUsername)
	ctx := proxy.NewContextWithMetadata(c, username.(string), req.DbName)
	if err := checkAuthorization(ctx, c, &req); err != nil {
		return
	}
	if !h.checkDatabase(ctx, c, dbName) {
		return
	}
	response, err := h.proxy.ShowCollections(ctx, &req)
	if err == nil {
		err = merr.Error(response.GetStatus())
	}
	if err != nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return
	}
	var collections []string
	if response.CollectionNames != nil {
		collections = response.CollectionNames
	} else {
		collections = []string{}
	}
	c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: collections})
}

func (h *Handlers) createCollection(c *gin.Context) {
	httpReq := CreateCollectionReq{
		DbName:       DefaultDbName,
		MetricType:   DefaultMetricType,
		PrimaryField: DefaultPrimaryFieldName,
		VectorField:  DefaultVectorFieldName,
	}
	if err := c.ShouldBindBodyWith(&httpReq, binding.JSON); err != nil {
		log.Warn("high level restful api, the parameter of create collection is incorrect", zap.Any("request", httpReq), zap.Error(err))
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
			HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", error: " + err.Error(),
		})
		return
	}
	if httpReq.CollectionName == "" || httpReq.Dimension == 0 {
		log.Warn("high level restful api, create collection require parameters: [collectionName, dimension], but miss", zap.Any("request", httpReq))
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrMissingRequiredParameters),
			HTTPReturnMessage: merr.ErrMissingRequiredParameters.Error() + ", required parameters: [collectionName, dimension]",
		})
		return
	}
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
		EnableDynamicField: EnableDynamic,
	})
	if err != nil {
		log.Warn("high level restful api, marshal collection schema fail", zap.Any("request", httpReq), zap.Error(err))
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrMarshalCollectionSchema),
			HTTPReturnMessage: merr.ErrMarshalCollectionSchema.Error() + ", error: " + err.Error(),
		})
		return
	}
	req := milvuspb.CreateCollectionRequest{
		DbName:           httpReq.DbName,
		CollectionName:   httpReq.CollectionName,
		Schema:           schema,
		ShardsNum:        ShardNumDefault,
		ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
	}
	username, _ := c.Get(ContextUsername)
	ctx := proxy.NewContextWithMetadata(c, username.(string), req.DbName)
	if err := checkAuthorization(ctx, c, &req); err != nil {
		return
	}
	if !h.checkDatabase(ctx, c, req.DbName) {
		return
	}
	response, err := h.proxy.CreateCollection(ctx, &req)
	if err == nil {
		err = merr.Error(response)
	}
	if err != nil {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return
	}

	response, err = h.proxy.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		DbName:         httpReq.DbName,
		CollectionName: httpReq.CollectionName,
		FieldName:      httpReq.VectorField,
		IndexName:      DefaultIndexName,
		ExtraParams:    []*commonpb.KeyValuePair{{Key: common.MetricTypeKey, Value: httpReq.MetricType}},
	})
	if err == nil {
		err = merr.Error(response)
	}
	if err != nil {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return
	}
	response, err = h.proxy.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         httpReq.DbName,
		CollectionName: httpReq.CollectionName,
	})
	if err == nil {
		err = merr.Error(response)
	}
	if err != nil {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{}})
}

func (h *Handlers) getCollectionDetails(c *gin.Context) {
	collectionName := c.Query(HTTPCollectionName)
	if collectionName == "" {
		log.Warn("high level restful api, desc collection require parameter: [collectionName], but miss")
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrMissingRequiredParameters),
			HTTPReturnMessage: merr.ErrMissingRequiredParameters.Error() + ", required parameters: [collectionName]",
		})
		return
	}
	dbName := c.DefaultQuery(HTTPDbName, DefaultDbName)
	username, _ := c.Get(ContextUsername)
	ctx := proxy.NewContextWithMetadata(c, username.(string), dbName)
	if !h.checkDatabase(ctx, c, dbName) {
		return
	}
	coll, err := h.describeCollection(ctx, c, dbName, collectionName, true)
	if err != nil {
		return
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
		if field.DataType == schemapb.DataType_BinaryVector || field.DataType == schemapb.DataType_FloatVector {
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
	c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{
		HTTPCollectionName:    coll.CollectionName,
		HTTPReturnDescription: coll.Schema.Description,
		"fields":              printFields(coll.Schema.Fields),
		"indexes":             indexDesc,
		"load":                collLoadState,
		"shardsNum":           coll.ShardsNum,
		"enableDynamic":       coll.Schema.EnableDynamicField,
	}})
}

func (h *Handlers) dropCollection(c *gin.Context) {
	httpReq := DropCollectionReq{
		DbName: DefaultDbName,
	}
	if err := c.ShouldBindBodyWith(&httpReq, binding.JSON); err != nil {
		log.Warn("high level restful api, the parameter of drop collection is incorrect", zap.Any("request", httpReq), zap.Error(err))
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
			HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", error: " + err.Error(),
		})
		return
	}
	if httpReq.CollectionName == "" {
		log.Warn("high level restful api, drop collection require parameter: [collectionName], but miss")
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrMissingRequiredParameters),
			HTTPReturnMessage: merr.ErrMissingRequiredParameters.Error() + ", required parameters: [collectionName]",
		})
		return
	}
	req := milvuspb.DropCollectionRequest{
		DbName:         httpReq.DbName,
		CollectionName: httpReq.CollectionName,
	}
	username, _ := c.Get(ContextUsername)
	ctx := proxy.NewContextWithMetadata(c, username.(string), req.DbName)
	if err := checkAuthorization(ctx, c, &req); err != nil {
		return
	}
	if !h.checkDatabase(ctx, c, req.DbName) {
		return
	}
	has, err := h.hasCollection(ctx, c, httpReq.DbName, httpReq.CollectionName)
	if err != nil {
		return
	}
	if !has {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrCollectionNotFound),
			HTTPReturnMessage: merr.ErrCollectionNotFound.Error() + ", database: " + httpReq.DbName + ", collection: " + httpReq.CollectionName,
		})
		return
	}
	response, err := h.proxy.DropCollection(ctx, &req)
	if err == nil {
		err = merr.Error(response)
	}
	if err != nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
	} else {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{}})
	}
}

func (h *Handlers) query(c *gin.Context) {
	httpReq := QueryReq{
		DbName:       DefaultDbName,
		Limit:        100,
		OutputFields: []string{DefaultOutputFields},
	}
	if err := c.ShouldBindBodyWith(&httpReq, binding.JSON); err != nil {
		log.Warn("high level restful api, the parameter of query is incorrect", zap.Any("request", httpReq), zap.Error(err))
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
			HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", error: " + err.Error(),
		})
		return
	}
	if httpReq.CollectionName == "" || httpReq.Filter == "" {
		log.Warn("high level restful api, query require parameter: [collectionName, filter], but miss")
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrMissingRequiredParameters),
			HTTPReturnMessage: merr.ErrMissingRequiredParameters.Error() + ", required parameters: [collectionName, filter]",
		})
		return
	}
	req := milvuspb.QueryRequest{
		DbName:             httpReq.DbName,
		CollectionName:     httpReq.CollectionName,
		Expr:               httpReq.Filter,
		OutputFields:       httpReq.OutputFields,
		GuaranteeTimestamp: BoundedTimestamp,
		QueryParams:        []*commonpb.KeyValuePair{},
	}
	if httpReq.Offset > 0 {
		req.QueryParams = append(req.QueryParams, &commonpb.KeyValuePair{Key: ParamOffset, Value: strconv.FormatInt(int64(httpReq.Offset), 10)})
	}
	if httpReq.Limit > 0 {
		req.QueryParams = append(req.QueryParams, &commonpb.KeyValuePair{Key: ParamLimit, Value: strconv.FormatInt(int64(httpReq.Limit), 10)})
	}
	username, _ := c.Get(ContextUsername)
	ctx := proxy.NewContextWithMetadata(c, username.(string), req.DbName)
	if err := checkAuthorization(ctx, c, &req); err != nil {
		return
	}
	if !h.checkDatabase(ctx, c, req.DbName) {
		return
	}
	response, err := h.proxy.Query(ctx, &req)
	if err == nil {
		err = merr.Error(response.GetStatus())
	}
	if err != nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
	} else {
		allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
		outputData, err := buildQueryResp(int64(0), response.OutputFields, response.FieldsData, nil, nil, allowJS)
		if err != nil {
			log.Warn("high level restful api, fail to deal with query result", zap.Any("response", response), zap.Error(err))
			c.JSON(http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrInvalidSearchResult),
				HTTPReturnMessage: merr.ErrInvalidSearchResult.Error() + ", error: " + err.Error(),
			})
		} else {
			c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: outputData})
		}
	}
}

func (h *Handlers) get(c *gin.Context) {
	httpReq := GetReq{
		DbName:       DefaultDbName,
		OutputFields: []string{DefaultOutputFields},
	}
	if err := c.ShouldBindBodyWith(&httpReq, binding.JSON); err != nil {
		log.Warn("high level restful api, the parameter of get is incorrect", zap.Any("request", httpReq), zap.Error(err))
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
			HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", error: " + err.Error(),
		})
		return
	}
	if httpReq.CollectionName == "" || httpReq.ID == nil {
		log.Warn("high level restful api, get require parameter: [collectionName, id], but miss")
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrMissingRequiredParameters),
			HTTPReturnMessage: merr.ErrMissingRequiredParameters.Error() + ", required parameters: [collectionName, id]",
		})
		return
	}
	req := milvuspb.QueryRequest{
		DbName:             httpReq.DbName,
		CollectionName:     httpReq.CollectionName,
		OutputFields:       httpReq.OutputFields,
		GuaranteeTimestamp: BoundedTimestamp,
	}
	username, _ := c.Get(ContextUsername)
	ctx := proxy.NewContextWithMetadata(c, username.(string), req.DbName)
	if err := checkAuthorization(ctx, c, &req); err != nil {
		return
	}
	if !h.checkDatabase(ctx, c, req.DbName) {
		return
	}
	coll, err := h.describeCollection(ctx, c, httpReq.DbName, httpReq.CollectionName, false)
	if err != nil || coll == nil {
		return
	}
	body, _ := c.Get(gin.BodyBytesKey)
	filter, err := checkGetPrimaryKey(coll.Schema, gjson.Get(string(body.([]byte)), DefaultPrimaryFieldName))
	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrCheckPrimaryKey),
			HTTPReturnMessage: merr.ErrCheckPrimaryKey.Error() + ", error: " + err.Error(),
		})
		return
	}
	req.Expr = filter
	response, err := h.proxy.Query(ctx, &req)
	if err == nil {
		err = merr.Error(response.GetStatus())
	}
	if err != nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
	} else {
		allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
		outputData, err := buildQueryResp(int64(0), response.OutputFields, response.FieldsData, nil, nil, allowJS)
		if err != nil {
			log.Warn("high level restful api, fail to deal with get result", zap.Any("response", response), zap.Error(err))
			c.JSON(http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrInvalidSearchResult),
				HTTPReturnMessage: merr.ErrInvalidSearchResult.Error() + ", error: " + err.Error(),
			})
		} else {
			c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: outputData})
		}
	}
}

func (h *Handlers) delete(c *gin.Context) {
	httpReq := DeleteReq{
		DbName: DefaultDbName,
	}
	if err := c.ShouldBindBodyWith(&httpReq, binding.JSON); err != nil {
		log.Warn("high level restful api, the parameter of delete is incorrect", zap.Any("request", httpReq), zap.Error(err))
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
			HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", error: " + err.Error(),
		})
		return
	}
	if httpReq.CollectionName == "" || (httpReq.ID == nil && httpReq.Filter == "") {
		log.Warn("high level restful api, delete require parameter: [collectionName, id/filter], but miss")
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrMissingRequiredParameters),
			HTTPReturnMessage: merr.ErrMissingRequiredParameters.Error() + ", required parameters: [collectionName, id/filter]",
		})
		return
	}
	req := milvuspb.DeleteRequest{
		DbName:         httpReq.DbName,
		CollectionName: httpReq.CollectionName,
	}
	username, _ := c.Get(ContextUsername)
	ctx := proxy.NewContextWithMetadata(c, username.(string), req.DbName)
	if err := checkAuthorization(ctx, c, &req); err != nil {
		return
	}
	if !h.checkDatabase(ctx, c, req.DbName) {
		return
	}
	coll, err := h.describeCollection(ctx, c, httpReq.DbName, httpReq.CollectionName, false)
	if err != nil || coll == nil {
		return
	}
	req.Expr = httpReq.Filter
	if req.Expr == "" {
		body, _ := c.Get(gin.BodyBytesKey)
		filter, err := checkGetPrimaryKey(coll.Schema, gjson.Get(string(body.([]byte)), DefaultPrimaryFieldName))
		if err != nil {
			c.JSON(http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrCheckPrimaryKey),
				HTTPReturnMessage: merr.ErrCheckPrimaryKey.Error() + ", error: " + err.Error(),
			})
			return
		}
		req.Expr = filter
	}
	response, err := h.proxy.Delete(ctx, &req)
	if err == nil {
		err = merr.Error(response.GetStatus())
	}
	if err != nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
	} else {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{}})
	}
}

func (h *Handlers) insert(c *gin.Context) {
	httpReq := InsertReq{
		DbName: DefaultDbName,
	}
	if err := c.ShouldBindBodyWith(&httpReq, binding.JSON); err != nil {
		singleInsertReq := SingleInsertReq{
			DbName: DefaultDbName,
		}
		if err = c.ShouldBindBodyWith(&singleInsertReq, binding.JSON); err != nil {
			log.Warn("high level restful api, the parameter of insert is incorrect", zap.Any("request", httpReq), zap.Error(err))
			c.AbortWithStatusJSON(http.StatusOK, gin.H{
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
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrMissingRequiredParameters),
			HTTPReturnMessage: merr.ErrMissingRequiredParameters.Error() + ", required parameters: [collectionName, data]",
		})
		return
	}
	req := milvuspb.InsertRequest{
		DbName:         httpReq.DbName,
		CollectionName: httpReq.CollectionName,
		PartitionName:  "_default",
		NumRows:        uint32(len(httpReq.Data)),
	}
	username, _ := c.Get(ContextUsername)
	ctx := proxy.NewContextWithMetadata(c, username.(string), req.DbName)
	if err := checkAuthorization(ctx, c, &req); err != nil {
		return
	}
	if !h.checkDatabase(ctx, c, req.DbName) {
		return
	}
	coll, err := h.describeCollection(ctx, c, httpReq.DbName, httpReq.CollectionName, false)
	if err != nil || coll == nil {
		return
	}
	body, _ := c.Get(gin.BodyBytesKey)
	err, httpReq.Data = checkAndSetData(string(body.([]byte)), coll)
	if err != nil {
		log.Warn("high level restful api, fail to deal with insert data", zap.Any("body", body), zap.Error(err))
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrInvalidInsertData),
			HTTPReturnMessage: merr.ErrInvalidInsertData.Error() + ", error: " + err.Error(),
		})
		return
	}
	req.FieldsData, err = anyToColumns(httpReq.Data, coll.Schema)
	if err != nil {
		log.Warn("high level restful api, fail to deal with insert data", zap.Any("data", httpReq.Data), zap.Error(err))
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrInvalidInsertData),
			HTTPReturnMessage: merr.ErrInvalidInsertData.Error() + ", error: " + err.Error(),
		})
		return
	}
	response, err := h.proxy.Insert(ctx, &req)
	if err == nil {
		err = merr.Error(response.GetStatus())
	}
	if err != nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
	} else {
		switch response.IDs.GetIdField().(type) {
		case *schemapb.IDs_IntId:
			allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
			if allowJS {
				c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{"insertCount": response.InsertCnt, "insertIds": response.IDs.IdField.(*schemapb.IDs_IntId).IntId.Data}})
			} else {
				c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{"insertCount": response.InsertCnt, "insertIds": formatInt64(response.IDs.IdField.(*schemapb.IDs_IntId).IntId.Data)}})
			}
		case *schemapb.IDs_StrId:
			c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{"insertCount": response.InsertCnt, "insertIds": response.IDs.IdField.(*schemapb.IDs_StrId).StrId.Data}})
		default:
			c.JSON(http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrCheckPrimaryKey),
				HTTPReturnMessage: merr.ErrCheckPrimaryKey.Error() + ", error: unsupported primary key data type",
			})
		}
	}
}

func (h *Handlers) upsert(c *gin.Context) {
	httpReq := UpsertReq{
		DbName: DefaultDbName,
	}
	if err := c.ShouldBindBodyWith(&httpReq, binding.JSON); err != nil {
		singleUpsertReq := SingleUpsertReq{
			DbName: DefaultDbName,
		}
		if err = c.ShouldBindBodyWith(&singleUpsertReq, binding.JSON); err != nil {
			log.Warn("high level restful api, the parameter of upsert is incorrect", zap.Any("request", httpReq), zap.Error(err))
			c.AbortWithStatusJSON(http.StatusOK, gin.H{
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
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrMissingRequiredParameters),
			HTTPReturnMessage: merr.ErrMissingRequiredParameters.Error() + ", required parameters: [collectionName, data]",
		})
		return
	}
	req := milvuspb.UpsertRequest{
		DbName:         httpReq.DbName,
		CollectionName: httpReq.CollectionName,
		PartitionName:  "_default",
		NumRows:        uint32(len(httpReq.Data)),
	}
	username, _ := c.Get(ContextUsername)
	ctx := proxy.NewContextWithMetadata(c, username.(string), req.DbName)
	if err := checkAuthorization(ctx, c, &req); err != nil {
		return
	}
	if !h.checkDatabase(ctx, c, req.DbName) {
		return
	}
	coll, err := h.describeCollection(ctx, c, httpReq.DbName, httpReq.CollectionName, false)
	if err != nil || coll == nil {
		return
	}
	if coll.Schema.AutoID {
		err := merr.WrapErrParameterInvalid("autoID: false", "autoID: true", "cannot upsert an autoID collection")
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
		return
	}
	body, _ := c.Get(gin.BodyBytesKey)
	err, httpReq.Data = checkAndSetData(string(body.([]byte)), coll)
	if err != nil {
		log.Warn("high level restful api, fail to deal with upsert data", zap.Any("body", body), zap.Error(err))
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrInvalidInsertData),
			HTTPReturnMessage: merr.ErrInvalidInsertData.Error() + ", error: " + err.Error(),
		})
		return
	}
	req.FieldsData, err = anyToColumns(httpReq.Data, coll.Schema)
	if err != nil {
		log.Warn("high level restful api, fail to deal with upsert data", zap.Any("data", httpReq.Data), zap.Error(err))
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrInvalidInsertData),
			HTTPReturnMessage: merr.ErrInvalidInsertData.Error() + ", error: " + err.Error(),
		})
		return
	}
	response, err := h.proxy.Upsert(ctx, &req)
	if err == nil {
		err = merr.Error(response.GetStatus())
	}
	if err != nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
	} else {
		switch response.IDs.GetIdField().(type) {
		case *schemapb.IDs_IntId:
			allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
			if allowJS {
				c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{"upsertCount": response.UpsertCnt, "upsertIds": response.IDs.IdField.(*schemapb.IDs_IntId).IntId.Data}})
			} else {
				c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{"upsertCount": response.UpsertCnt, "upsertIds": formatInt64(response.IDs.IdField.(*schemapb.IDs_IntId).IntId.Data)}})
			}
		case *schemapb.IDs_StrId:
			c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{"upsertCount": response.UpsertCnt, "upsertIds": response.IDs.IdField.(*schemapb.IDs_StrId).StrId.Data}})
		default:
			c.JSON(http.StatusOK, gin.H{
				HTTPReturnCode:    merr.Code(merr.ErrCheckPrimaryKey),
				HTTPReturnMessage: merr.ErrCheckPrimaryKey.Error() + ", error: unsupported primary key data type",
			})
		}
	}
}

func (h *Handlers) search(c *gin.Context) {
	httpReq := SearchReq{
		DbName: DefaultDbName,
		Limit:  100,
	}
	if err := c.ShouldBindBodyWith(&httpReq, binding.JSON); err != nil {
		log.Warn("high level restful api, the parameter of search is incorrect", zap.Any("request", httpReq), zap.Error(err))
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrIncorrectParameterFormat),
			HTTPReturnMessage: merr.ErrIncorrectParameterFormat.Error() + ", error: " + err.Error(),
		})
		return
	}
	if httpReq.CollectionName == "" || httpReq.Vector == nil {
		log.Warn("high level restful api, search require parameter: [collectionName, vector], but miss")
		c.AbortWithStatusJSON(http.StatusOK, gin.H{
			HTTPReturnCode:    merr.Code(merr.ErrMissingRequiredParameters),
			HTTPReturnMessage: merr.ErrMissingRequiredParameters.Error() + ", required parameters: [collectionName, vector]",
		})
		return
	}
	params := map[string]interface{}{ // auto generated mapping
		"level": int(commonpb.ConsistencyLevel_Bounded),
	}
	bs, _ := json.Marshal(params)
	searchParams := []*commonpb.KeyValuePair{
		{Key: common.TopKKey, Value: strconv.FormatInt(int64(httpReq.Limit), 10)},
		{Key: Params, Value: string(bs)},
		{Key: ParamRoundDecimal, Value: "-1"},
		{Key: ParamOffset, Value: strconv.FormatInt(int64(httpReq.Offset), 10)},
	}
	req := milvuspb.SearchRequest{
		DbName:             httpReq.DbName,
		CollectionName:     httpReq.CollectionName,
		Dsl:                httpReq.Filter,
		PlaceholderGroup:   vector2PlaceholderGroupBytes(httpReq.Vector),
		DslType:            commonpb.DslType_BoolExprV1,
		OutputFields:       httpReq.OutputFields,
		SearchParams:       searchParams,
		GuaranteeTimestamp: BoundedTimestamp,
		Nq:                 int64(1),
	}
	username, _ := c.Get(ContextUsername)
	ctx := proxy.NewContextWithMetadata(c, username.(string), req.DbName)
	if err := checkAuthorization(ctx, c, &req); err != nil {
		return
	}
	if !h.checkDatabase(ctx, c, req.DbName) {
		return
	}
	response, err := h.proxy.Search(ctx, &req)
	if err == nil {
		err = merr.Error(response.GetStatus())
	}
	if err != nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: merr.Code(err), HTTPReturnMessage: err.Error()})
	} else {
		if response.Results.TopK == int64(0) {
			c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: []interface{}{}})
		} else {
			allowJS, _ := strconv.ParseBool(c.Request.Header.Get(HTTPHeaderAllowInt64))
			outputData, err := buildQueryResp(response.Results.TopK, response.Results.OutputFields, response.Results.FieldsData, response.Results.Ids, response.Results.Scores, allowJS)
			if err != nil {
				log.Warn("high level restful api, fail to deal with search result", zap.Any("result", response.Results), zap.Error(err))
				c.JSON(http.StatusOK, gin.H{
					HTTPReturnCode:    merr.Code(merr.ErrInvalidSearchResult),
					HTTPReturnMessage: merr.ErrInvalidSearchResult.Error() + ", error: " + err.Error(),
				})
			} else {
				c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: outputData})
			}
		}
	}
}
