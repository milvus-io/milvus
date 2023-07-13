package httpserver

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
)

func (h *Handlers) describeCollection(c *gin.Context, dbName string, collectionName string, needAuth bool) (*milvuspb.DescribeCollectionResponse, error) {
	req := milvuspb.DescribeCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	}
	if needAuth {
		username, ok := c.Get(ContextUsername)
		if !ok {
			msg := "the user hasn't authenticate"
			c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusProxyAuthRequired, HTTPReturnMessage: msg})
			return nil, errors.New(msg)
		}
		_, authErr := proxy.PrivilegeInterceptorWithUsername(c, username.(string), &req)
		if authErr != nil {
			c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusUnauthorized, HTTPReturnMessage: authErr.Error()})
			return nil, authErr
		}
	}
	response, err := h.proxy.DescribeCollection(c, &req)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "describe collection " + collectionName + " fail", HTTPReturnError: err.Error()})
		return nil, err
	} else if response.Status.ErrorCode != commonpb.ErrorCode_Success {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: response.Status.ErrorCode, HTTPReturnMessage: response.Status.Reason})
		return nil, errors.New(response.Status.Reason)
	}
	primaryField, ok := getPrimaryField(response.Schema)
	if ok && primaryField.AutoID && !response.Schema.AutoID {
		log.Warn("primary filed autoID VS schema autoID", zap.String("collectionName", collectionName), zap.Bool("primary Field", primaryField.AutoID), zap.Bool("schema", response.Schema.AutoID))
		response.Schema.AutoID = EnableAutoID
	}
	return response, nil
}

func (h *Handlers) hasCollection(c *gin.Context, dbName string, collectionName string) (bool, error) {
	req := milvuspb.HasCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	}
	response, err := h.proxy.HasCollection(c, &req)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "check collections " + req.CollectionName + " exists fail", HTTPReturnError: err.Error()})
		return false, err
	} else if response.Status.ErrorCode != commonpb.ErrorCode_Success {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: response.Status.ErrorCode, HTTPReturnMessage: "check collections " + req.CollectionName + " exists fail", HTTPReturnError: response.Status.Reason})
		return false, errors.New(response.Status.Reason)
	} else {
		return response.Value, nil
	}
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
	router.POST(VectorSearchPath, h.search)
}

func (h *Handlers) listCollections(c *gin.Context) {
	dbName := c.DefaultQuery(HTTPDbName, DefaultDbName)
	req := milvuspb.ShowCollectionsRequest{
		DbName: dbName,
	}
	username, _ := c.Get(ContextUsername)
	_, authErr := proxy.PrivilegeInterceptorWithUsername(c, username.(string), &req)
	if authErr != nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusUnauthorized, HTTPReturnMessage: authErr.Error()})
		return
	}
	response, err := h.proxy.ShowCollections(c, &req)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "show collections fail", HTTPReturnError: err.Error()})
	} else if response.Status.ErrorCode != commonpb.ErrorCode_Success {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "show collections fail", HTTPReturnError: response.Status.Reason})
	} else {
		var collections []string
		if response.CollectionNames != nil {
			collections = response.CollectionNames
		} else {
			collections = []string{}
		}
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: collections})
	}
}

func (h *Handlers) createCollection(c *gin.Context) {
	httpReq := CreateCollectionReq{
		DbName:       DefaultDbName,
		MetricType:   DefaultMetricType,
		PrimaryField: DefaultPrimaryFieldName,
		VectorField:  DefaultVectorFieldName,
	}
	if err := c.ShouldBindBodyWith(&httpReq, binding.JSON); err != nil {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "check your parameters conform to the json format", HTTPReturnError: err.Error()})
		return
	}
	if httpReq.CollectionName == "" || httpReq.Dimension == 0 {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "collectionName and dimension are both required."})
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
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "marshal collection schema to string", HTTPReturnError: err.Error()})
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
	_, authErr := proxy.PrivilegeInterceptorWithUsername(c, username.(string), &req)
	if authErr != nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusUnauthorized, HTTPReturnMessage: authErr.Error()})
		return
	}
	resp, err := h.proxy.CreateCollection(c, &req)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "create collection " + httpReq.CollectionName + " fail", HTTPReturnError: err.Error()})
		return
	} else if resp.ErrorCode != commonpb.ErrorCode_Success {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: resp.ErrorCode, HTTPReturnMessage: "create collection " + httpReq.CollectionName + " fail", HTTPReturnError: resp.Reason})
		return
	}

	resp, err = h.proxy.CreateIndex(c, &milvuspb.CreateIndexRequest{
		DbName:         httpReq.DbName,
		CollectionName: httpReq.CollectionName,
		FieldName:      httpReq.VectorField,
		ExtraParams:    []*commonpb.KeyValuePair{{Key: common.MetricTypeKey, Value: httpReq.MetricType}},
	})
	if err != nil {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "create index for collection " + httpReq.CollectionName + " fail, after the collection was created", HTTPReturnError: err.Error()})
		return
	} else if resp.ErrorCode != commonpb.ErrorCode_Success {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: resp.ErrorCode, HTTPReturnMessage: "create index for collection " + httpReq.CollectionName + " fail, after the collection was created", HTTPReturnError: resp.Reason})
		return
	}
	resp, err = h.proxy.LoadCollection(c, &milvuspb.LoadCollectionRequest{
		DbName:         httpReq.DbName,
		CollectionName: httpReq.CollectionName,
	})
	if err != nil {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "load collection " + httpReq.CollectionName + " fail, after the index was created", HTTPReturnError: err.Error()})
		return
	} else if resp.ErrorCode != commonpb.ErrorCode_Success {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: resp.ErrorCode, HTTPReturnMessage: "load collection " + httpReq.CollectionName + " fail, after the index was created", HTTPReturnError: resp.Reason})
		return
	}
	c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{}})
}

func (h *Handlers) getCollectionDetails(c *gin.Context) {
	collectionName := c.Query(HTTPCollectionName)
	if collectionName == "" {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "collectionName is required."})
		return
	}
	dbName := c.DefaultQuery(HTTPDbName, DefaultDbName)
	coll, err := h.describeCollection(c, dbName, collectionName, true)
	if err != nil {
		return
	}
	stateResp, stateErr := h.proxy.GetLoadState(c, &milvuspb.GetLoadStateRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	collLoadState := ""
	if stateErr != nil {
		log.Warn("get collection load state fail", zap.String("collection", collectionName), zap.String("err", stateErr.Error()))
	} else if stateResp.Status.ErrorCode != commonpb.ErrorCode_Success {
		log.Warn("get collection load state fail", zap.String("collection", collectionName), zap.String("err", stateResp.Status.Reason))
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
	indexResp, indexErr := h.proxy.DescribeIndex(c, &milvuspb.DescribeIndexRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		FieldName:      vectorField,
	})
	var indexDesc []gin.H
	if indexErr != nil {
		indexDesc = []gin.H{}
		log.Warn("get indexes description fail", zap.String("collection", collectionName), zap.String("vectorField", vectorField), zap.String("err", indexErr.Error()))
	} else if indexResp.Status.ErrorCode != commonpb.ErrorCode_Success {
		indexDesc = []gin.H{}
		log.Warn("get indexes description fail", zap.String("collection", collectionName), zap.String("vectorField", vectorField), zap.String("err", indexResp.Status.Reason))
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
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "check your parameters conform to the json format", HTTPReturnError: err.Error()})
		return
	}
	if httpReq.CollectionName == "" {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "collectionName is required."})
		return
	}
	has, err := h.hasCollection(c, httpReq.DbName, httpReq.CollectionName)
	if err != nil {
		return
	}
	if !has {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "can't find collection: " + httpReq.CollectionName})
		return
	}
	req := milvuspb.DropCollectionRequest{
		DbName:         httpReq.DbName,
		CollectionName: httpReq.CollectionName,
	}
	username, _ := c.Get(ContextUsername)
	_, authErr := proxy.PrivilegeInterceptorWithUsername(c, username.(string), &req)
	if authErr != nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusUnauthorized, HTTPReturnMessage: authErr.Error()})
		return
	}
	response, err := h.proxy.DropCollection(c, &req)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "drop collection " + httpReq.CollectionName + " fail", HTTPReturnError: err.Error()})
	} else if response.ErrorCode != commonpb.ErrorCode_Success {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: response.ErrorCode, HTTPReturnMessage: "drop collection " + httpReq.CollectionName + " fail", HTTPReturnError: response.Reason})
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
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "check your parameters conform to the json format", HTTPReturnError: err.Error()})
		return
	}
	if httpReq.CollectionName == "" {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "collectionName is required."})
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
	_, authErr := proxy.PrivilegeInterceptorWithUsername(c, username.(string), &req)
	if authErr != nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusUnauthorized, HTTPReturnMessage: authErr.Error()})
		return
	}
	response, err := h.proxy.Query(c, &req)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "query fail", HTTPReturnError: err.Error()})
	} else if response.Status.ErrorCode != commonpb.ErrorCode_Success {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: response.Status.ErrorCode, HTTPReturnMessage: response.Status.Reason})
	} else {
		outputData, err := buildQueryResp(int64(0), response.OutputFields, response.FieldsData, nil)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "show result by row wrong", "originData": response.FieldsData, HTTPReturnError: err.Error()})
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
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "check your parameters conform to the json format", HTTPReturnError: err.Error()})
		return
	}
	if httpReq.CollectionName == "" {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "collectionName is required."})
		return
	}
	coll, err := h.describeCollection(c, httpReq.DbName, httpReq.CollectionName, false)
	if err != nil || coll == nil {
		return
	}
	body, _ := c.Get(gin.BodyBytesKey)
	filter, err := checkGetPrimaryKey(coll.Schema, gjson.Get(string(body.([]byte)), "id"))
	if err != nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "make sure the collection's primary field", HTTPReturnError: err.Error()})
		return
	}
	req := milvuspb.QueryRequest{
		DbName:             httpReq.DbName,
		CollectionName:     httpReq.CollectionName,
		Expr:               filter,
		OutputFields:       httpReq.OutputFields,
		GuaranteeTimestamp: BoundedTimestamp,
	}
	username, _ := c.Get(ContextUsername)
	_, authErr := proxy.PrivilegeInterceptorWithUsername(c, username.(string), &req)
	if authErr != nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusUnauthorized, HTTPReturnMessage: authErr.Error()})
		return
	}
	response, err := h.proxy.Query(c, &req)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "query fail", HTTPReturnError: err.Error()})
	} else if response.Status.ErrorCode != commonpb.ErrorCode_Success {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: response.Status.ErrorCode, HTTPReturnMessage: response.Status.Reason})
	} else {
		outputData, err := buildQueryResp(int64(0), response.OutputFields, response.FieldsData, nil)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "show result by row wrong", "originData": response.FieldsData, HTTPReturnError: err.Error()})
		} else {
			c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: outputData})
			log.Error("get resultIS: ", zap.Any("res", outputData))
		}
	}
}

func (h *Handlers) delete(c *gin.Context) {
	httpReq := DeleteReq{
		DbName: DefaultDbName,
	}
	if err := c.ShouldBindBodyWith(&httpReq, binding.JSON); err != nil {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "check your parameters conform to the json format", HTTPReturnError: err.Error()})
		return
	}
	if httpReq.CollectionName == "" {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "collectionName is required."})
		return
	}
	coll, err := h.describeCollection(c, httpReq.DbName, httpReq.CollectionName, false)
	if err != nil || coll == nil {
		return
	}
	body, _ := c.Get(gin.BodyBytesKey)
	filter, err := checkGetPrimaryKey(coll.Schema, gjson.Get(string(body.([]byte)), "id"))
	if err != nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "make sure the collection's primary field", HTTPReturnError: err.Error()})
		return
	}
	req := milvuspb.DeleteRequest{
		DbName:         httpReq.DbName,
		CollectionName: httpReq.CollectionName,
		Expr:           filter,
	}
	username, _ := c.Get(ContextUsername)
	_, authErr := proxy.PrivilegeInterceptorWithUsername(c, username.(string), &req)
	if authErr != nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusUnauthorized, HTTPReturnMessage: authErr.Error()})
		return
	}
	response, err := h.proxy.Delete(c, &req)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "delete fail", HTTPReturnError: err.Error()})
	} else if response.Status.ErrorCode != commonpb.ErrorCode_Success {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: response.Status.ErrorCode, HTTPReturnMessage: "delete fail", HTTPReturnError: response.Status.Reason})
	} else {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{}})
	}
}

func (h *Handlers) insert(c *gin.Context) {
	httpReq := InsertReq{
		DbName: DefaultDbName,
	}
	if err := c.ShouldBindBodyWith(&httpReq, binding.JSON); err != nil {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "check your parameters conform to the json format", HTTPReturnError: err.Error()})
		return
	}
	if httpReq.CollectionName == "" {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "collectionName is required."})
		return
	}
	coll, err := h.describeCollection(c, httpReq.DbName, httpReq.CollectionName, false)
	if err != nil || coll == nil {
		return
	}
	body, _ := c.Get(gin.BodyBytesKey)
	err = checkAndSetData(string(body.([]byte)), coll, &httpReq)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "checkout your params", HTTPReturnError: err.Error()})
		return
	}
	req := milvuspb.InsertRequest{
		DbName:         httpReq.DbName,
		CollectionName: httpReq.CollectionName,
		PartitionName:  "_default",
		NumRows:        uint32(len(httpReq.Data)),
	}
	req.FieldsData, err = anyToColumns(httpReq.Data, coll.Schema)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "insert data by column wrong", HTTPReturnError: err.Error()})
		return
	}
	username, _ := c.Get(ContextUsername)
	_, authErr := proxy.PrivilegeInterceptorWithUsername(c, username.(string), &req)
	if authErr != nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusUnauthorized, HTTPReturnMessage: authErr.Error()})
		return
	}
	response, err := h.proxy.Insert(c, &req)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "insert fail", HTTPReturnError: err.Error()})
	} else if response.Status.ErrorCode != commonpb.ErrorCode_Success {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: response.Status.ErrorCode, HTTPReturnMessage: "insert fail", HTTPReturnError: response.Status.Reason})
	} else {
		switch response.IDs.GetIdField().(type) {
		case *schemapb.IDs_IntId:
			c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{"insertCount": response.InsertCnt, "insertIds": response.IDs.IdField.(*schemapb.IDs_IntId).IntId.Data}})
		case *schemapb.IDs_StrId:
			c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: gin.H{"insertCount": response.InsertCnt, "insertIds": response.IDs.IdField.(*schemapb.IDs_StrId).StrId.Data}})
		default:
			c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "ids' type neither int or string"})
		}
	}
}

func (h *Handlers) search(c *gin.Context) {
	httpReq := SearchReq{
		DbName: DefaultDbName,
		Limit:  100,
	}
	if err := c.ShouldBindBodyWith(&httpReq, binding.JSON); err != nil {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "check your parameters conform to the json format", HTTPReturnError: err.Error()})
		return
	}
	if httpReq.CollectionName == "" {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "collectionName is required."})
		return
	}

	searchParams := []*commonpb.KeyValuePair{
		{Key: common.TopKKey, Value: strconv.FormatInt(int64(httpReq.Limit), 10)},
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
	_, authErr := proxy.PrivilegeInterceptorWithUsername(c, username.(string), &req)
	if authErr != nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusUnauthorized, HTTPReturnMessage: authErr.Error()})
		return
	}
	response, err := h.proxy.Search(c, &req)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "search fail", HTTPReturnError: err.Error()})
	} else if response.Status.ErrorCode != commonpb.ErrorCode_Success {
		c.JSON(http.StatusOK, gin.H{HTTPReturnCode: response.Status.ErrorCode, HTTPReturnMessage: "search fail", HTTPReturnError: response.Status.Reason})
	} else {
		outputData, err := buildQueryResp(response.Results.TopK, response.Results.OutputFields, response.Results.FieldsData, response.Results.Scores)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusBadRequest, HTTPReturnMessage: "show result by row wrong", HTTPReturnError: err.Error()})
		} else {
			c.JSON(http.StatusOK, gin.H{HTTPReturnCode: http.StatusOK, HTTPReturnData: outputData})
		}
	}
}
