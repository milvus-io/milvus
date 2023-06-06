package httpserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus-sdk-go/v2/entity"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/spf13/cast"
	"github.com/tidwall/gjson"
	"go.uber.org/zap"
	"net/http"
	"reflect"
	"strings"
)

const (
	VectorCollectionsPath         = "/vector/collections"
	VectorCollectionsCreatePath   = "/vector/collections/create"
	VectorCollectionsDescribePath = "/vector/collections/describe"
	VectorCollectionsDropPath     = "/vector/collections/drop"
	VectorInsertPath              = "/vector/insert"
	VectorSearchPath              = "/vector/search"
	VectorGetPath                 = "/vector/get"
	VectorQueryPath               = "/vector/query"
	VectorDeletePath              = "/vector/delete"

	Int8    = "int8"
	Int16   = "int16"
	Int32   = "int32"
	Int64   = "int64"
	Varchar = "varchar"
	String  = "string"
	Float64 = "float64"
	Float32 = "float32"
	Bool    = "bool"

	InnerFloatVector  = "floatvector"
	InnerBinaryVector = "binaryvector"

	OuterFloatVector  = "floatVector"
	OuterBinaryVector = "binaryVector"

	MetricType              = "metric_type"
	VectorIndexDefaultName  = "vector_idx"
	VectorFieldDefaultName  = "vector"
	PrimaryFieldDefaultName = "id"

	CollectionNameRegex = "^[A-Za-z_]{1}[A-Za-z0-9_]{0,254}$"
	CollNameLengthMin   = 1
	CollNameLengthMax   = 255

	ShardNumMix     = 1
	ShardNumMax     = 32
	ShardNumDefault = 2

	EnableDynamic = true
	EnableAutoID  = true
	DisableAutoID = false

	DefaultListenPort = "9092"
	ListenPortEnvKey  = "RESTFUL_API_PORT"
)

func (h *Handlers) describeCollection(c *gin.Context, collectionName string, needAuth bool) (*milvuspb.DescribeCollectionResponse, error) {
	req := milvuspb.DescribeCollectionRequest{
		Base:           nil,
		DbName:         "",
		CollectionName: collectionName,
		CollectionID:   0,
		TimeStamp:      0,
	}
	if needAuth {
		_, authErr := proxy.PrivilegeInterceptor(c, &req)
		if authErr != nil {
			c.JSON(http.StatusOK, gin.H{"code": http.StatusUnauthorized, "message": authErr.Error()})
			return nil, authErr
		}
	}
	response, err := h.proxy.DescribeCollection(c, &req)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{"code": 400, "message": "describe collection " + collectionName + " fail", "error": err})
		return nil, err
	} else if response.Status.ErrorCode != commonpb.ErrorCode_Success {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{"code": response.Status.ErrorCode, "message": response.Status.Reason})
		return nil, errors.New(response.Status.Reason)
	}
	primaryField := getPrimaryField(response.Schema)
	if primaryField.AutoID && !response.Schema.AutoID {
		log.Warn("primary filed autoID VS schema autoID", zap.String("collectionName", collectionName), zap.Bool("primary Field", primaryField.AutoID), zap.Bool("schema", response.Schema.AutoID))
		response.Schema.AutoID = EnableAutoID
	}
	return response, nil
}

func (h *Handlers) hasCollection(c *gin.Context, collectionName string) (bool, error) {
	req := milvuspb.HasCollectionRequest{
		Base:           nil,
		DbName:         "",
		CollectionName: collectionName,
		TimeStamp:      0,
	}
	response, err := h.proxy.HasCollection(c, &req)
	if err != nil {
		c.AbortWithStatusJSON(http.StatusOK, gin.H{"code": 400, "message": err})
		return false, err
	}
	return response.Value, nil
}

func printFields(fields []*schemapb.FieldSchema) []gin.H {
	res := []gin.H{}
	for _, field := range fields {
		res = append(res, gin.H{
			"name":        field.Name,
			"type":        field.DataType.String(),
			"primaryKey":  field.IsPrimaryKey,
			"autoId":      field.AutoID,
			"description": field.Description,
		})
	}
	return res
}

func getMetricType(pairs []*commonpb.KeyValuePair) string {
	metricType := "L2"
	for _, pair := range pairs {
		if pair.Key == "metric_type" {
			metricType = pair.Value
			break
		}
	}
	return metricType
}

func printIndexes(fields []*milvuspb.IndexDescription) []gin.H {
	res := []gin.H{}
	for _, field := range fields {
		res = append(res, gin.H{
			"indexName":  field.IndexName,
			"fieldName":  field.FieldName,
			"metricType": getMetricType(field.Params),
		})
	}
	return res
}

func (h *Handlers) RegisterRoutesToV1(router gin.IRouter) {
	router.GET(VectorCollectionsPath, func(c *gin.Context) {
		req := milvuspb.ShowCollectionsRequest{
			Base:      nil,
			DbName:    "",
			TimeStamp: 0,
			Type:      0,
		}
		_, authErr := proxy.PrivilegeInterceptor(c, &req)
		if authErr != nil {
			c.JSON(http.StatusOK, gin.H{"code": http.StatusUnauthorized, "message": authErr.Error()})
			return
		}
		response, err := h.proxy.ShowCollections(c, &req)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"code": 400, "message": "show collections fail", "error": err})
		} else {
			c.JSON(http.StatusOK, gin.H{"code": 200, "data": response.CollectionNames})
		}
	})
	router.POST(VectorCollectionsCreatePath, func(c *gin.Context) {
		httpReq := CreateCollectionReq{
			CollectionName: "",
			Dimension:      128,
			Description:    "",
			MetricType:     "L2",
			PrimaryField:   "id",
			VectorField:    "vector",
		}
		if err := c.ShouldBindBodyWith(&httpReq, binding.JSON); err != nil {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{"code": 400, "message": "check your parameters conform to the json format", "error": err})
			return
		}
		has, err := h.hasCollection(c, httpReq.CollectionName)
		if err != nil {
			return
		}
		if has {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{"code": 200, "message": "collection " + httpReq.CollectionName + " already exist."})
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
							Key:   "dim",
							Value: fmt.Sprintf("%d", httpReq.Dimension),
						},
					},
					AutoID: DisableAutoID,
				},
			},
			EnableDynamicField: EnableDynamic,
		})
		if err != nil {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{"code": 400, "message": "marshal collection schema to string", "error": err})
			return
		}
		req := milvuspb.CreateCollectionRequest{
			Base:             nil,
			DbName:           "",
			CollectionName:   httpReq.CollectionName,
			Schema:           schema,
			ShardsNum:        ShardNumDefault,
			ConsistencyLevel: commonpb.ConsistencyLevel_Bounded,
			Properties:       nil,
			NumPartitions:    0,
		}
		_, authErr := proxy.PrivilegeInterceptor(c, &req)
		if authErr != nil {
			c.JSON(http.StatusOK, gin.H{"code": http.StatusUnauthorized, "message": authErr.Error()})
			return
		}
		_, err = h.proxy.CreateCollection(c, &req)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{"code": 400, "message": "create collection " + httpReq.CollectionName + " fail", "error": err})
			return
		}
		_, err = h.proxy.CreateIndex(c, &milvuspb.CreateIndexRequest{
			Base:           nil,
			DbName:         "",
			CollectionName: httpReq.CollectionName,
			FieldName:      httpReq.VectorField,
			ExtraParams:    []*commonpb.KeyValuePair{{Key: MetricType, Value: httpReq.MetricType}},
			IndexName:      "",
		})
		if err != nil {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{"code": 400, "message": "create index for collection " + httpReq.CollectionName + " fail, after the collection was created", "error": err})
			return
		}
		_, err = h.proxy.LoadCollection(c, &milvuspb.LoadCollectionRequest{
			Base:           nil,
			DbName:         "",
			CollectionName: httpReq.CollectionName,
			ReplicaNumber:  0,
			ResourceGroups: nil,
			Refresh:        false,
		})
		if err != nil {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{"code": 400, "message": "load collection " + httpReq.CollectionName + " fail, after the index was created", "error": err})
			return
		}
		c.JSON(http.StatusOK, gin.H{"code": 200, "data": gin.H{}})
	})
	router.GET(VectorCollectionsDescribePath, func(c *gin.Context) {
		collectionName := c.Query("collectionName")
		coll, err := h.describeCollection(c, collectionName, true)
		if err != nil {
			return
		}
		stateResp, stateErr := h.proxy.GetLoadState(c, &milvuspb.GetLoadStateRequest{
			DbName:         "",
			CollectionName: collectionName,
		})
		if stateErr != nil {
			log.Warn("get collection load state fail", zap.String("collection", collectionName), zap.String("err", stateErr.Error()))
		} else if stateResp.Status.ErrorCode != commonpb.ErrorCode_Success {
			log.Warn("get collection load state fail", zap.String("collection", collectionName), zap.String("err", stateResp.Status.Reason))
		}
		vectorField := ""
		for _, field := range coll.Schema.Fields {
			if field.DataType == schemapb.DataType_BinaryVector || field.DataType == schemapb.DataType_FloatVector {
				vectorField = field.Name
				break
			}
		}
		indexResp, indexErr := h.proxy.DescribeIndex(c, &milvuspb.DescribeIndexRequest{
			Base:           nil,
			DbName:         "",
			CollectionName: collectionName,
			FieldName:      vectorField,
			IndexName:      "",
		})
		if indexErr != nil {
			log.Warn("get indexes description fail", zap.String("collection", collectionName), zap.String("vectorField", vectorField), zap.String("err", indexErr.Error()))
		} else if indexResp.Status.ErrorCode != commonpb.ErrorCode_Success {
			log.Warn("get indexes description fail", zap.String("collection", collectionName), zap.String("vectorField", vectorField), zap.String("err", indexResp.Status.Reason))
		}
		c.JSON(http.StatusOK, gin.H{"code": 200, "data": gin.H{
			"collectionName": coll.CollectionName,
			"description":    coll.Schema.Description,
			"fields":         printFields(coll.Schema.Fields),
			"indexes":        printIndexes(indexResp.IndexDescriptions),
			"load":           stateResp.State.String(),
			"shardsNum":      coll.ShardsNum,
			"enableDynamic":  coll.Schema.EnableDynamicField,
		}})
	})
	router.POST(VectorCollectionsDropPath, func(c *gin.Context) {
		httpReq := DropCollectionReq{
			CollectionName: "",
		}
		if err := c.ShouldBindBodyWith(&httpReq, binding.JSON); err != nil {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{"code": 400, "message": "check your parameters conform to the json format", "error": err})
			return
		}
		has, err := h.hasCollection(c, httpReq.CollectionName)
		if err != nil {
			return
		}
		if !has {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{"code": 200, "message": "can't find collection:  " + httpReq.CollectionName})
			return
		}
		req := milvuspb.DropCollectionRequest{
			Base:           nil,
			DbName:         "",
			CollectionName: httpReq.CollectionName,
		}
		_, authErr := proxy.PrivilegeInterceptor(c, &req)
		if authErr != nil {
			c.JSON(http.StatusOK, gin.H{"code": http.StatusUnauthorized, "message": authErr.Error()})
			return
		}
		response, err := h.proxy.DropCollection(c, &req)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"code": 400, "message": "drop collection " + httpReq.CollectionName + " fail", "error": err})
		} else if response.ErrorCode != commonpb.ErrorCode_Success {
			c.JSON(http.StatusOK, gin.H{"code": response.ErrorCode, "message": response.Reason})
		} else {
			c.JSON(http.StatusOK, gin.H{"code": 200, "data": gin.H{}})
		}
	})
	router.POST(VectorQueryPath, func(c *gin.Context) {
		httpReq := QueryReq{
			CollectionName: "",
			OutputFields:   nil,
			Filter:         "",
			Limit:          100,
			Offset:         0,
		}
		if err := c.ShouldBindBodyWith(&httpReq, binding.JSON); err != nil {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{"code": 400, "message": "check your parameters conform to the json format", "error": err})
			return
		}
		coll, err := h.describeCollection(c, httpReq.CollectionName, false)
		if err != nil || coll == nil {
			return
		}
		httpReq.OutputFields = getDynamicOutputFields(coll.Schema, httpReq.OutputFields)
		req := milvuspb.QueryRequest{
			Base:                  nil,
			DbName:                "",
			CollectionName:        httpReq.CollectionName,
			Expr:                  httpReq.Filter,
			OutputFields:          httpReq.OutputFields,
			PartitionNames:        nil,
			TravelTimestamp:       0,
			GuaranteeTimestamp:    2, // BoundedTimestamp
			QueryParams:           nil,
			NotReturnAllMeta:      false,
			UseDefaultConsistency: true,
		}
		_, authErr := proxy.PrivilegeInterceptor(c, &req)
		if authErr != nil {
			c.JSON(http.StatusOK, gin.H{"code": http.StatusUnauthorized, "message": authErr.Error()})
			return
		}
		response, err := h.proxy.Query(c, &req)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"code": 400, "message": "query fail", "error": err})
		} else if response.Status.ErrorCode != commonpb.ErrorCode_Success {
			c.JSON(http.StatusOK, gin.H{"code": response.Status.ErrorCode, "message": response.Status.Reason})
		} else {
			outputData, err := buildQueryResp(req.OutputFields, response.FieldsData, nil)
			if err != nil {
				c.JSON(http.StatusOK, gin.H{"code": 400, "message": "show result by row wrong", "originData": response.FieldsData, "error": err})
			} else {
				c.JSON(http.StatusOK, gin.H{"code": 200, "data": outputData})
			}
		}
	})
	router.POST(VectorGetPath, func(c *gin.Context) {
		httpReq := GetReq{
			CollectionName: "",
			OutputFields:   nil,
			Id:             nil,
		}
		if err := c.ShouldBindBodyWith(&httpReq, binding.JSON); err != nil {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{"code": 400, "message": "check your parameters conform to the json format", "error": err})
			return
		}
		coll, err := h.describeCollection(c, httpReq.CollectionName, false)
		if err != nil || coll == nil {
			return
		}
		httpReq.OutputFields = getDynamicOutputFields(coll.Schema, httpReq.OutputFields)
		body, _ := c.Get(gin.BodyBytesKey)
		filter := checkGetPrimaryKey(coll, gjson.Get(string(body.([]byte)), "id"))
		req := milvuspb.QueryRequest{
			Base:                  nil,
			DbName:                "",
			CollectionName:        httpReq.CollectionName,
			Expr:                  filter,
			OutputFields:          httpReq.OutputFields,
			PartitionNames:        []string{},
			GuaranteeTimestamp:    2, // BoundedTimestamp
			QueryParams:           nil,
			NotReturnAllMeta:      false,
			UseDefaultConsistency: true,
		}
		_, authErr := proxy.PrivilegeInterceptor(c, &req)
		if authErr != nil {
			c.JSON(http.StatusOK, gin.H{"code": http.StatusUnauthorized, "message": authErr.Error()})
			return
		}
		response, err := h.proxy.Query(c, &req)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"code": 400, "message": "query fail", "error": err})
		} else if response.Status.ErrorCode != commonpb.ErrorCode_Success {
			c.JSON(http.StatusOK, gin.H{"code": response.Status.ErrorCode, "message": response.Status.Reason})
		} else {
			outputData, err := buildQueryResp(req.OutputFields, response.FieldsData, nil)
			if err != nil {
				c.JSON(http.StatusOK, gin.H{"code": 400, "message": "show result by row wrong", "originData": response.FieldsData, "error": err})
			}
			c.JSON(http.StatusOK, gin.H{"code": 200, "data": outputData})
		}
	})
	router.POST(VectorDeletePath, func(c *gin.Context) {
		httpReq := DeleteReq{
			CollectionName: "",
			Id:             nil,
		}
		if err := c.ShouldBindBodyWith(&httpReq, binding.JSON); err != nil {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{"code": 400, "message": "check your parameters conform to the json format", "error": err})
			return
		}
		coll, err := h.describeCollection(c, httpReq.CollectionName, false)
		if err != nil || coll == nil {
			return
		}
		primaryField := getPrimaryField(coll.Schema)
		primaryFieldName := primaryField.Name
		primaryFieldType := primaryField.DataType

		body, _ := c.Get(gin.BodyBytesKey)
		resultStr, err := convertRange(primaryField, gjson.Get(string(body.([]byte)), "id"))
		if err != nil {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{"code": 400, "message": "checkout your params", "originRequest": httpReq, "error": err})
			return
		}
		dataArray := strings.Split(resultStr, ",")
		var ids entity.Column
		if primaryFieldType == schemapb.DataType_Int64 {
			dataArray := convertStringArrayToTypeArray(dataArray, primaryFieldType)
			ids = entity.NewColumnInt64(primaryFieldName, dataArray.([]int64))
		} else if primaryFieldType == schemapb.DataType_VarChar {
			ids = entity.NewColumnVarChar(primaryFieldName, dataArray)
		} else {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{"code": 400, "message": "primary field type neither Int64 nor VarChar", "error": err})
			return
		}
		expr := PKs2Expr(ids.Name(), ids)
		req := milvuspb.DeleteRequest{
			Base:           nil,
			DbName:         "",
			CollectionName: httpReq.CollectionName,
			Expr:           expr,
		}
		_, authErr := proxy.PrivilegeInterceptor(c, &req)
		if authErr != nil {
			c.JSON(http.StatusOK, gin.H{"code": http.StatusUnauthorized, "message": authErr.Error()})
			return
		}
		response, err := h.proxy.Delete(c, &req)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"code": 400, "message": "delete fail", "error": err})
		} else if response.Status.ErrorCode != commonpb.ErrorCode_Success {
			c.JSON(http.StatusOK, gin.H{"code": response.Status.ErrorCode, "message": response.Status.Reason})
		} else {
			c.JSON(http.StatusOK, gin.H{"code": 200, "data": gin.H{}})
		}
	})
	router.POST(VectorInsertPath, func(c *gin.Context) {
		httpReq := InsertReq{
			CollectionName: "",
			Data:           nil,
		}
		if err := c.ShouldBindBodyWith(&httpReq, binding.JSON); err != nil {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{"code": 400, "message": "check your parameters conform to the json format", "error": err})
			return
		}
		coll, err := h.describeCollection(c, httpReq.CollectionName, false)
		if err != nil || coll == nil {
			return
		}
		body, _ := c.Get(gin.BodyBytesKey)
		err = checkAndSetData(string(body.([]byte)), coll, &httpReq)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{"code": 400, "message": "checkout your params", "originRequest": httpReq, "error": err})
			return
		}
		rows := rowToEntityRowArray(httpReq.Data)
		fields := []*entity.Field{}
		for _, field := range coll.Schema.Fields {
			fields = append(fields, &entity.Field{
				ID:             field.FieldID,
				Name:           field.Name,
				PrimaryKey:     field.IsPrimaryKey,
				AutoID:         field.AutoID,
				Description:    field.Description,
				DataType:       entity.FieldType(field.DataType),
				TypeParams:     convertKeyValuaPairToMap(field.TypeParams),
				IndexParams:    convertKeyValuaPairToMap(field.IndexParams),
				IsDynamic:      field.IsDynamic,
				IsPartitionKey: field.IsPartitionKey,
			})
		}
		columns, err := entity.RowsToColumns(rows, &entity.Schema{
			CollectionName:     coll.CollectionName,
			Description:        coll.Schema.Description,
			AutoID:             coll.Schema.AutoID,
			Fields:             fields,
			EnableDynamicField: coll.Schema.EnableDynamicField,
		})
		if err != nil {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{"code": 400, "message": "insert data by column wrong", "originData": rows, "error": err})
			return
		}
		req := milvuspb.InsertRequest{
			DbName:         "", // reserved
			CollectionName: httpReq.CollectionName,
			PartitionName:  "_default",
			NumRows:        uint32(len(rows)),
		}
		req.NumRows = uint32(len(rows))
		for _, column := range columns {
			req.FieldsData = append(req.FieldsData, column.FieldData())
		}
		_, authErr := proxy.PrivilegeInterceptor(c, &req)
		if authErr != nil {
			c.JSON(http.StatusOK, gin.H{"code": http.StatusUnauthorized, "message": authErr.Error()})
			return
		}
		response, err := h.proxy.Insert(c, &req)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"code": 400, "message": "insert fail", "error": err})
		} else if response.Status.ErrorCode != commonpb.ErrorCode_Success {
			c.JSON(http.StatusOK, gin.H{"code": response.Status.ErrorCode, "message": response.Status.Reason})
		} else {
			switch response.IDs.GetIdField().(type) {
			case *schemapb.IDs_IntId:
				c.JSON(http.StatusOK, gin.H{"code": 200, "data": gin.H{"insertCount": response.InsertCnt, "insertIds": response.IDs.IdField.(*schemapb.IDs_IntId).IntId.Data}})
			case *schemapb.IDs_StrId:
				c.JSON(http.StatusOK, gin.H{"code": 200, "data": gin.H{"insertCount": response.InsertCnt, "insertIds": response.IDs.IdField.(*schemapb.IDs_StrId).StrId.Data}})
			}
		}
	})
	router.POST(VectorSearchPath, func(c *gin.Context) {
		httpReq := SearchReq{
			CollectionName: "",
			Filter:         "",
			Limit:          100,
			Offset:         0,
			OutputFields:   nil,
			Vector:         nil,
		}
		if err := c.ShouldBindBodyWith(&httpReq, binding.JSON); err != nil {
			c.AbortWithStatusJSON(http.StatusOK, gin.H{"code": 400, "message": "check your parameters conform to the json format", "error": err})
			return
		}
		coll, err := h.describeCollection(c, httpReq.CollectionName, false)
		if err != nil {
			return
		}
		httpReq.OutputFields = getDynamicOutputFields(coll.Schema, httpReq.OutputFields)
		vectorField := ""
		for _, field := range coll.Schema.Fields {
			if field.DataType == schemapb.DataType_BinaryVector || field.DataType == schemapb.DataType_FloatVector {
				vectorField = field.Name
				break
			}
		}
		indexResp, indexErr := h.proxy.DescribeIndex(c, &milvuspb.DescribeIndexRequest{
			Base:           nil,
			DbName:         "",
			CollectionName: httpReq.CollectionName,
			FieldName:      vectorField,
			IndexName:      "",
		})
		if indexErr != nil {
			log.Warn("get indexes description fail", zap.String("collection", httpReq.CollectionName), zap.String("vectorField", vectorField), zap.String("err", indexErr.Error()))
		} else if indexResp.Status.ErrorCode != commonpb.ErrorCode_Success {
			log.Warn("get indexes description fail", zap.String("collection", httpReq.CollectionName), zap.String("vectorField", vectorField), zap.String("err", indexResp.Status.Reason))
		}

		sp, err := entity.NewIndexAUTOINDEXSearchParam(int(commonpb.ConsistencyLevel_Bounded))
		float32Array, _ := convertData(httpReq.Vector, InnerFloatVector)
		metricType := getMetricType(indexResp.IndexDescriptions[0].Params)
		params := sp.Params()
		bs, err := json.Marshal(params)
		if err != nil {
			log.Warn("get indexes description fail", zap.String("collection", httpReq.CollectionName))
		}

		searchParams := entity.MapKvPairs(map[string]string{
			"anns_field":    vectorField,
			"topk":          fmt.Sprintf("%d", httpReq.Limit),
			"params":        string(bs),
			"metric_type":   metricType,
			"round_decimal": "-1",
			"offset":        fmt.Sprintf("%d", httpReq.Offset),
		})
		req := milvuspb.SearchRequest{
			DbName:             "",
			CollectionName:     httpReq.CollectionName,
			PartitionNames:     []string{},
			Dsl:                httpReq.Filter,
			PlaceholderGroup:   vector2PlaceholderGroupBytes([]entity.Vector{entity.FloatVector(float32Array.([]float32))}),
			DslType:            commonpb.DslType_BoolExprV1,
			OutputFields:       httpReq.OutputFields,
			SearchParams:       searchParams,
			GuaranteeTimestamp: 2, // BoundedTimestamp
			Nq:                 int64(1),
		}
		_, authErr := proxy.PrivilegeInterceptor(c, &req)
		if authErr != nil {
			c.JSON(http.StatusOK, gin.H{"code": http.StatusUnauthorized, "message": authErr.Error()})
			return
		}
		response, err := h.proxy.Search(c, &req)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{"code": 400, "message": "search fail", "error": err})
		} else if response.Status.ErrorCode != commonpb.ErrorCode_Success {
			c.JSON(http.StatusOK, gin.H{"code": response.Status.ErrorCode, "message": response.Status.Reason})
		} else {
			outputData, err := buildQueryResp(req.OutputFields, response.Results.FieldsData, response.Results.Scores)
			if err != nil {
				c.JSON(http.StatusOK, gin.H{"code": 400, "message": "show result by row wrong", "originResult": response.Results, "error": err})
			} else {
				c.JSON(http.StatusOK, gin.H{"code": 200, "data": outputData})
			}
		}
	})
}

type CreateCollectionReq struct {
	CollectionName string `json:"collectionName" validate:"required"`
	Dimension      int32  `json:"dimension" validate:"required"`
	Description    string `json:"description"`
	MetricType     string `json:"metricType"`
	PrimaryField   string `json:"primaryField"`
	VectorField    string `json:"vectorField"`
}

type DropCollectionReq struct {
	CollectionName string `json:"collectionName" validate:"required"`
}

type QueryReq struct {
	CollectionName string   `json:"collectionName" validate:"required"`
	OutputFields   []string `json:"outputFields"`
	Filter         string   `json:"filter" validate:"required"`
	Limit          int32    `json:"limit"`
	Offset         int32    `json:"offset"`
}

type GetReq struct {
	CollectionName string   `json:"collectionName" validate:"required"`
	OutputFields   []string `json:"outputFields"`
	// TODO 先统一按照 id处理，后续再调整
	Id interface{} `json:"id" validate:"required"`
}

type DeleteReq struct {
	CollectionName string `json:"collectionName" validate:"required"`
	// TODO 先统一按照 id处理，后续再调整
	Id interface{} `json:"id" validate:"required"`
}

type InsertReq struct {
	CollectionName string                   `json:"collectionName" validate:"required"`
	Data           []map[string]interface{} `json:"data" validate:"required"`
}

type SearchReq struct {
	CollectionName string    `json:"collectionName" validate:"required"`
	Filter         string    `json:"filter"`
	Limit          int32     `json:"limit"`
	Offset         int32     `json:"offset"`
	OutputFields   []string  `json:"outputFields"`
	Vector         []float32 `json:"vector"`
}

func getPrimaryField(schema *schemapb.CollectionSchema) *schemapb.FieldSchema {
	primaryField := schema.Fields[0]
	for _, field := range schema.Fields {
		if field.IsPrimaryKey {
			primaryField = field
			break
		}
	}
	return primaryField
}

func rowToEntityRowArray(data []map[string]interface{}) []entity.Row {
	entityRows := []entity.Row{}
	for _, row := range data {
		entityRows = append(entityRows, entity.MapRow(row))
	}
	return entityRows
}

func convertData(data interface{}, dataType string) (interface{}, error) {
	switch dataType {
	case Bool:
		return cast.ToBoolE(data)
	case Int8:
		return cast.ToInt8E(data)
	case Int16:
		return cast.ToInt16E(data)
	case Int32:
		return cast.ToInt32E(data)
	case Int64:
		return cast.ToInt64E(data)
	case Float32:
		return cast.ToFloat32E(data)
	case Float64:
		return cast.ToFloat64E(data)
	case String:
		return cast.ToStringE(data)
	case Varchar:
		return cast.ToStringE(data)
	case InnerFloatVector:
		// 判断输入数据是否为 []interface{} 类型
		arr, ok := data.([]interface{})
		if ok {
			var floatArr []float32
			for _, fieldData := range arr {
				floatArr = append(floatArr, cast.ToFloat32(fieldData))
			}
			return floatArr, nil
		}

		value, ok := data.([]float32)
		if ok {
			return value, nil
		}

		return nil, fmt.Errorf("input data is not of type []interface{}")
	default:
		return nil, fmt.Errorf("unsupported data type: %v", dataType)
	}
}

func vector2Placeholder(vectors []entity.Vector) *commonpb.PlaceholderValue {
	var placeHolderType commonpb.PlaceholderType
	ph := &commonpb.PlaceholderValue{
		Tag:    "$0",
		Values: make([][]byte, 0, len(vectors)),
	}
	if len(vectors) == 0 {
		return ph
	}
	switch vectors[0].(type) {
	case entity.FloatVector:
		placeHolderType = commonpb.PlaceholderType_FloatVector
	case entity.BinaryVector:
		placeHolderType = commonpb.PlaceholderType_BinaryVector
	}
	ph.Type = placeHolderType
	for _, vector := range vectors {
		ph.Values = append(ph.Values, vector.Serialize())
	}
	return ph
}

func vector2PlaceholderGroupBytes(vectors []entity.Vector) []byte {
	phg := &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			vector2Placeholder(vectors),
		},
	}

	bs, _ := proto.Marshal(phg)
	return bs
}

func convertKeyValuaPairToMap(pairs []*commonpb.KeyValuePair) map[string]string {
	res := map[string]string{}
	for _, pair := range pairs {
		res[pair.Key] = pair.Value
	}
	return res
}

func getDynamicOutputFields(schema *schemapb.CollectionSchema, outputFields []string) []string {
	fieldNames := []string{}
	for _, field := range schema.Fields {
		if field.IsPrimaryKey && !containsString(outputFields, field.Name) {
			outputFields = append(outputFields, field.Name)
		}
		fieldNames = append(fieldNames, field.Name)
	}
	if schema.EnableDynamicField {
		return getNotExistData(outputFields, fieldNames)
	} else {
		return outputFields
	}
}

func getNotExistData(arrayA []string, allArray []string) []string {
	// 使用一个 map 来记录数组 B 中的所有元素
	arrayBMap := make(map[string]bool)
	for _, value := range allArray {
		arrayBMap[value] = true
	}

	// 遍历数组 A，检查每个元素是否在该 map 中出现过
	resultArray := []string{}
	for _, value := range arrayA {
		if _, ok := arrayBMap[value]; !ok {
			resultArray = append(resultArray, value)
		}
	}

	return resultArray
}

func PKs2Expr(backName string, ids entity.Column) string {
	var expr string
	var pkName = ids.Name()
	if ids.Name() == "" {
		pkName = backName
	}
	switch ids.Type() {
	case entity.FieldTypeInt64:
		expr = fmt.Sprintf("%s in %s", pkName, strings.Join(strings.Fields(fmt.Sprint(ids.FieldData().GetScalars().GetLongData().GetData())), ","))
	case entity.FieldTypeVarChar:
		data := ids.FieldData().GetScalars().GetData().(*schemapb.ScalarField_StringData).StringData.Data
		for i := range data {
			data[i] = fmt.Sprintf("\"%s\"", data[i])
		}
		expr = fmt.Sprintf("%s in %s", pkName, strings.Join(strings.Fields(fmt.Sprint(data)), ","))
	}
	return expr
}

func convertRange(field *schemapb.FieldSchema, result gjson.Result) (string, error) {
	var resultStr string
	fieldType := field.DataType

	if fieldType == schemapb.DataType_Int64 {
		dataArray := []int64{}
		for _, data := range result.Array() {
			if data.Type == gjson.String {
				value, err := cast.ToInt64E(data.Str)
				if err != nil {
					return "", err
				}
				dataArray = append(dataArray, value)
			} else {
				value, err := cast.ToInt64E(data.Raw)
				if err != nil {
					return "", err
				}
				dataArray = append(dataArray, value)
			}
		}
		resultStr = JoinArray(dataArray)
	} else if fieldType == schemapb.DataType_VarChar {
		dataArray := []string{}
		for _, data := range result.Array() {
			value, err := cast.ToStringE(data.Str)
			if err != nil {
				return "", err
			}
			dataArray = append(dataArray, value)
		}
		resultStr = JoinArray(dataArray)
	}
	return resultStr, nil
}

func convertStringArrayToTypeArray(strArr []string, dataType schemapb.DataType) interface{} {
	switch dataType {
	case schemapb.DataType_Int8:
		result := make([]int8, len(strArr))
		for i, str := range strArr {
			result[i] = cast.ToInt8(str)
		}
		return result
	case schemapb.DataType_Int16:
		result := make([]int16, len(strArr))
		for i, str := range strArr {
			result[i] = cast.ToInt16(str)
		}
		return result
	case schemapb.DataType_Int32:
		result := make([]int32, len(strArr))
		for i, str := range strArr {
			result[i] = cast.ToInt32(str)
		}
		return result
	case schemapb.DataType_Int64:
		result := make([]int64, len(strArr))
		for i, str := range strArr {
			result[i] = cast.ToInt64(str)
		}
		return result
	case schemapb.DataType_VarChar:
		result := strArr
		return result
	default:
		return nil
	}
}

func JoinArray(data interface{}) string {
	var buffer bytes.Buffer
	arr := reflect.ValueOf(data)

	for i := 0; i < arr.Len(); i++ {
		if i > 0 {
			buffer.WriteString(",")
		}

		buffer.WriteString(fmt.Sprintf("%v", arr.Index(i)))
	}

	return buffer.String()
}

func checkGetPrimaryKey(coll *milvuspb.DescribeCollectionResponse, idResult gjson.Result) string {
	primaryField := getPrimaryField(coll.Schema)
	resultStr, _ := convertRange(primaryField, idResult)
	filter := primaryField.Name + " in [" + resultStr + "]"
	return filter
}

func containsString(arr []string, s string) bool {
	for _, str := range arr {
		if str == s {
			return true
		}
	}
	return false
}

func checkAndSetData(body string, collDescResp *milvuspb.DescribeCollectionResponse, req *InsertReq) error {
	reallyDataArray := []map[string]interface{}{}
	dataResult := gjson.Get(body, "data")
	dataResultArray := dataResult.Array()

	fieldNames := []string{}
	for _, field := range collDescResp.Schema.Fields {
		fieldNames = append(fieldNames, field.Name)
	}

	for _, data := range dataResultArray {
		reallyData := map[string]interface{}{}
		vectorArray := []float32{}
		if data.Type == gjson.JSON {
			for _, field := range collDescResp.Schema.Fields {
				fieldType := field.DataType
				fieldName := field.Name

				dataString := gjson.Get(data.Raw, fieldName).String()

				// 如果AutoId 则报错
				if field.IsPrimaryKey && collDescResp.Schema.AutoID {
					if dataString != "" {
						return errors.New(fmt.Sprintf("[checkAndSetData] fieldName %s AutoId already open, not support insert data %s", fieldName, dataString))
					} else {
						continue
					}
				}

				// 转换对应数据,同时检查数据合法性
				if fieldType == schemapb.DataType_FloatVector || fieldType == schemapb.DataType_BinaryVector {
					for _, vector := range gjson.Get(data.Raw, fieldName).Array() {
						vectorArray = append(vectorArray, cast.ToFloat32(vector.Num))
					}
					reallyData[fieldName] = vectorArray
				} else if fieldType == schemapb.DataType_Int8 {
					result, err := cast.ToInt8E(dataString)
					if err != nil {
						return errors.New(fmt.Sprintf("[checkAndSetData] dataString %s cast to int8 error: %s", dataString, err.Error()))
					}
					reallyData[fieldName] = result
				} else if fieldType == schemapb.DataType_Int16 {
					result, err := cast.ToInt16E(dataString)
					if err != nil {
						return errors.New(fmt.Sprintf("[checkAndSetData] dataString %s cast to int16 error: %s", dataString, err.Error()))
					}
					reallyData[fieldName] = result
				} else if fieldType == schemapb.DataType_Int32 {
					result, err := cast.ToInt32E(dataString)
					if err != nil {
						return errors.New(fmt.Sprintf("[checkAndSetData] dataString %s cast to int32 error: %s", dataString, err.Error()))
					}
					reallyData[fieldName] = result
				} else if fieldType == schemapb.DataType_Int64 {
					result, err := cast.ToInt64E(dataString)
					if err != nil {
						return errors.New(fmt.Sprintf("[checkAndSetData] dataString %s cast to int64 error: %s", dataString, err.Error()))
					}
					reallyData[fieldName] = result
				} else if fieldType == schemapb.DataType_VarChar || fieldType == schemapb.DataType_String {
					reallyData[fieldName] = dataString
				} else if fieldType == schemapb.DataType_Float {
					result, err := cast.ToFloat64E(dataString)
					if err != nil {
						return errors.New(fmt.Sprintf("[checkAndSetData] dataString %s cast to float64 error: %s", dataString, err.Error()))
					}
					reallyData[fieldName] = result
				} else if fieldType == schemapb.DataType_Bool {
					result, err := cast.ToBoolE(dataString)
					if err != nil {
						return errors.New(fmt.Sprintf("[checkAndSetData] dataString %s cast to bool error: %s", dataString, err.Error()))
					}
					reallyData[fieldName] = result
				} else {
					errMsg := fmt.Sprintf("[checkAndSetData] not support fieldName %s dataType %s", fieldName, fieldType)
					return errors.New(errMsg)
				}
			}

			// fill dynamic schema
			if collDescResp.Schema.EnableDynamicField {
				for mapKey, mapValue := range data.Map() {
					if !containsString(fieldNames, mapKey) {
						mapValueStr := mapValue.String()
						if mapValue.Type == gjson.True || mapValue.Type == gjson.False {
							reallyData[mapKey] = cast.ToBool(mapValueStr)
						} else if mapValue.Type == gjson.String {
							reallyData[mapKey] = mapValueStr
						} else if mapValue.Type == gjson.Number {
							if strings.Contains(mapValue.Raw, ".") {
								reallyData[mapKey] = cast.ToFloat64(mapValue.Raw)
							} else {
								reallyData[mapKey] = cast.ToInt64(mapValueStr)
							}
						} else if mapValue.Type == gjson.JSON {
							reallyData[mapKey] = mapValue.Value()
						} else {

						}
					}
				}
			}

			reallyDataArray = append(reallyDataArray, reallyData)
		} else {
			errMsg := fmt.Sprintf("[checkAndSetData] dataType %s not Json", data.Type)
			return errors.New(errMsg)
		}
	}
	req.Data = reallyDataArray
	return nil
}

func buildQueryResp(dynamicOutputFields []string, fieldDataList []*schemapb.FieldData, scores []float32) ([]map[string]interface{}, error) {
	from := 0
	to := -1
	fields := make(map[string]*schemapb.FieldData)
	var dynamicColumn *entity.ColumnJSONBytes
	for _, fieldData := range fieldDataList {
		fields[fieldData.GetFieldName()] = fieldData
		if fieldData.GetIsDynamic() {
			column, err := entity.FieldDataColumn(fieldData, from, to)
			if err != nil {
				return nil, err
			}
			var ok bool
			dynamicColumn, ok = column.(*entity.ColumnJSONBytes)
			if !ok {
				return nil, errors.New("dynamic field not json")
			}
		}
	}
	columns := make([]entity.Column, 0, len(dynamicOutputFields))
	for _, outputField := range dynamicOutputFields {
		fieldData, ok := fields[outputField]
		var column entity.Column
		var err error
		if !ok {
			if dynamicColumn == nil {
				return nil, errors.New("output fields not match and result field data does not contain dynamic field")
			}
			column = entity.NewColumnDynamic(dynamicColumn, outputField)
		} else {
			column, err = entity.FieldDataColumn(fieldData, from, to)
		}
		if err != nil {
			return nil, err
		}
		columns = append(columns, column)
	}

	querysResp := []map[string]interface{}{}

	columnNum := len(columns)
	columnDataNum := columns[0].Len()
	if scores != nil && columnDataNum != len(scores) {
		return nil, errors.New("search result invalid")
	}
	for i := 0; i < columnDataNum; i++ {
		row := map[string]interface{}{}
		for j := 0; j < columnNum; j++ {
			dataType := columns[j].Type()
			if dataType == entity.FieldTypeBool {
				row[columns[j].Name()] = columns[j].(*entity.ColumnBool).Data()[i]
			} else if dataType == entity.FieldTypeInt8 {
				row[columns[j].Name()] = columns[j].(*entity.ColumnInt8).Data()[i]
			} else if dataType == entity.FieldTypeInt16 {
				row[columns[j].Name()] = columns[j].(*entity.ColumnInt16).Data()[i]
			} else if dataType == entity.FieldTypeInt32 {
				row[columns[j].Name()] = columns[j].(*entity.ColumnInt32).Data()[i]
			} else if dataType == entity.FieldTypeInt64 {
				row[columns[j].Name()] = columns[j].(*entity.ColumnInt64).Data()[i]
			} else if dataType == entity.FieldTypeFloat {
				row[columns[j].Name()] = columns[j].(*entity.ColumnFloat).Data()[i]
			} else if dataType == entity.FieldTypeDouble {
				row[columns[j].Name()] = columns[j].(*entity.ColumnDouble).Data()[i]
			} else if dataType == entity.FieldTypeVarChar {
				row[columns[j].Name()] = columns[j].(*entity.ColumnVarChar).Data()[i]
			} else if dataType == entity.FieldTypeBinaryVector {
				row[columns[j].Name()] = columns[j].(*entity.ColumnBinaryVector).Data()[i]
			} else if dataType == entity.FieldTypeFloatVector {
				row[columns[j].Name()] = columns[j].(*entity.ColumnFloatVector).Data()[i]
			} else if dataType == entity.FieldTypeJSON {
				data, ok := columns[j].(*entity.ColumnJSONBytes)
				if ok && !data.FieldData().IsDynamic {
					row[columns[j].Name()] = string(data.Data()[i])
				} else {
					var dataMap map[string]interface{}
					var byteValue []byte
					if ok {
						jsonData, _ := columns[j].(*entity.ColumnJSONBytes)
						byteValue = jsonData.Data()[i]
					} else {
						jsonData, _ := columns[j].(*entity.ColumnDynamic)
						byteValue = jsonData.Data()[i]
					}

					err := json.Unmarshal(byteValue, &dataMap)
					if err != nil {
						msg := fmt.Sprintf("[BuildQueryResp] Unmarshal error %s", err.Error())
						log.Error(msg)
						return nil, err
					}

					if containsString(dynamicOutputFields, "*") || containsString(dynamicOutputFields, "$meta") {
						for key, value := range dataMap {
							row[key] = value
						}
					} else {
						// 针对动态列需要拆分其中的数据
						for _, dynamicField := range dynamicOutputFields {
							row[dynamicField] = dataMap[dynamicField]
						}
					}

				}

			}
		}
		if scores != nil {
			row["distance"] = scores[i]
		}
		querysResp = append(querysResp, row)
	}

	return querysResp, nil
}
