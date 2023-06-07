package httpserver

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
)

// Handlers handles http requests
type Handlers struct {
	proxy types.ProxyComponent
}

// NewHandlers creates a new Handlers
func NewHandlers(proxy types.ProxyComponent) *Handlers {
	return &Handlers{
		proxy: proxy,
	}
}

// RegisterRouters registers routes to given router
func (h *Handlers) RegisterRoutesTo(router gin.IRouter) {
	router.GET("/health", wrapHandler(h.handleGetHealth))
	router.POST("/dummy", wrapHandler(h.handleDummy))

	router.POST("/collection", wrapHandler(h.handleCreateCollection))
	router.DELETE("/collection", wrapHandler(h.handleDropCollection))
	router.GET("/collection/existence", wrapHandler(h.handleHasCollection))
	router.GET("/collection", wrapHandler(h.handleDescribeCollection))
	router.POST("/collection/load", wrapHandler(h.handleLoadCollection))
	router.DELETE("/collection/load", wrapHandler(h.handleReleaseCollection))
	router.GET("/collection/statistics", wrapHandler(h.handleGetCollectionStatistics))
	router.GET("/collections", wrapHandler(h.handleShowCollections))

	router.POST("/partition", wrapHandler(h.handleCreatePartition))
	router.DELETE("/partition", wrapHandler(h.handleDropPartition))
	router.GET("/partition/existence", wrapHandler(h.handleHasPartition))
	router.POST("/partitions/load", wrapHandler(h.handleLoadPartitions))
	router.DELETE("/partitions/load", wrapHandler(h.handleReleasePartitions))
	router.GET("/partition/statistics", wrapHandler(h.handleGetPartitionStatistics))
	router.GET("/partitions", wrapHandler(h.handleShowPartitions))

	router.POST("/alias", wrapHandler(h.handleCreateAlias))
	router.DELETE("/alias", wrapHandler(h.handleDropAlias))
	router.PATCH("/alias", wrapHandler(h.handleAlterAlias))

	router.POST("/index", wrapHandler(h.handleCreateIndex))
	router.GET("/index", wrapHandler(h.handleDescribeIndex))
	router.GET("/index/state", wrapHandler(h.handleGetIndexState))
	router.GET("/index/progress", wrapHandler(h.handleGetIndexBuildProgress))
	router.DELETE("/index", wrapHandler(h.handleDropIndex))

	router.POST("/entities", wrapHandler(h.handleInsert))
	router.DELETE("/entities", wrapHandler(h.handleDelete))
	router.POST("/search", wrapHandler(h.handleSearch))
	router.POST("/query", wrapHandler(h.handleQuery))

	router.POST("/persist", wrapHandler(h.handleFlush))
	router.GET("/distance", wrapHandler(h.handleCalcDistance))
	router.GET("/persist/state", wrapHandler(h.handleGetFlushState))
	router.GET("/persist/segment-info", wrapHandler(h.handleGetPersistentSegmentInfo))
	router.GET("/query-segment-info", wrapHandler(h.handleGetQuerySegmentInfo))
	router.GET("/replicas", wrapHandler(h.handleGetReplicas))

	router.GET("/metrics", wrapHandler(h.handleGetMetrics))
	router.POST("/load-balance", wrapHandler(h.handleLoadBalance))
	router.GET("/compaction/state", wrapHandler(h.handleGetCompactionState))
	router.GET("/compaction/plans", wrapHandler(h.handleGetCompactionStateWithPlans))
	router.POST("/compaction", wrapHandler(h.handleManualCompaction))

	router.POST("/import", wrapHandler(h.handleImport))
	router.GET("/import/state", wrapHandler(h.handleGetImportState))
	router.GET("/import/tasks", wrapHandler(h.handleListImportTasks))

	router.POST("/credential", wrapHandler(h.handleCreateCredential))
	router.PATCH("/credential", wrapHandler(h.handleUpdateCredential))
	router.DELETE("/credential", wrapHandler(h.handleDeleteCredential))
	router.GET("/credential/users", wrapHandler(h.handleListCredUsers))

}

func (h *Handlers) handleGetHealth(c *gin.Context) (interface{}, error) {
	return gin.H{"status": "ok"}, nil
}

func (h *Handlers) handleDummy(c *gin.Context) (interface{}, error) {
	req := milvuspb.DummyRequest{}
	// use ShouldBind to supports binding JSON, XML, YAML, and protobuf.
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.Dummy(c, &req)
}

func (h *Handlers) handleCreateCollection(c *gin.Context) (interface{}, error) {
	wrappedReq := WrappedCreateCollectionRequest{}
	err := shouldBind(c, &wrappedReq)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	schemaProto, err := proto.Marshal(&wrappedReq.Schema)
	if err != nil {
		return nil, fmt.Errorf("%w: marshal schema failed: %v", errBadRequest, err)
	}
	req := &milvuspb.CreateCollectionRequest{
		Base:             wrappedReq.Base,
		DbName:           wrappedReq.DbName,
		CollectionName:   wrappedReq.CollectionName,
		Schema:           schemaProto,
		ShardsNum:        wrappedReq.ShardsNum,
		ConsistencyLevel: wrappedReq.ConsistencyLevel,
		Properties:       wrappedReq.Properties,
	}
	return h.proxy.CreateCollection(c, req)
}

func (h *Handlers) handleDropCollection(c *gin.Context) (interface{}, error) {
	req := milvuspb.DropCollectionRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.DropCollection(c, &req)
}

func (h *Handlers) handleHasCollection(c *gin.Context) (interface{}, error) {
	req := milvuspb.HasCollectionRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.HasCollection(c, &req)
}

func (h *Handlers) handleDescribeCollection(c *gin.Context) (interface{}, error) {
	req := milvuspb.DescribeCollectionRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.DescribeCollection(c, &req)
}

func (h *Handlers) handleLoadCollection(c *gin.Context) (interface{}, error) {
	req := milvuspb.LoadCollectionRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.LoadCollection(c, &req)
}

func (h *Handlers) handleReleaseCollection(c *gin.Context) (interface{}, error) {
	req := milvuspb.ReleaseCollectionRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.ReleaseCollection(c, &req)
}

func (h *Handlers) handleGetCollectionStatistics(c *gin.Context) (interface{}, error) {
	req := milvuspb.GetCollectionStatisticsRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.GetCollectionStatistics(c, &req)
}

func (h *Handlers) handleShowCollections(c *gin.Context) (interface{}, error) {
	req := milvuspb.ShowCollectionsRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.ShowCollections(c, &req)
}

func (h *Handlers) handleCreatePartition(c *gin.Context) (interface{}, error) {
	req := milvuspb.CreatePartitionRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.CreatePartition(c, &req)
}

func (h *Handlers) handleDropPartition(c *gin.Context) (interface{}, error) {
	req := milvuspb.DropPartitionRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.DropPartition(c, &req)
}

func (h *Handlers) handleHasPartition(c *gin.Context) (interface{}, error) {
	req := milvuspb.HasPartitionRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.HasPartition(c, &req)
}

func (h *Handlers) handleLoadPartitions(c *gin.Context) (interface{}, error) {
	req := milvuspb.LoadPartitionsRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.LoadPartitions(c, &req)
}

func (h *Handlers) handleReleasePartitions(c *gin.Context) (interface{}, error) {
	req := milvuspb.ReleasePartitionsRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.ReleasePartitions(c, &req)
}

func (h *Handlers) handleGetPartitionStatistics(c *gin.Context) (interface{}, error) {
	req := milvuspb.GetPartitionStatisticsRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.GetPartitionStatistics(c, &req)
}

func (h *Handlers) handleShowPartitions(c *gin.Context) (interface{}, error) {
	req := milvuspb.ShowPartitionsRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.ShowPartitions(c, &req)
}

func (h *Handlers) handleCreateAlias(c *gin.Context) (interface{}, error) {
	req := milvuspb.CreateAliasRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.CreateAlias(c, &req)
}

func (h *Handlers) handleDropAlias(c *gin.Context) (interface{}, error) {
	req := milvuspb.DropAliasRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.DropAlias(c, &req)
}

func (h *Handlers) handleAlterAlias(c *gin.Context) (interface{}, error) {
	req := milvuspb.AlterAliasRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.AlterAlias(c, &req)
}

func (h *Handlers) handleCreateIndex(c *gin.Context) (interface{}, error) {
	req := milvuspb.CreateIndexRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.CreateIndex(c, &req)
}

func (h *Handlers) handleDescribeIndex(c *gin.Context) (interface{}, error) {
	req := milvuspb.DescribeIndexRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.DescribeIndex(c, &req)
}

func (h *Handlers) handleGetIndexState(c *gin.Context) (interface{}, error) {
	req := milvuspb.GetIndexStateRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.GetIndexState(c, &req)
}

func (h *Handlers) handleGetIndexBuildProgress(c *gin.Context) (interface{}, error) {
	req := milvuspb.GetIndexBuildProgressRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.GetIndexBuildProgress(c, &req)
}

func (h *Handlers) handleDropIndex(c *gin.Context) (interface{}, error) {
	req := milvuspb.DropIndexRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.DropIndex(c, &req)
}

func (h *Handlers) handleInsert(c *gin.Context) (interface{}, error) {
	wrappedReq := WrappedInsertRequest{}
	err := shouldBind(c, &wrappedReq)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	req, err := wrappedReq.AsInsertRequest()
	if err != nil {
		return nil, fmt.Errorf("%w: convert body to pb failed: %v", errBadRequest, err)
	}
	return h.proxy.Insert(c, req)
}

func (h *Handlers) handleDelete(c *gin.Context) (interface{}, error) {
	req := milvuspb.DeleteRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.Delete(c, &req)
}

func (h *Handlers) handleSearch(c *gin.Context) (interface{}, error) {
	wrappedReq := SearchRequest{}
	err := shouldBind(c, &wrappedReq)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	req := milvuspb.SearchRequest{
		Base:               wrappedReq.Base,
		DbName:             wrappedReq.DbName,
		CollectionName:     wrappedReq.CollectionName,
		PartitionNames:     wrappedReq.PartitionNames,
		Dsl:                wrappedReq.Dsl,
		DslType:            wrappedReq.DslType,
		OutputFields:       wrappedReq.OutputFields,
		SearchParams:       wrappedReq.SearchParams,
		TravelTimestamp:    wrappedReq.TravelTimestamp,
		GuaranteeTimestamp: wrappedReq.GuaranteeTimestamp,
		Nq:                 wrappedReq.Nq,
	}
	if len(wrappedReq.BinaryVectors) > 0 {
		req.PlaceholderGroup = binaryVector2Bytes(wrappedReq.BinaryVectors)
	} else {
		req.PlaceholderGroup = vector2Bytes(wrappedReq.Vectors)
	}
	return h.proxy.Search(c, &req)
}

func (h *Handlers) handleQuery(c *gin.Context) (interface{}, error) {
	req := milvuspb.QueryRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.Query(c, &req)
}

func (h *Handlers) handleFlush(c *gin.Context) (interface{}, error) {
	req := milvuspb.FlushRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.Flush(c, &req)
}

func (h *Handlers) handleCalcDistance(c *gin.Context) (interface{}, error) {
	wrappedReq := WrappedCalcDistanceRequest{}
	err := shouldBind(c, &wrappedReq)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}

	req := milvuspb.CalcDistanceRequest{
		Base:    wrappedReq.Base,
		Params:  wrappedReq.Params,
		OpLeft:  wrappedReq.OpLeft.AsPbVectorArray(),
		OpRight: wrappedReq.OpRight.AsPbVectorArray(),
	}
	return h.proxy.CalcDistance(c, &req)
}

func (h *Handlers) handleGetFlushState(c *gin.Context) (interface{}, error) {
	req := milvuspb.GetFlushStateRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.GetFlushState(c, &req)
}

func (h *Handlers) handleGetPersistentSegmentInfo(c *gin.Context) (interface{}, error) {
	req := milvuspb.GetPersistentSegmentInfoRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.GetPersistentSegmentInfo(c, &req)
}

func (h *Handlers) handleGetQuerySegmentInfo(c *gin.Context) (interface{}, error) {
	req := milvuspb.GetQuerySegmentInfoRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.GetQuerySegmentInfo(c, &req)
}

func (h *Handlers) handleGetReplicas(c *gin.Context) (interface{}, error) {
	req := milvuspb.GetReplicasRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.GetReplicas(c, &req)
}

func (h *Handlers) handleGetMetrics(c *gin.Context) (interface{}, error) {
	req := milvuspb.GetMetricsRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.GetMetrics(c, &req)
}

func (h *Handlers) handleLoadBalance(c *gin.Context) (interface{}, error) {
	req := milvuspb.LoadBalanceRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.LoadBalance(c, &req)
}

func (h *Handlers) handleGetCompactionState(c *gin.Context) (interface{}, error) {
	req := milvuspb.GetCompactionStateRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.GetCompactionState(c, &req)
}

func (h *Handlers) handleGetCompactionStateWithPlans(c *gin.Context) (interface{}, error) {
	req := milvuspb.GetCompactionPlansRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.GetCompactionStateWithPlans(c, &req)
}

func (h *Handlers) handleManualCompaction(c *gin.Context) (interface{}, error) {
	req := milvuspb.ManualCompactionRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.ManualCompaction(c, &req)
}

func (h *Handlers) handleImport(c *gin.Context) (interface{}, error) {
	req := milvuspb.ImportRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.Import(c, &req)
}

func (h *Handlers) handleGetImportState(c *gin.Context) (interface{}, error) {
	req := milvuspb.GetImportStateRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.GetImportState(c, &req)
}

func (h *Handlers) handleListImportTasks(c *gin.Context) (interface{}, error) {
	req := milvuspb.ListImportTasksRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.ListImportTasks(c, &req)
}

func (h *Handlers) handleCreateCredential(c *gin.Context) (interface{}, error) {
	req := milvuspb.CreateCredentialRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.CreateCredential(c, &req)
}

func (h *Handlers) handleUpdateCredential(c *gin.Context) (interface{}, error) {
	req := milvuspb.UpdateCredentialRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.UpdateCredential(c, &req)
}

func (h *Handlers) handleDeleteCredential(c *gin.Context) (interface{}, error) {
	req := milvuspb.DeleteCredentialRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.DeleteCredential(c, &req)
}

func (h *Handlers) handleListCredUsers(c *gin.Context) (interface{}, error) {
	req := milvuspb.ListCredUsersRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.ListCredUsers(c, &req)
}
