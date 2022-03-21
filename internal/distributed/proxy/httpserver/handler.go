package httpserver

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
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
	router.POST("/dummy", wrapHandler(h.handlePostDummy))

	router.POST("/entities", wrapHandler(h.handleInsert))
	router.DELETE("/entities", wrapHandler(h.handleDelete))
	router.POST("/search", wrapHandler(h.handleSearch))
	router.POST("/query", wrapHandler(h.handleQuery))

	router.POST("/persist", wrapHandler(h.handleFlush))
	router.GET("/distance", wrapHandler(h.handleCalcDistance))
}

func (h *Handlers) handleGetHealth(c *gin.Context) (interface{}, error) {
	return gin.H{"status": "ok"}, nil
}

func (h *Handlers) handlePostDummy(c *gin.Context) (interface{}, error) {
	req := milvuspb.DummyRequest{}
	// use ShouldBind to supports binding JSON, XML, YAML, and protobuf.
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.Dummy(c, &req)
}

func (h *Handlers) handleInsert(c *gin.Context) (interface{}, error) {
	req := milvuspb.InsertRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.Insert(c, &req)
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
	req := milvuspb.SearchRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
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
	req := milvuspb.CalcDistanceRequest{}
	err := shouldBind(c, &req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse body failed: %v", errBadRequest, err)
	}
	return h.proxy.CalcDistance(c, &req)
}
