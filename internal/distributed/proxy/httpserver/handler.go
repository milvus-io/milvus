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
}

func (h *Handlers) handleGetHealth(c *gin.Context) (interface{}, error) {
	return gin.H{"status": "ok"}, nil
}

func (h *Handlers) handlePostDummy(c *gin.Context) (interface{}, error) {
	req := milvuspb.DummyRequest{}
	// use ShouldBind to supports binding JSON, XML, YAML, and protobuf.
	err := c.ShouldBind(&req)
	if err != nil {
		return nil, fmt.Errorf("%w: parse json failed: %v", errBadRequest, err)
	}
	return h.proxy.Dummy(c, &req)
}
