package client

import (
	"github.com/milvus-io/milvus/internal/streamingcoord/client/assignment"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
)

// clientImpl is the implementation of Client.
type clientImpl struct {
	conn              lazygrpc.Conn
	rb                resolver.Builder
	assignmentService *assignment.AssignmentServiceImpl
}

// Assignment access assignment service.
func (c *clientImpl) Assignment() AssignmentService {
	return c.assignmentService
}

// Close close the client.
func (c *clientImpl) Close() {
	c.assignmentService.Close()
	c.conn.Close()
	c.rb.Close()
}
