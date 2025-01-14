package client

import (
	"github.com/milvus-io/milvus/internal/streamingcoord/client/assignment"
	"github.com/milvus-io/milvus/internal/streamingcoord/client/broadcast"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/resolver"
)

// clientImpl is the implementation of Client.
type clientImpl struct {
	conn              lazygrpc.Conn
	rb                resolver.Builder
	assignmentService *assignment.AssignmentServiceImpl
	broadcastService  *broadcast.BroadcastServiceImpl
}

func (c *clientImpl) Broadcast() BroadcastService {
	return c.broadcastService
}

// Assignment access assignment service.
func (c *clientImpl) Assignment() AssignmentService {
	return c.assignmentService
}

// Close close the client.
func (c *clientImpl) Close() {
	if c.assignmentService != nil {
		c.assignmentService.Close()
	}
	c.conn.Close()
	c.rb.Close()
}
