package client

import (
	"github.com/milvus-io/milvus/internal/streamingcoord/client/assignment"
	"github.com/milvus-io/milvus/internal/streamingcoord/client/broadcast"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/service/lazygrpc"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/service/resolver"
)

// clientImpl is the implementation of Client.
type clientImpl struct {
	conn              lazygrpc.Conn
	rb                resolver.Builder
	assignmentService *assignment.AssignmentServiceImpl
	broadcastService  *broadcast.GRPCBroadcastServiceImpl
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
