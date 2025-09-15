package querycoordv2

import "github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"

// RegisterDDLCallbacks registers the ddl callbacks.
func RegisterDDLCallbacks(s *Server) {
	ddlCallback := &DDLCallbacks{
		Server: s,
	}
	ddlCallback.registerResourceGroupCallbacks()
}

type DDLCallbacks struct {
	*Server
}

func (c *DDLCallbacks) registerResourceGroupCallbacks() {
	registry.RegisterAlterResourceGroupV2AckCallback(c.putResourceGroupV2AckCallback)
	registry.RegisterDropResourceGroupV2AckCallback(c.dropResourceGroupV2AckCallback)
}

func (c *DDLCallbacks) RegisterDDLCallbacks() {
	c.registerResourceGroupCallbacks()
}
