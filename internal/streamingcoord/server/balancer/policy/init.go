package vchannelfair

import (
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/policy/vchannelfair"
)

func init() {
	balancer.RegisterPolicy(&vchannelfair.PolicyBuilder{})
}
