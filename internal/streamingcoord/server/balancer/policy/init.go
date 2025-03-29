package policy

import "github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"

func init() {
	balancer.RegisterPolicy(&vchannelFairPolicyBuilder{})
}
