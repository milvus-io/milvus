package balancer

import "github.com/milvus-io/milvus/internal/views/coord/balancer/api"

type (
	BalanceNode   = api.BalanceNode
	BalanceConfig = api.BalanceConfig
)

func DefaultBalanceConfig() *BalanceConfig { return api.DefaultBalanceConfig() }
