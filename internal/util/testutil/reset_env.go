package testutil

import (
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
)

func ResetEnvironment() {
	channel.ResetStaticPChannelStatsManager()
	registry.ResetRegistration()
}
