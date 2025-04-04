package testutil

import (
	"github.com/milvus-io/milvus/internal/coordinator/coordclient"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
)

func ResetEnvironment() {
	coordclient.ResetRegistration()
	channel.ResetStaticPChannelStatsManager()
	registry.ResetRegistration()
}
