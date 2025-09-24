package testutil

import (
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
	registry2 "github.com/milvus-io/milvus/internal/streamingnode/client/handler/registry"
)

func ResetEnvironment() {
	channel.ResetStaticPChannelStatsManager()
	registry.ResetRegistration()
	snmanager.ResetStreamingNodeManager()
	registry2.ResetRegisterLocalWALManager()
	broadcast.ResetBroadcaster()
}
