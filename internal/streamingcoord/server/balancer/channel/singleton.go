package channel

import (
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

var singleton = syncutil.NewFuture[*ChannelManager]()

// Register sets the global ChannelManager singleton.
func register(cm *ChannelManager) {
	singleton.Set(cm)
}

// GetClusterChannels blocks until the ChannelManager is registered,
// then returns the cluster channel topology.
func GetClusterChannels() message.ClusterChannels {
	return singleton.Get().getClusterChannels()
}
