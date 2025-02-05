package registry

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/syncutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var (
	registry                   = syncutil.NewFuture[WALManager]()
	ErrNoStreamingNodeDeployed = errors.New("no streaming node deployed")
)

// RegisterLocalWALManager registers the local wal manager.
// When the streaming node is started, it should call this function to register the wal manager.
func RegisterLocalWALManager(manager WALManager) {
	if !paramtable.IsLocalComponentEnabled(typeutil.StreamingNodeRole) {
		panic("unreachable: streaming node is not enabled but wal setup")
	}
	registry.Set(manager)
	log.Ctx(context.Background()).Info("register local wal manager done")
}

// GetAvailableWAL returns a available wal instance for the channel.
func GetAvailableWAL(channel types.PChannelInfo) (wal.WAL, error) {
	if !paramtable.IsLocalComponentEnabled(typeutil.StreamingNodeRole) {
		return nil, ErrNoStreamingNodeDeployed
	}
	return registry.Get().GetAvailableWAL(channel)
}

// WALManager is a hint type for wal manager at streaming node.
type WALManager interface {
	// GetAvailableWAL returns a available wal instance for the channel.
	// Return nil if the wal instance is not found.
	GetAvailableWAL(channel types.PChannelInfo) (wal.WAL, error)
}
