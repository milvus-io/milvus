package walmanager

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
)

var _ Manager = (*managerImpl)(nil)

// Manager is the interface for managing the wal instances.
type Manager interface {
	// Open opens a wal instance for the channel on this Manager.
	Open(ctx context.Context, channel types.PChannelInfo) error

	// GetAvailableWAL returns a available wal instance for the channel.
	// Return nil if the wal instance is not found.
	GetAvailableWAL(channelName string, term int64) (wal.WAL, error)

	// GetAllAvailableWALInfo returns all available channel info.
	GetAllAvailableChannels() ([]types.PChannelInfo, error)

	// Remove removes the wal instance for the channel.
	Remove(ctx context.Context, channel types.PChannelInfo) error

	// Close these manager and release all managed WAL.
	Close()
}
