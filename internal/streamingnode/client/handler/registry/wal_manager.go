package registry

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
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

// GetLocalAvailableWAL returns a available wal instance for the channel.
func GetLocalAvailableWAL(channel types.PChannelInfo) (wal.WAL, error) {
	if !paramtable.IsLocalComponentEnabled(typeutil.StreamingNodeRole) {
		return nil, ErrNoStreamingNodeDeployed
	}
	l, err := registry.Get().GetAvailableWAL(channel)
	if err != nil {
		return nil, err
	}
	return localWAL{l}, nil // because the appended message object may be reused, make some difference between remote wal and local wal.
	// so make a copy before appending for local wal to keep the consistency.
}

// WALManager is a hint type for wal manager at streaming node.
type WALManager interface {
	// GetAvailableWAL returns a available wal instance for the channel.
	// Return nil if the wal instance is not found.
	GetAvailableWAL(channel types.PChannelInfo) (wal.WAL, error)
}

// localWAL is a hint type for local wal.
type localWAL struct {
	wal.WAL
}

func (l localWAL) isLocal() localTrait {
	return localTrait{}
}

// Append writes a record to the log.
func (l localWAL) Append(ctx context.Context, msg message.MutableMessage) (*types.AppendResult, error) {
	msg = message.CloneMutableMessage(msg)
	return l.WAL.Append(ctx, msg)
}

// Append a record to the log asynchronously.
func (l localWAL) AppendAsync(ctx context.Context, msg message.MutableMessage, cb func(*types.AppendResult, error)) {
	msg = message.CloneMutableMessage(msg)
	l.WAL.AppendAsync(ctx, msg, cb)
}

func (l localWAL) Read(ctx context.Context, deliverPolicy wal.ReadOption) (wal.Scanner, error) {
	s, err := l.WAL.Read(ctx, deliverPolicy)
	if err != nil {
		return nil, err
	}
	return localScanner{s}, nil
}

// localScanner is a hint type for local wal scanner.
type localScanner struct {
	wal.Scanner
}

func (s localScanner) isLocal() localTrait {
	return localTrait{}
}
