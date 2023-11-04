package writebuffer

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

// Manager is the interface for WriteBuffer management.
type Manager interface {
	// Register adds a WriteBuffer with provided schema & options.
	Register(channel string, schema *schemapb.CollectionSchema, metacache metacache.MetaCache, opts ...WriteBufferOption) error
	// FlushSegments notifies writeBuffer corresponding to provided channel to flush segments.
	FlushSegments(ctx context.Context, channel string, segmentIDs []int64) error
	// RemoveChannel removes a write buffer from manager.
	RemoveChannel(channel string)
	// BufferData put data into channel write buffer.
	BufferData(channel string, insertMsgs []*msgstream.InsertMsg, deleteMsgs []*msgstream.DeleteMsg, startPos, endPos *msgpb.MsgPosition) error
}

// NewManager returns initialized manager as `Manager`
func NewManager(syncMgr syncmgr.SyncManager) Manager {
	return &manager{
		syncMgr: syncMgr,
		buffers: make(map[string]WriteBuffer),
	}
}

type manager struct {
	syncMgr syncmgr.SyncManager
	buffers map[string]WriteBuffer
	mut     sync.RWMutex
}

// Register a new WriteBuffer for channel.
func (m *manager) Register(channel string, schema *schemapb.CollectionSchema, metacache metacache.MetaCache, opts ...WriteBufferOption) error {
	m.mut.Lock()
	defer m.mut.Unlock()

	_, ok := m.buffers[channel]
	if ok {
		return merr.WrapErrChannelReduplicate(channel)
	}
	buf, err := NewWriteBuffer(schema, metacache, m.syncMgr, opts...)
	if err != nil {
		return err
	}
	m.buffers[channel] = buf
	return nil
}

// FlushSegments call sync segment and change segments state to Flushed.
func (m *manager) FlushSegments(ctx context.Context, channel string, segmentIDs []int64) error {
	m.mut.RLock()
	buf, ok := m.buffers[channel]
	m.mut.RUnlock()

	if !ok {
		log.Ctx(ctx).Warn("write buffer not found when flush segments",
			zap.String("channel", channel),
			zap.Int64s("segmentIDs", segmentIDs))
		return merr.WrapErrChannelNotFound(channel)
	}

	return buf.FlushSegments(ctx, segmentIDs)
}

// BufferData put data into channel write buffer.
func (m *manager) BufferData(channel string, insertMsgs []*msgstream.InsertMsg, deleteMsgs []*msgstream.DeleteMsg, startPos, endPos *msgpb.MsgPosition) error {
	m.mut.RLock()
	buf, ok := m.buffers[channel]
	m.mut.RUnlock()

	if !ok {
		log.Ctx(context.Background()).Warn("write buffer not found when buffer data",
			zap.String("channel", channel))
		return merr.WrapErrChannelNotFound(channel)
	}

	return buf.BufferData(insertMsgs, deleteMsgs, startPos, endPos)
}

// RemoveChannel remove channel WriteBuffer from manager.
func (m *manager) RemoveChannel(channel string) {
	m.mut.Lock()
	buf, ok := m.buffers[channel]
	delete(m.buffers, channel)
	m.mut.Unlock()

	if !ok {
		log.Warn("failed to remove channel, channel not maintained in manager", zap.String("channel", channel))
		return
	}

	buf.Close()
}
