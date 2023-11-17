package writebuffer

import (
	"context"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

// BufferManager is the interface for WriteBuffer management.
type BufferManager interface {
	// Register adds a WriteBuffer with provided schema & options.
	Register(channel string, metacache metacache.MetaCache, opts ...WriteBufferOption) error
	// FlushSegments notifies writeBuffer corresponding to provided channel to flush segments.
	FlushSegments(ctx context.Context, channel string, segmentIDs []int64) error
	// FlushChannel
	FlushChannel(ctx context.Context, channel string, flushTs uint64) error
	// RemoveChannel removes a write buffer from manager.
	RemoveChannel(channel string)
	// DropChannel remove write buffer and perform drop.
	DropChannel(channel string)
	// BufferData put data into channel write buffer.
	BufferData(channel string, insertMsgs []*msgstream.InsertMsg, deleteMsgs []*msgstream.DeleteMsg, startPos, endPos *msgpb.MsgPosition) error
	// GetCheckpoint returns checkpoint for provided channel.
	GetCheckpoint(channel string) (*msgpb.MsgPosition, bool, error)
	// NotifyCheckpointUpdated notify write buffer checkpoint updated to reset flushTs.
	NotifyCheckpointUpdated(channel string, ts uint64)
}

// NewManager returns initialized manager as `Manager`
func NewManager(syncMgr syncmgr.SyncManager) BufferManager {
	return &bufferManager{
		syncMgr: syncMgr,
		buffers: make(map[string]WriteBuffer),
	}
}

type bufferManager struct {
	syncMgr syncmgr.SyncManager
	buffers map[string]WriteBuffer
	mut     sync.RWMutex
}

// Register a new WriteBuffer for channel.
func (m *bufferManager) Register(channel string, metacache metacache.MetaCache, opts ...WriteBufferOption) error {
	m.mut.Lock()
	defer m.mut.Unlock()

	_, ok := m.buffers[channel]
	if ok {
		return merr.WrapErrChannelReduplicate(channel)
	}
	buf, err := NewWriteBuffer(channel, metacache, m.syncMgr, opts...)
	if err != nil {
		return err
	}
	m.buffers[channel] = buf
	return nil
}

// FlushSegments call sync segment and change segments state to Flushed.
func (m *bufferManager) FlushSegments(ctx context.Context, channel string, segmentIDs []int64) error {
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

func (m *bufferManager) FlushChannel(ctx context.Context, channel string, flushTs uint64) error {
	m.mut.RLock()
	buf, ok := m.buffers[channel]
	m.mut.RUnlock()

	if !ok {
		log.Ctx(ctx).Warn("write buffer not found when flush segments",
			zap.String("channel", channel),
			zap.Uint64("flushTs", flushTs))
		return merr.WrapErrChannelNotFound(channel)
	}
	buf.SetFlushTimestamp(flushTs)
	return nil
}

// BufferData put data into channel write buffer.
func (m *bufferManager) BufferData(channel string, insertMsgs []*msgstream.InsertMsg, deleteMsgs []*msgstream.DeleteMsg, startPos, endPos *msgpb.MsgPosition) error {
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

// GetCheckpoint returns checkpoint for provided channel.
func (m *bufferManager) GetCheckpoint(channel string) (*msgpb.MsgPosition, bool, error) {
	m.mut.RLock()
	buf, ok := m.buffers[channel]
	m.mut.RUnlock()

	if !ok {
		return nil, false, merr.WrapErrChannelNotFound(channel)
	}
	cp := buf.GetCheckpoint()
	flushTs := buf.GetFlushTimestamp()

	return cp, flushTs != nonFlushTS && cp.GetTimestamp() >= flushTs, nil
}

func (m *bufferManager) NotifyCheckpointUpdated(channel string, ts uint64) {
	m.mut.Lock()
	defer m.mut.Unlock()
	buf, ok := m.buffers[channel]
	if !ok {
		return
	}
	flushTs := buf.GetFlushTimestamp()
	if flushTs != nonFlushTS && ts > flushTs {
		log.Info("reset channel flushTs", zap.String("channel", channel))
		buf.SetFlushTimestamp(nonFlushTS)
	}
}

// RemoveChannel remove channel WriteBuffer from manager.
// this method discards all buffered data since datanode no longer has the ownership
func (m *bufferManager) RemoveChannel(channel string) {
	m.mut.Lock()
	buf, ok := m.buffers[channel]
	delete(m.buffers, channel)
	m.mut.Unlock()

	if !ok {
		log.Warn("failed to remove channel, channel not maintained in manager", zap.String("channel", channel))
		return
	}

	buf.Close(false)
}

// DropChannel removes channel WriteBuffer and process `DropChannel`
// this method will save all buffered data
func (m *bufferManager) DropChannel(channel string) {
	m.mut.Lock()
	buf, ok := m.buffers[channel]
	delete(m.buffers, channel)
	m.mut.Unlock()

	if !ok {
		log.Warn("failed to drop channel, channel not maintained in manager", zap.String("channel", channel))
		return
	}

	buf.Close(true)
}
