package writebuffer

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/lifetime"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// BufferManager is the interface for WriteBuffer management.
//
//go:generate mockery --name=BufferManager --structname=MockBufferManager --output=./  --filename=mock_manager.go --with-expecter --inpackage
type BufferManager interface {
	// Register adds a WriteBuffer with provided schema & options.
	Register(channel string, metacache metacache.MetaCache, opts ...WriteBufferOption) error
	// CreateNewGrowingSegment notifies writeBuffer to create a new growing segment.
	CreateNewGrowingSegment(ctx context.Context, channel string, partition int64, segmentID int64) error
	// SealSegments notifies writeBuffer corresponding to provided channel to seal segments.
	// which will cause segment start flush procedure.
	SealSegments(ctx context.Context, channel string, segmentIDs []int64) error
	// FlushChannel set the flushTs of the provided write buffer.
	FlushChannel(ctx context.Context, channel string, flushTs uint64) error
	// SealAllSegments flushes all segments in the write buffer and seals them.
	SealAllSegments(ctx context.Context, channel string)
	// RemoveChannel removes a write buffer from manager.
	RemoveChannel(channel string)
	// DropChannel remove write buffer and perform drop.
	DropChannel(channel string)
	DropPartitions(channel string, partitionIDs []int64)
	// BufferData put data into channel write buffer.
	BufferData(channel string, insertData []*InsertData, deleteMsgs []*msgstream.DeleteMsg, startPos, endPos *msgpb.MsgPosition) error
	// GetCheckpoint returns checkpoint for provided channel.
	GetCheckpoint(channel string) (*msgpb.MsgPosition, bool, error)
	// NotifyCheckpointUpdated notify write buffer checkpoint updated to reset flushTs.
	NotifyCheckpointUpdated(channel string, ts uint64)

	// Start makes the background check start to work.
	Start()
	// Stop the background checker and wait for worker goroutine quit.
	Stop()
}

// NewManager returns initialized manager as `Manager`
func NewManager(syncMgr syncmgr.SyncManager) BufferManager {
	return &bufferManager{
		syncMgr: syncMgr,
		buffers: typeutil.NewConcurrentMap[string, WriteBuffer](),

		ch: lifetime.NewSafeChan(),
	}
}

type bufferManager struct {
	syncMgr syncmgr.SyncManager
	buffers *typeutil.ConcurrentMap[string, WriteBuffer]

	wg sync.WaitGroup
	ch lifetime.SafeChan
}

func (m *bufferManager) Start() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.check()
	}()
}

func (m *bufferManager) check() {
	timer := time.NewTimer(paramtable.Get().DataNodeCfg.MemoryCheckInterval.GetAsDuration(time.Millisecond))
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			m.memoryCheck()
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(paramtable.Get().DataNodeCfg.MemoryCheckInterval.GetAsDuration(time.Millisecond))
		case <-m.ch.CloseCh():
			log.Info("buffer manager memory check stopped")
			return
		}
	}
}

// memoryCheck performs check based on current memory usage & configuration.
func (m *bufferManager) memoryCheck() {
	if !paramtable.Get().DataNodeCfg.MemoryForceSyncEnable.GetAsBool() {
		return
	}
	startTime := time.Now()
	defer func() {
		dur := time.Since(startTime)
		if dur > 30*time.Second {
			log.Warn("memory check takes too long", zap.Duration("time", dur))
		}
	}()

	for {
		var total int64
		var candidate WriteBuffer
		var candiSize int64
		var candiChan string

		toMB := func(mem float64) float64 {
			return mem / 1024 / 1024
		}

		select {
		case <-m.ch.CloseCh():
			log.Info("stop memory check due to manager stop")
			return
		default:
		}

		m.buffers.Range(func(chanName string, buf WriteBuffer) bool {
			size := buf.MemorySize()
			total += size
			if size > candiSize {
				candiSize = size
				candidate = buf
				candiChan = chanName
			}
			return true
		})

		totalMemory := hardware.GetMemoryCount()
		memoryWatermark := float64(totalMemory) * paramtable.Get().DataNodeCfg.MemoryForceSyncWatermark.GetAsFloat()
		if float64(total) < memoryWatermark {
			log.RatedDebug(20, "skip force sync because memory level is not high enough",
				zap.Float64("current_total_memory_usage", toMB(float64(total))),
				zap.Float64("current_memory_watermark", toMB(memoryWatermark)))
			return
		}

		if candidate != nil {
			candidate.EvictBuffer(GetOldestBufferPolicy(paramtable.Get().DataNodeCfg.MemoryForceSyncSegmentNum.GetAsInt()))
			log.Info("notify writebuffer to sync",
				zap.String("channel", candiChan), zap.Float64("bufferSize(MB)", toMB(float64(candiSize))))
		}
	}
}

func (m *bufferManager) Stop() {
	m.ch.Close()
	m.wg.Wait()
}

// Register a new WriteBuffer for channel.
func (m *bufferManager) Register(channel string, metacache metacache.MetaCache, opts ...WriteBufferOption) error {
	buf, err := NewWriteBuffer(channel, metacache, m.syncMgr, opts...)
	if err != nil {
		return err
	}

	_, loaded := m.buffers.GetOrInsert(channel, buf)
	if loaded {
		buf.Close(context.Background(), false)
		return merr.WrapErrChannelReduplicate(channel)
	}
	return nil
}

// CreateNewGrowingSegment notifies writeBuffer to create a new growing segment.
func (m *bufferManager) CreateNewGrowingSegment(ctx context.Context, channel string, partitionID int64, segmentID int64) error {
	buf, loaded := m.buffers.Get(channel)
	if !loaded {
		log.Ctx(ctx).Warn("write buffer not found when create new growing segment",
			zap.String("channel", channel),
			zap.Int64("partitionID", partitionID),
			zap.Int64("segmentID", segmentID))
		return merr.WrapErrChannelNotFound(channel)
	}
	buf.CreateNewGrowingSegment(partitionID, segmentID, nil)
	return nil
}

// SealSegments call sync segment and change segments state to Flushed.
func (m *bufferManager) SealSegments(ctx context.Context, channel string, segmentIDs []int64) error {
	buf, loaded := m.buffers.Get(channel)
	if !loaded {
		log.Ctx(ctx).Warn("write buffer not found when flush segments",
			zap.String("channel", channel),
			zap.Int64s("segmentIDs", segmentIDs))
		return merr.WrapErrChannelNotFound(channel)
	}

	return buf.SealSegments(ctx, segmentIDs)
}

// SealAllSegments flushes all segments in the write buffer and seals them.
func (m *bufferManager) SealAllSegments(ctx context.Context, channel string) {
	buf, loaded := m.buffers.Get(channel)
	if !loaded {
		log.Ctx(ctx).Warn("write buffer not found when flush all segments, ignored",
			zap.String("channel", channel))
	}
	buf.SealAllSegments(ctx)
}

func (m *bufferManager) FlushChannel(ctx context.Context, channel string, flushTs uint64) error {
	buf, loaded := m.buffers.Get(channel)
	if !loaded {
		log.Ctx(ctx).Warn("write buffer not found when flush channel",
			zap.String("channel", channel),
			zap.Uint64("flushTs", flushTs))
		return merr.WrapErrChannelNotFound(channel)
	}
	buf.SetFlushTimestamp(flushTs)
	return nil
}

// BufferData put data into channel write buffer.
func (m *bufferManager) BufferData(channel string, insertData []*InsertData, deleteMsgs []*msgstream.DeleteMsg, startPos, endPos *msgpb.MsgPosition) error {
	buf, loaded := m.buffers.Get(channel)
	if !loaded {
		log.Ctx(context.Background()).Warn("write buffer not found when buffer data",
			zap.String("channel", channel))
		return merr.WrapErrChannelNotFound(channel)
	}

	return buf.BufferData(insertData, deleteMsgs, startPos, endPos)
}

// GetCheckpoint returns checkpoint for provided channel.
func (m *bufferManager) GetCheckpoint(channel string) (*msgpb.MsgPosition, bool, error) {
	buf, loaded := m.buffers.Get(channel)
	if !loaded {
		return nil, false, merr.WrapErrChannelNotFound(channel)
	}
	cp := buf.GetCheckpoint()
	flushTs := buf.GetFlushTimestamp()

	return cp, flushTs != nonFlushTS && cp.GetTimestamp() >= flushTs, nil
}

func (m *bufferManager) NotifyCheckpointUpdated(channel string, ts uint64) {
	buf, loaded := m.buffers.Get(channel)
	if !loaded {
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
	buf, loaded := m.buffers.GetAndRemove(channel)
	if !loaded {
		log.Warn("failed to remove channel, channel not maintained in manager", zap.String("channel", channel))
		return
	}

	buf.Close(context.Background(), false)
}

// DropChannel removes channel WriteBuffer and process `DropChannel`
// this method will save all buffered data
func (m *bufferManager) DropChannel(channel string) {
	buf, loaded := m.buffers.GetAndRemove(channel)
	if !loaded {
		log.Warn("failed to drop channel, channel not maintained in manager", zap.String("channel", channel))
		return
	}

	buf.Close(context.Background(), true)
}

func (m *bufferManager) DropPartitions(channel string, partitionIDs []int64) {
	buf, loaded := m.buffers.Get(channel)
	if !loaded {
		log.Warn("failed to drop partition, channel not maintained in manager", zap.String("channel", channel), zap.Int64s("partitionIDs", partitionIDs))
		return
	}

	buf.DropPartitions(partitionIDs)
}
