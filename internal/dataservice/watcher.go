package dataservice

import (
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"go.uber.org/zap"

	"golang.org/x/net/context"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type (
	proxyTimeTickWatcher struct {
		allocator segmentAllocatorInterface
		msgQueue  chan *msgstream.TimeTickMsg
	}
	dataNodeTimeTickWatcher struct {
		meta      *meta
		cluster   *dataNodeCluster
		allocator segmentAllocatorInterface
		msgQueue  chan *msgstream.TimeTickMsg
	}
)

func newProxyTimeTickWatcher(allocator segmentAllocatorInterface) *proxyTimeTickWatcher {
	return &proxyTimeTickWatcher{
		allocator: allocator,
		msgQueue:  make(chan *msgstream.TimeTickMsg, 1),
	}
}

func (watcher *proxyTimeTickWatcher) StartBackgroundLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Debug("proxy time tick watcher closed")
			return
		case msg := <-watcher.msgQueue:
			if err := watcher.allocator.ExpireAllocations(msg.Base.Timestamp); err != nil {
				log.Error("expire allocations error", zap.Error(err))
			}
		}
	}
}

func (watcher *proxyTimeTickWatcher) Watch(msg *msgstream.TimeTickMsg) {
	watcher.msgQueue <- msg
}

func newDataNodeTimeTickWatcher(meta *meta, allocator segmentAllocatorInterface, cluster *dataNodeCluster) *dataNodeTimeTickWatcher {
	return &dataNodeTimeTickWatcher{
		meta:      meta,
		allocator: allocator,
		cluster:   cluster,
		msgQueue:  make(chan *msgstream.TimeTickMsg, 1),
	}
}

func (watcher *dataNodeTimeTickWatcher) Watch(msg *msgstream.TimeTickMsg) {
	watcher.msgQueue <- msg
}

func (watcher *dataNodeTimeTickWatcher) StartBackgroundLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			log.Debug("data node time tick watcher closed")
			return
		case msg := <-watcher.msgQueue:
			if err := watcher.handleTimeTickMsg(msg); err != nil {
				log.Error("handle time tick error", zap.Error(err))
				continue
			}
		}
	}
}

func (watcher *dataNodeTimeTickWatcher) handleTimeTickMsg(msg *msgstream.TimeTickMsg) error {
	segments, err := watcher.allocator.GetSealedSegments()
	if err != nil {
		return err
	}
	for _, id := range segments {
		expired, err := watcher.allocator.IsAllocationsExpired(id, msg.Base.Timestamp)
		if err != nil {
			log.Error("check allocations expired error", zap.Int64("segmentID", id), zap.Error(err))
			continue
		}
		if expired {
			segmentInfo, err := watcher.meta.GetSegment(id)
			if err != nil {
				log.Error("get segment from meta error", zap.Int64("segmentID", id), zap.Error(err))
				continue
			}
			if err = watcher.meta.SetSegmentState(id, commonpb.SegmentState_SegmentSealed); err != nil {
				log.Error("set segment state error", zap.Int64("segmentID", id), zap.Error(err))
				continue
			}
			watcher.cluster.FlushSegment(&datapb.FlushSegRequest{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_kFlush,
					MsgID:     -1, // todo add msg id
					Timestamp: 0,  // todo
					SourceID:  Params.NodeID,
				},
				CollectionID: segmentInfo.CollectionID,
				SegmentIDs:   []int64{segmentInfo.SegmentID},
			})
			watcher.allocator.DropSegment(id)
		}
	}
	return nil
}
