package timesync

import (
	"context"
	"log"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	ms "github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type timeSyncMsgProducer struct {
	//softTimeTickBarrier
	proxyTtBarrier TimeTickBarrier
	//hardTimeTickBarrier
	writeNodeTtBarrier TimeTickBarrier

	ddSyncStream  ms.MsgStream // insert & delete
	dmSyncStream  ms.MsgStream
	k2sSyncStream ms.MsgStream

	ctx    context.Context
	cancel context.CancelFunc

	proxyWatchers     []TimeTickWatcher
	writeNodeWatchers []TimeTickWatcher
}

func NewTimeSyncMsgProducer(ctx context.Context) (*timeSyncMsgProducer, error) {
	ctx2, cancel := context.WithCancel(ctx)
	return &timeSyncMsgProducer{ctx: ctx2, cancel: cancel}, nil
}

func (syncMsgProducer *timeSyncMsgProducer) SetProxyTtBarrier(proxyTtBarrier TimeTickBarrier) {
	syncMsgProducer.proxyTtBarrier = proxyTtBarrier
}

func (syncMsgProducer *timeSyncMsgProducer) SetWriteNodeTtBarrier(writeNodeTtBarrier TimeTickBarrier) {
	syncMsgProducer.writeNodeTtBarrier = writeNodeTtBarrier
}
func (syncMsgProducer *timeSyncMsgProducer) SetDDSyncStream(ddSync ms.MsgStream) {
	syncMsgProducer.ddSyncStream = ddSync
}

func (syncMsgProducer *timeSyncMsgProducer) SetDMSyncStream(dmSync ms.MsgStream) {
	syncMsgProducer.dmSyncStream = dmSync
}

func (syncMsgProducer *timeSyncMsgProducer) SetK2sSyncStream(k2sSync ms.MsgStream) {
	syncMsgProducer.k2sSyncStream = k2sSync
}

func (syncMsgProducer *timeSyncMsgProducer) WatchProxyTtBarrier(watcher TimeTickWatcher) {
	syncMsgProducer.proxyWatchers = append(syncMsgProducer.proxyWatchers, watcher)
}

func (syncMsgProducer *timeSyncMsgProducer) WatchWriteNodeTtBarrier(watcher TimeTickWatcher) {
	syncMsgProducer.writeNodeWatchers = append(syncMsgProducer.writeNodeWatchers, watcher)
}

func (syncMsgProducer *timeSyncMsgProducer) broadcastMsg(barrier TimeTickBarrier, streams []ms.MsgStream, watchers []TimeTickWatcher) error {
	for {
		select {
		case <-syncMsgProducer.ctx.Done():
			{
				log.Printf("broadcast context done, exit")
				return errors.Errorf("broadcast done exit")
			}
		default:
			timetick, err := barrier.GetTimeTick()
			if err != nil {
				log.Printf("broadcast get time tick error")
			}
			msgPack := ms.MsgPack{}
			baseMsg := ms.BaseMsg{
				BeginTimestamp: timetick,
				EndTimestamp:   timetick,
				HashValues:     []uint32{0},
			}
			timeTickResult := internalpb2.TimeTickMsg{
				Base: &commonpb.MsgBase{
					MsgType:   commonpb.MsgType_kTimeTick,
					MsgID:     0,
					Timestamp: timetick,
					SourceID:  0,
				},
			}
			timeTickMsg := &ms.TimeTickMsg{
				BaseMsg:     baseMsg,
				TimeTickMsg: timeTickResult,
			}
			msgPack.Msgs = append(msgPack.Msgs, timeTickMsg)
			for _, stream := range streams {
				err = stream.Broadcast(&msgPack)
			}

			for _, watcher := range watchers {
				watcher.Watch(timeTickMsg)
			}
			if err != nil {
				return err
			}
		}
	}
}

func (syncMsgProducer *timeSyncMsgProducer) Start() error {
	err := syncMsgProducer.proxyTtBarrier.Start()
	if err != nil {
		return err
	}

	err = syncMsgProducer.writeNodeTtBarrier.Start()
	if err != nil {
		return err
	}

	for _, watcher := range syncMsgProducer.proxyWatchers {
		watcher.Start()
	}
	for _, watcher := range syncMsgProducer.writeNodeWatchers {
		watcher.Start()
	}

	go syncMsgProducer.broadcastMsg(syncMsgProducer.proxyTtBarrier, []ms.MsgStream{syncMsgProducer.dmSyncStream, syncMsgProducer.ddSyncStream}, syncMsgProducer.proxyWatchers)
	go syncMsgProducer.broadcastMsg(syncMsgProducer.writeNodeTtBarrier, []ms.MsgStream{syncMsgProducer.k2sSyncStream}, syncMsgProducer.writeNodeWatchers)

	return nil
}

func (syncMsgProducer *timeSyncMsgProducer) Close() {
	syncMsgProducer.ddSyncStream.Close()
	syncMsgProducer.dmSyncStream.Close()
	syncMsgProducer.k2sSyncStream.Close()
	syncMsgProducer.cancel()
	syncMsgProducer.proxyTtBarrier.Close()
	syncMsgProducer.writeNodeTtBarrier.Close()
	for _, watcher := range syncMsgProducer.proxyWatchers {
		watcher.Close()
	}
	for _, watcher := range syncMsgProducer.writeNodeWatchers {
		watcher.Close()
	}
}
