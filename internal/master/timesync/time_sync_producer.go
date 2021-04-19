package timesync

import (
	"context"
	"log"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	ms "github.com/zilliztech/milvus-distributed/internal/msgstream"
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
)

type timeSyncMsgProducer struct {
	//softTimeTickBarrier
	proxyTtBarrier TimeTickBarrier
	//hardTimeTickBarrier
	writeNodeTtBarrier TimeTickBarrier

	dmSyncStream  ms.MsgStream // insert & delete
	k2sSyncStream ms.MsgStream

	ctx    context.Context
	cancel context.CancelFunc
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

func (syncMsgProducer *timeSyncMsgProducer) SetDMSyncStream(dmSync ms.MsgStream) {
	syncMsgProducer.dmSyncStream = dmSync
}

func (syncMsgProducer *timeSyncMsgProducer) SetK2sSyncStream(k2sSync ms.MsgStream) {
	syncMsgProducer.k2sSyncStream = k2sSync
}

func (syncMsgProducer *timeSyncMsgProducer) broadcastMsg(barrier TimeTickBarrier, stream ms.MsgStream) error {
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
				HashValues:     []int32{0},
			}
			timeTickResult := internalPb.TimeTickMsg{
				MsgType:   internalPb.MsgType_kTimeTick,
				PeerID:    0,
				Timestamp: timetick,
			}
			timeTickMsg := &ms.TimeTickMsg{
				BaseMsg:     baseMsg,
				TimeTickMsg: timeTickResult,
			}
			var tsMsg ms.TsMsg
			tsMsg = timeTickMsg
			msgPack.Msgs = append(msgPack.Msgs, &tsMsg)
			err = stream.Broadcast(&msgPack)
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

	go syncMsgProducer.broadcastMsg(syncMsgProducer.proxyTtBarrier, syncMsgProducer.dmSyncStream)
	go syncMsgProducer.broadcastMsg(syncMsgProducer.writeNodeTtBarrier, syncMsgProducer.k2sSyncStream)

	return nil
}

func (syncMsgProducer *timeSyncMsgProducer) Close() {
	syncMsgProducer.proxyTtBarrier.Close()
	syncMsgProducer.writeNodeTtBarrier.Close()
	syncMsgProducer.dmSyncStream.Close()
	syncMsgProducer.k2sSyncStream.Close()
	syncMsgProducer.cancel()
}
