package proxyservice

import (
	"context"
	"log"
	"math"
	"sync"
	"sync/atomic"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	ms "github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type (
	TimeTickBarrier interface {
		GetTimeTick() (Timestamp, error)
		Start() error
		Close()
		AddPeer(peerID UniqueID) error
		TickChan() <-chan Timestamp
	}

	softTimeTickBarrier struct {
		peer2LastTt   map[UniqueID]Timestamp
		peerMtx       sync.RWMutex
		minTtInterval Timestamp
		lastTt        int64
		outTt         chan Timestamp
		ttStream      ms.MsgStream
		ctx           context.Context
		cancel        context.CancelFunc
	}
)

func (ttBarrier *softTimeTickBarrier) TickChan() <-chan Timestamp {
	return ttBarrier.outTt
}

func (ttBarrier *softTimeTickBarrier) AddPeer(peerID UniqueID) error {
	ttBarrier.peerMtx.Lock()
	defer ttBarrier.peerMtx.Unlock()

	_, ok := ttBarrier.peer2LastTt[peerID]
	if ok {
		log.Println("no need to add duplicated peer: ", peerID)
		return nil
	}

	ttBarrier.peer2LastTt[peerID] = 0

	return nil
}

func (ttBarrier *softTimeTickBarrier) GetTimeTick() (Timestamp, error) {
	select {
	case <-ttBarrier.ctx.Done():
		return 0, errors.Errorf("[GetTimeTick] closed.")
	case ts, ok := <-ttBarrier.outTt:
		if !ok {
			return 0, errors.Errorf("[GetTimeTick] closed.")
		}
		num := len(ttBarrier.outTt)
		for i := 0; i < num; i++ {
			ts, ok = <-ttBarrier.outTt
			if !ok {
				return 0, errors.Errorf("[GetTimeTick] closed.")
			}
		}
		atomic.StoreInt64(&(ttBarrier.lastTt), int64(ts))
		log.Println("current tick: ", ts)
		return ts, ttBarrier.ctx.Err()
	}
}

func (ttBarrier *softTimeTickBarrier) Start() error {
	go func() {
		for {
			select {
			case <-ttBarrier.ctx.Done():
				log.Printf("[TtBarrierStart] %s\n", ttBarrier.ctx.Err())
				return

			case ttmsgs := <-ttBarrier.ttStream.Chan():
				//log.Println("ttmsgs: ", ttmsgs)
				ttBarrier.peerMtx.RLock()
				log.Println("peer2LastTt map: ", ttBarrier.peer2LastTt)
				log.Println("len(ttmsgs.Msgs): ", len(ttmsgs.Msgs))
				if len(ttmsgs.Msgs) > 0 {
					for _, timetickmsg := range ttmsgs.Msgs {
						ttmsg := timetickmsg.(*ms.TimeTickMsg)
						oldT, ok := ttBarrier.peer2LastTt[ttmsg.Base.SourceID]
						// log.Printf("[softTimeTickBarrier] peer(%d)=%d\n", ttmsg.PeerID, ttmsg.Timestamp)

						if !ok {
							log.Printf("[softTimeTickBarrier] Warning: peerID %d not exist\n", ttmsg.Base.SourceID)
							continue
						}
						log.Println("ttmsg.Base.Timestamp: ", ttmsg.Base.Timestamp)
						log.Println("oldT: ", oldT)
						if ttmsg.Base.Timestamp > oldT {
							ttBarrier.peer2LastTt[ttmsg.Base.SourceID] = ttmsg.Base.Timestamp

							// get a legal Timestamp
							ts := ttBarrier.minTimestamp()
							lastTt := atomic.LoadInt64(&(ttBarrier.lastTt))
							if lastTt != 0 && ttBarrier.minTtInterval > ts-Timestamp(lastTt) {
								continue
							}
							ttBarrier.outTt <- ts
						}
					}
				}
				ttBarrier.peerMtx.RUnlock()
			}
		}
	}()

	ttBarrier.ttStream.Start()

	return nil
}

func newSoftTimeTickBarrier(ctx context.Context,
	ttStream ms.MsgStream,
	peerIds []UniqueID,
	minTtInterval Timestamp) TimeTickBarrier {

	if len(peerIds) <= 0 {
		log.Printf("[newSoftTimeTickBarrier] Warning: peerIds is empty!\n")
		//return nil
	}

	sttbarrier := softTimeTickBarrier{}
	sttbarrier.minTtInterval = minTtInterval
	sttbarrier.ttStream = ttStream
	sttbarrier.outTt = make(chan Timestamp, 1024)
	sttbarrier.ctx, sttbarrier.cancel = context.WithCancel(ctx)
	sttbarrier.peer2LastTt = make(map[UniqueID]Timestamp)
	for _, id := range peerIds {
		sttbarrier.peer2LastTt[id] = Timestamp(0)
	}
	if len(peerIds) != len(sttbarrier.peer2LastTt) {
		log.Printf("[newSoftTimeTickBarrier] Warning: there are duplicate peerIds!\n")
	}

	return &sttbarrier
}

func (ttBarrier *softTimeTickBarrier) Close() {
	ttBarrier.cancel()
}

func (ttBarrier *softTimeTickBarrier) minTimestamp() Timestamp {
	tempMin := Timestamp(math.MaxUint64)
	for _, tt := range ttBarrier.peer2LastTt {
		if tt < tempMin {
			tempMin = tt
		}
	}
	return tempMin
}
