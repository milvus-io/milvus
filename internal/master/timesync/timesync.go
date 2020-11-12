package timesync

import (
	"context"
	"log"
	"math"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	ms "github.com/zilliztech/milvus-distributed/internal/msgstream"
)

type (
	softTimeTickBarrier struct {
		peer2LastTt   map[UniqueID]Timestamp
		minTtInterval Timestamp
		lastTt        Timestamp
		outTt         chan Timestamp
		ttStream      ms.MsgStream
		ctx           context.Context
		closeCh       chan struct{} // close goroutinue in Start()
		closed        bool
	}

	hardTimeTickBarrier struct {
		peer2Tt  map[UniqueID]Timestamp
		outTt    chan Timestamp
		ttStream ms.MsgStream
		ctx      context.Context
		closeCh  chan struct{} // close goroutinue in Start()
		closed   bool
	}
)

func (ttBarrier *softTimeTickBarrier) GetTimeTick() (Timestamp, error) {
	isEmpty := true
	for {

		if ttBarrier.closed {
			return 0, errors.Errorf("[GetTimeTick] closed.")
		}

		select {
		case ts := <-ttBarrier.outTt:
			isEmpty = false
			ttBarrier.lastTt = ts

		default:
			if isEmpty || ttBarrier.closed {
				continue
			}
			return ttBarrier.lastTt, nil
		}
	}
}

func (ttBarrier *softTimeTickBarrier) Start() error {
	ttBarrier.closeCh = make(chan struct{})
	go func() {
		for {
			select {

			case <-ttBarrier.closeCh:
				log.Printf("[TtBarrierStart] closed\n")
				return

			case <-ttBarrier.ctx.Done():
				log.Printf("[TtBarrierStart] %s\n", ttBarrier.ctx.Err())
				ttBarrier.closed = true
				return

			case ttmsgs := <-ttBarrier.ttStream.Chan():
				if len(ttmsgs.Msgs) > 0 {
					for _, timetickmsg := range ttmsgs.Msgs {
						ttmsg := (*timetickmsg).(*ms.TimeTickMsg)
						oldT, ok := ttBarrier.peer2LastTt[ttmsg.PeerId]
						log.Printf("[softTimeTickBarrier] peer(%d)=%d\n", ttmsg.PeerId, ttmsg.Timestamp)

						if !ok {
							log.Printf("[softTimeTickBarrier] Warning: peerID %d not exist\n", ttmsg.PeerId)
							continue
						}

						if ttmsg.Timestamp > oldT {
							ttBarrier.peer2LastTt[ttmsg.PeerId] = ttmsg.Timestamp

							// get a legal Timestamp
							ts := ttBarrier.minTimestamp()

							if ttBarrier.lastTt != 0 && ttBarrier.minTtInterval > ts-ttBarrier.lastTt {
								continue
							}

							ttBarrier.outTt <- ts
						}
					}
				}

			default:
			}
		}
	}()
	return nil
}

func NewSoftTimeTickBarrier(ctx context.Context,
	ttStream *ms.MsgStream,
	peerIds []UniqueID,
	minTtInterval Timestamp) *softTimeTickBarrier {

	if len(peerIds) <= 0 {
		log.Printf("[NewSoftTimeTickBarrier] Error: peerIds is empty!\n")
		return nil
	}

	sttbarrier := softTimeTickBarrier{}
	sttbarrier.minTtInterval = minTtInterval
	sttbarrier.ttStream = *ttStream
	sttbarrier.outTt = make(chan Timestamp, 1024)
	sttbarrier.ctx = ctx
	sttbarrier.closed = false

	sttbarrier.peer2LastTt = make(map[UniqueID]Timestamp)
	for _, id := range peerIds {
		sttbarrier.peer2LastTt[id] = Timestamp(0)
	}
	if len(peerIds) != len(sttbarrier.peer2LastTt) {
		log.Printf("[NewSoftTimeTickBarrier] Warning: there are duplicate peerIds!\n")
	}

	return &sttbarrier
}

func (ttBarrier *softTimeTickBarrier) Close() {

	if ttBarrier.closeCh != nil {
		ttBarrier.closeCh <- struct{}{}
	}

	ttBarrier.closed = true
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

func (ttBarrier *hardTimeTickBarrier) GetTimeTick() (Timestamp, error) {
	for {

		if ttBarrier.closed {
			return 0, errors.Errorf("[GetTimeTick] closed.")
		}

		select {
		case ts := <-ttBarrier.outTt:
			return ts, nil
		default:
		}
	}
}

func (ttBarrier *hardTimeTickBarrier) Start() error {
	ttBarrier.closeCh = make(chan struct{})

	go func() {
		// Last timestamp synchronized
		state := Timestamp(0)
		for {
			select {

			case <-ttBarrier.closeCh:
				log.Printf("[TtBarrierStart] closed\n")
				return

			case <-ttBarrier.ctx.Done():
				log.Printf("[TtBarrierStart] %s\n", ttBarrier.ctx.Err())
				ttBarrier.closed = true
				return

			case ttmsgs := <-ttBarrier.ttStream.Chan():
				if len(ttmsgs.Msgs) > 0 {
					for _, timetickmsg := range ttmsgs.Msgs {

						// Suppose ttmsg.Timestamp from stream is always larger than the previous one,
						// that `ttmsg.Timestamp > oldT`
						ttmsg := (*timetickmsg).(*ms.TimeTickMsg)
						log.Printf("[hardTimeTickBarrier] peer(%d)=%d\n", ttmsg.PeerId, ttmsg.Timestamp)

						oldT, ok := ttBarrier.peer2Tt[ttmsg.PeerId]
						if !ok {
							log.Printf("[hardTimeTickBarrier] Warning: peerID %d not exist\n", ttmsg.PeerId)
							continue
						}

						if oldT > state {
							log.Printf("[hardTimeTickBarrier] Warning: peer(%d) timestamp(%d) ahead\n",
								ttmsg.PeerId, ttmsg.Timestamp)
						}

						ttBarrier.peer2Tt[ttmsg.PeerId] = ttmsg.Timestamp

						newState := ttBarrier.minTimestamp()
						if newState > state {
							ttBarrier.outTt <- newState
							state = newState
						}
					}
				}
			default:
			}
		}
	}()
	return nil
}

func (ttBarrier *hardTimeTickBarrier) minTimestamp() Timestamp {
	tempMin := Timestamp(math.MaxUint64)
	for _, tt := range ttBarrier.peer2Tt {
		if tt < tempMin {
			tempMin = tt
		}
	}
	return tempMin
}

func NewHardTimeTickBarrier(ctx context.Context,
	ttStream *ms.MsgStream,
	peerIds []UniqueID) *hardTimeTickBarrier {

	if len(peerIds) <= 0 {
		log.Printf("[NewSoftTimeTickBarrier] Error: peerIds is empty!")
		return nil
	}

	sttbarrier := hardTimeTickBarrier{}
	sttbarrier.ttStream = *ttStream
	sttbarrier.outTt = make(chan Timestamp, 1024)
	sttbarrier.ctx = ctx
	sttbarrier.closed = false

	sttbarrier.peer2Tt = make(map[UniqueID]Timestamp)
	for _, id := range peerIds {
		sttbarrier.peer2Tt[id] = Timestamp(0)
	}
	if len(peerIds) != len(sttbarrier.peer2Tt) {
		log.Printf("[NewSoftTimeTickBarrier] Warning: there are duplicate peerIds!")
	}

	return &sttbarrier
}

func (ttBarrier *hardTimeTickBarrier) Close() {
	if ttBarrier.closeCh != nil {
		ttBarrier.closeCh <- struct{}{}
	}
	ttBarrier.closed = true
}
