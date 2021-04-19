package master

import (
	"context"
	"log"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	ms "github.com/zilliztech/milvus-distributed/internal/msgstream"
)

//type ddRequestScheduler interface {}

//type ddReqFIFOScheduler struct {}

type ddRequestScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc

	globalIDAllocator func() (UniqueID, error)
	reqQueue          chan task
	scheduleTimeStamp Timestamp
	ddMsgStream       ms.MsgStream
}

func NewDDRequestScheduler(ctx context.Context) *ddRequestScheduler {
	const channelSize = 1024

	ctx2, cancel := context.WithCancel(ctx)

	rs := ddRequestScheduler{
		ctx:      ctx2,
		cancel:   cancel,
		reqQueue: make(chan task, channelSize),
	}
	return &rs
}

func (rs *ddRequestScheduler) Enqueue(task task) error {
	rs.reqQueue <- task
	return nil
}

func (rs *ddRequestScheduler) SetIDAllocator(allocGlobalID func() (UniqueID, error)) {
	rs.globalIDAllocator = allocGlobalID
}

func (rs *ddRequestScheduler) SetDDMsgStream(ddStream ms.MsgStream) {
	rs.ddMsgStream = ddStream
}

func (rs *ddRequestScheduler) scheduleLoop() {
	for {
		select {
		case task := <-rs.reqQueue:
			err := rs.schedule(task)
			if err != nil {
				log.Println(err)
			}
		case <-rs.ctx.Done():
			log.Print("server is closed, exit task execution loop")
			return
		}
	}
}

func (rs *ddRequestScheduler) schedule(t task) error {
	timeStamp, err := t.Ts()
	if err != nil {
		log.Println(err)
		return err
	}
	if timeStamp < rs.scheduleTimeStamp {
		t.Notify(errors.Errorf("input timestamp = %d, schduler timestamp = %d", timeStamp, rs.scheduleTimeStamp))
	} else {
		rs.scheduleTimeStamp = timeStamp
		err = t.Execute()
		t.Notify(err)
	}
	return nil
}

func (rs *ddRequestScheduler) Start() error {
	go rs.scheduleLoop()
	return nil
}

func (rs *ddRequestScheduler) Close() {
	rs.cancel()
}
