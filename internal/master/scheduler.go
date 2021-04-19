package master

import "math/rand"

type ddRequestScheduler struct {
	reqQueue          chan task
	scheduleTimeStamp Timestamp
}

func NewDDRequestScheduler() *ddRequestScheduler {
	const channelSize = 1024

	rs := ddRequestScheduler{
		reqQueue: make(chan task, channelSize),
	}
	return &rs
}

func (rs *ddRequestScheduler) Enqueue(task task) error {
	rs.reqQueue <- task
	return nil
}

//TODO, allocGlobalId
func allocGlobalId() (UniqueID, error) {
	return rand.Int63(), nil
}
