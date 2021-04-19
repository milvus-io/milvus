package master

type ddRequestScheduler struct {
	globalIDAllocator func() (UniqueID, error)
	reqQueue          chan task
	scheduleTimeStamp Timestamp
}

func NewDDRequestScheduler(allocGlobalID func() (UniqueID, error)) *ddRequestScheduler {
	const channelSize = 1024

	rs := ddRequestScheduler{
		globalIDAllocator: allocGlobalID,
		reqQueue:          make(chan task, channelSize),
	}
	return &rs
}

func (rs *ddRequestScheduler) Enqueue(task task) error {
	rs.reqQueue <- task
	return nil
}
