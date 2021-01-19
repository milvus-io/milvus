package dataservice

type Server struct {
	segAllocator  segmentAllocator
	meta          *meta
	insertCMapper insertChannelMapper
	scheduler     *ddRequestScheduler
}
