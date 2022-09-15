package session

type stats struct {
	segmentCnt int
	channelCnt int
}

func (s *stats) setSegmentCnt(cnt int) {
	s.segmentCnt = cnt
}

func (s *stats) getSegmentCnt() int {
	return s.segmentCnt
}

func (s *stats) setChannelCnt(cnt int) {
	s.channelCnt = cnt
}

func (s *stats) getChannelCnt() int {
	return s.channelCnt
}

func newStats() stats {
	return stats{}
}
