package meta

type DistributionManager struct {
	*SegmentDistManager
	*ChannelDistManager
	*LeaderViewManager
}

func NewDistributionManager() *DistributionManager {
	return &DistributionManager{
		SegmentDistManager: NewSegmentDistManager(),
		ChannelDistManager: NewChannelDistManager(),
		LeaderViewManager:  NewLeaderViewManager(),
	}
}
