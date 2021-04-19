package dataservice

import (
	"strconv"
	"sync"
)

type (
	insertChannelManager struct {
		mu            sync.RWMutex
		count         int
		channelGroups map[UniqueID][]string // collection id to channel ranges
	}
)

func newInsertChannelManager() *insertChannelManager {
	return &insertChannelManager{
		count:         0,
		channelGroups: make(map[UniqueID][]string),
	}
}

func (cm *insertChannelManager) GetChannels(collectionID UniqueID) ([]string, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if _, ok := cm.channelGroups[collectionID]; ok {
		return cm.channelGroups[collectionID], nil
	}
	channels := Params.InsertChannelNumPerCollection
	cg := make([]string, channels)
	var i int64 = 0
	for ; i < channels; i++ {
		cg[i] = Params.InsertChannelPrefixName + strconv.Itoa(cm.count)
		cm.count++
	}
	cm.channelGroups[collectionID] = cg
	return cg, nil
}
