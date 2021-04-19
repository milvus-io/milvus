package dataservice

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChannelAllocation(t *testing.T) {
	Params.Init()
	Params.InsertChannelNumPerCollection = 4
	manager := newInsertChannelManager()
	cases := []struct {
		collectionID   UniqueID
		groupNum       int
		expectGroupNum int
		success        bool
	}{
		{1, 4, 4, true},
		{1, 4, 4, false},
		{2, 1, 1, true},
		{3, 5, 4, true},
	}
	for _, c := range cases {
		channels, err := manager.AllocChannels(c.collectionID, c.expectGroupNum)
		if !c.success {
			assert.NotNil(t, err)
			continue
		}
		assert.Nil(t, err)
		assert.EqualValues(t, c.expectGroupNum, len(channels))
		total := 0
		for _, channel := range channels {
			total += len(channel)
		}
		assert.EqualValues(t, Params.InsertChannelNumPerCollection, total)
	}
}
