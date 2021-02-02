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
	}{
		{1, 4, 4},
		{1, 4, 4},
		{2, 1, 1},
		{3, 5, 4},
	}
	for _, c := range cases {
		channels, err := manager.GetChannels(c.collectionID, c.expectGroupNum)
		assert.Nil(t, err)
		assert.EqualValues(t, c.expectGroupNum, len(channels))
		total := 0
		for _, channel := range channels {
			total += len(channel)
		}
		assert.EqualValues(t, Params.InsertChannelNumPerCollection, total)
	}
}
