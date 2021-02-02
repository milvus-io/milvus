package dataservice

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetChannel(t *testing.T) {
	Params.Init()
	Params.InsertChannelNumPerCollection = 4
	Params.InsertChannelPrefixName = "channel"
	manager := newInsertChannelManager()
	channels, err := manager.GetChannels(1)
	assert.Nil(t, err)
	assert.EqualValues(t, Params.InsertChannelNumPerCollection, len(channels))
	for i := 0; i < len(channels); i++ {
		assert.EqualValues(t, Params.InsertChannelPrefixName+strconv.Itoa(i), channels[i])
	}
}
