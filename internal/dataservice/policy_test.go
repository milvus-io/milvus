package dataservice

import (
	"testing"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/stretchr/testify/assert"
)

func TestWatchRestartsPolicy(t *testing.T) {
	p := newWatchRestartsStartupPolicy()
	c := make(map[string]*datapb.DataNodeInfo)
	c["localhost:1111"] = &datapb.DataNodeInfo{
		Address: "localhost:1111",
		Version: 0,
		Channels: []*datapb.ChannelStatus{
			{
				Name:         "vch1",
				State:        datapb.ChannelWatchState_Complete,
				CollectionID: 0,
			},
		},
	}

	c["localhost:2222"] = &datapb.DataNodeInfo{
		Address: "localhost:2222",
		Version: 0,
		Channels: []*datapb.ChannelStatus{
			{
				Name:         "vch2",
				State:        datapb.ChannelWatchState_Complete,
				CollectionID: 0,
			},
		},
	}

	dchange := &clusterDeltaChange{
		newNodes: []string{},
		offlines: []string{},
		restarts: []string{"localhost:2222"},
	}

	nodes := p.apply(c, dchange)
	assert.EqualValues(t, 1, len(nodes))
	assert.EqualValues(t, datapb.ChannelWatchState_Uncomplete, nodes[0].Channels[0].State)
}
