package proxynode

import (
	"testing"

	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
)

func TestInsertChannelsMap_CreateInsertMsgStream(t *testing.T) {
	msFactory := msgstream.NewSimpleMsgStreamFactory()
	node := &ProxyNode{
		segAssigner: nil,
		msFactory:   msFactory,
	}
	m := newInsertChannelsMap(node)

	var err error

	err = m.CreateInsertMsgStream(1, []string{"1"})
	assert.Equal(t, nil, err)

	// duplicated
	err = m.CreateInsertMsgStream(1, []string{"1"})
	assert.NotEqual(t, nil, err)

	// duplicated
	err = m.CreateInsertMsgStream(1, []string{"1", "2"})
	assert.NotEqual(t, nil, err)

	// use same channels
	err = m.CreateInsertMsgStream(2, []string{"1"})
	assert.Equal(t, nil, err)

	err = m.CreateInsertMsgStream(3, []string{"3"})
	assert.Equal(t, nil, err)
}

func TestInsertChannelsMap_CloseInsertMsgStream(t *testing.T) {
	msFactory := msgstream.NewSimpleMsgStreamFactory()
	node := &ProxyNode{
		segAssigner: nil,
		msFactory:   msFactory,
	}
	m := newInsertChannelsMap(node)

	var err error

	_ = m.CreateInsertMsgStream(1, []string{"1"})
	_ = m.CreateInsertMsgStream(2, []string{"1"})
	_ = m.CreateInsertMsgStream(3, []string{"3"})

	// don't exist
	err = m.CloseInsertMsgStream(0)
	assert.NotEqual(t, nil, err)

	err = m.CloseInsertMsgStream(1)
	assert.Equal(t, nil, err)

	// close twice
	err = m.CloseInsertMsgStream(1)
	assert.NotEqual(t, nil, err)

	err = m.CloseInsertMsgStream(2)
	assert.Equal(t, nil, err)

	// close twice
	err = m.CloseInsertMsgStream(2)
	assert.NotEqual(t, nil, err)

	err = m.CloseInsertMsgStream(3)
	assert.Equal(t, nil, err)

	// close twice
	err = m.CloseInsertMsgStream(3)
	assert.NotEqual(t, nil, err)
}

func TestInsertChannelsMap_GetInsertChannels(t *testing.T) {
	msFactory := msgstream.NewSimpleMsgStreamFactory()
	node := &ProxyNode{
		segAssigner: nil,
		msFactory:   msFactory,
	}
	m := newInsertChannelsMap(node)

	var err error
	var channels []string

	_ = m.CreateInsertMsgStream(1, []string{"1"})
	_ = m.CreateInsertMsgStream(2, []string{"1"})
	_ = m.CreateInsertMsgStream(3, []string{"3"})

	// don't exist
	channels, err = m.GetInsertChannels(0)
	assert.NotEqual(t, nil, err)
	assert.Equal(t, 0, len(channels))

	channels, err = m.GetInsertChannels(1)
	assert.Equal(t, nil, err)
	assert.Equal(t, true, funcutil.SortedSliceEqual(channels, []string{"1"}))

	channels, err = m.GetInsertChannels(2)
	assert.Equal(t, nil, err)
	assert.Equal(t, true, funcutil.SortedSliceEqual(channels, []string{"1"}))

	channels, err = m.GetInsertChannels(3)
	assert.Equal(t, nil, err)
	assert.Equal(t, true, funcutil.SortedSliceEqual(channels, []string{"3"}))

	_ = m.CloseInsertMsgStream(1)
	channels, err = m.GetInsertChannels(1)
	assert.NotEqual(t, nil, err)
	assert.Equal(t, 0, len(channels))

	_ = m.CloseInsertMsgStream(2)
	channels, err = m.GetInsertChannels(2)
	assert.NotEqual(t, nil, err)
	assert.Equal(t, 0, len(channels))

	_ = m.CloseInsertMsgStream(3)
	channels, err = m.GetInsertChannels(3)
	assert.NotEqual(t, nil, err)
	assert.Equal(t, 0, len(channels))
}

func TestInsertChannelsMap_GetInsertMsgStream(t *testing.T) {
	msFactory := msgstream.NewSimpleMsgStreamFactory()
	node := &ProxyNode{
		segAssigner: nil,
		msFactory:   msFactory,
	}
	m := newInsertChannelsMap(node)

	var err error
	var stream msgstream.MsgStream

	_ = m.CreateInsertMsgStream(1, []string{"1"})
	_ = m.CreateInsertMsgStream(2, []string{"1"})
	_ = m.CreateInsertMsgStream(3, []string{"3"})

	// don't exist
	stream, err = m.GetInsertMsgStream(0)
	assert.NotEqual(t, nil, err)
	assert.Equal(t, nil, stream)

	stream, err = m.GetInsertMsgStream(1)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, stream)

	stream, err = m.GetInsertMsgStream(2)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, stream)

	stream, err = m.GetInsertMsgStream(3)
	assert.Equal(t, nil, err)
	assert.NotEqual(t, nil, stream)

	_ = m.CloseInsertMsgStream(1)
	stream, err = m.GetInsertMsgStream(1)
	assert.NotEqual(t, nil, err)
	assert.Equal(t, nil, stream)

	_ = m.CloseInsertMsgStream(2)
	stream, err = m.GetInsertMsgStream(2)
	assert.NotEqual(t, nil, err)
	assert.Equal(t, nil, stream)

	_ = m.CloseInsertMsgStream(3)
	stream, err = m.GetInsertMsgStream(3)
	assert.NotEqual(t, nil, err)
	assert.Equal(t, nil, stream)
}

func TestInsertChannelsMap_CloseAllMsgStream(t *testing.T) {
	msFactory := msgstream.NewSimpleMsgStreamFactory()
	node := &ProxyNode{
		segAssigner: nil,
		msFactory:   msFactory,
	}
	m := newInsertChannelsMap(node)

	var err error
	var stream msgstream.MsgStream
	var channels []string

	_ = m.CreateInsertMsgStream(1, []string{"1"})
	_ = m.CreateInsertMsgStream(2, []string{"1"})
	_ = m.CreateInsertMsgStream(3, []string{"3"})

	m.CloseAllMsgStream()

	err = m.CloseInsertMsgStream(1)
	assert.NotEqual(t, nil, err)

	err = m.CloseInsertMsgStream(2)
	assert.NotEqual(t, nil, err)

	err = m.CloseInsertMsgStream(3)
	assert.NotEqual(t, nil, err)

	channels, err = m.GetInsertChannels(1)
	assert.NotEqual(t, nil, err)
	assert.Equal(t, 0, len(channels))

	channels, err = m.GetInsertChannels(2)
	assert.NotEqual(t, nil, err)
	assert.Equal(t, 0, len(channels))

	channels, err = m.GetInsertChannels(3)
	assert.NotEqual(t, nil, err)
	assert.Equal(t, 0, len(channels))

	stream, err = m.GetInsertMsgStream(1)
	assert.NotEqual(t, nil, err)
	assert.Equal(t, nil, stream)

	stream, err = m.GetInsertMsgStream(2)
	assert.NotEqual(t, nil, err)
	assert.Equal(t, nil, stream)

	stream, err = m.GetInsertMsgStream(3)
	assert.NotEqual(t, nil, err)
	assert.Equal(t, nil, stream)
}
