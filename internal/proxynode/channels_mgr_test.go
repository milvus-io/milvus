package proxynode

import (
	"testing"

	"github.com/milvus-io/milvus/internal/msgstream"

	"github.com/stretchr/testify/assert"
)

func TestNaiveUniqueIntGenerator_get(t *testing.T) {
	exists := make(map[int]bool)
	num := 10

	generator := newNaiveUniqueIntGenerator()

	for i := 0; i < num; i++ {
		g := generator.get()
		_, ok := exists[g]
		assert.False(t, ok)
		exists[g] = true
	}
}

func TestChannelsMgrImpl_getChannels(t *testing.T) {
	master := newMockGetChannelsService()
	query := newMockGetChannelsService()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgr(master.GetChannels, query.GetChannels, factory)
	defer mgr.removeAllDMLStream()

	collID := UniqueID(getUniqueIntGeneratorIns().get())
	_, err := mgr.getChannels(collID)
	assert.NotEqual(t, nil, err)

	err = mgr.createDMLMsgStream(collID)
	assert.Equal(t, nil, err)

	_, err = mgr.getChannels(collID)
	assert.Equal(t, nil, err)
}

func TestChannelsMgrImpl_getVChannels(t *testing.T) {
	master := newMockGetChannelsService()
	query := newMockGetChannelsService()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgr(master.GetChannels, query.GetChannels, factory)
	defer mgr.removeAllDMLStream()

	collID := UniqueID(getUniqueIntGeneratorIns().get())
	_, err := mgr.getVChannels(collID)
	assert.NotEqual(t, nil, err)

	err = mgr.createDMLMsgStream(collID)
	assert.Equal(t, nil, err)

	_, err = mgr.getVChannels(collID)
	assert.Equal(t, nil, err)
}

func TestChannelsMgrImpl_createDMLMsgStream(t *testing.T) {
	master := newMockGetChannelsService()
	query := newMockGetChannelsService()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgr(master.GetChannels, query.GetChannels, factory)
	defer mgr.removeAllDMLStream()

	collID := UniqueID(getUniqueIntGeneratorIns().get())
	_, err := mgr.getChannels(collID)
	assert.NotEqual(t, nil, err)
	_, err = mgr.getVChannels(collID)
	assert.NotEqual(t, nil, err)

	err = mgr.createDMLMsgStream(collID)
	assert.Equal(t, nil, err)

	_, err = mgr.getChannels(collID)
	assert.Equal(t, nil, err)
	_, err = mgr.getVChannels(collID)
	assert.Equal(t, nil, err)
}

func TestChannelsMgrImpl_getDMLMsgStream(t *testing.T) {
	master := newMockGetChannelsService()
	query := newMockGetChannelsService()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgr(master.GetChannels, query.GetChannels, factory)
	defer mgr.removeAllDMLStream()

	collID := UniqueID(getUniqueIntGeneratorIns().get())
	_, err := mgr.getDMLStream(collID)
	assert.NotEqual(t, nil, err)

	err = mgr.createDMLMsgStream(collID)
	assert.Equal(t, nil, err)

	_, err = mgr.getDMLStream(collID)
	assert.Equal(t, nil, err)
}

func TestChannelsMgrImpl_removeDMLMsgStream(t *testing.T) {
	master := newMockGetChannelsService()
	query := newMockGetChannelsService()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgr(master.GetChannels, query.GetChannels, factory)
	defer mgr.removeAllDMLStream()

	collID := UniqueID(getUniqueIntGeneratorIns().get())
	_, err := mgr.getDMLStream(collID)
	assert.NotEqual(t, nil, err)

	err = mgr.removeDMLStream(collID)
	assert.NotEqual(t, nil, err)

	err = mgr.createDMLMsgStream(collID)
	assert.Equal(t, nil, err)

	_, err = mgr.getDMLStream(collID)
	assert.Equal(t, nil, err)

	err = mgr.removeDMLStream(collID)
	assert.Equal(t, nil, err)

	_, err = mgr.getDMLStream(collID)
	assert.NotEqual(t, nil, err)
}

func TestChannelsMgrImpl_removeAllDMLMsgStream(t *testing.T) {
	master := newMockGetChannelsService()
	query := newMockGetChannelsService()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgr(master.GetChannels, query.GetChannels, factory)
	defer mgr.removeAllDMLStream()

	num := 10
	for i := 0; i < num; i++ {
		collID := UniqueID(getUniqueIntGeneratorIns().get())
		err := mgr.createDMLMsgStream(collID)
		assert.Equal(t, nil, err)
	}
}

func TestChannelsMgrImpl_createDQLMsgStream(t *testing.T) {
	master := newMockGetChannelsService()
	query := newMockGetChannelsService()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgr(master.GetChannels, query.GetChannels, factory)
	defer mgr.removeAllDMLStream()

	collID := UniqueID(getUniqueIntGeneratorIns().get())
	_, err := mgr.getChannels(collID)
	assert.NotEqual(t, nil, err)
	_, err = mgr.getVChannels(collID)
	assert.NotEqual(t, nil, err)

	err = mgr.createDQLStream(collID)
	assert.Equal(t, nil, err)

	_, err = mgr.getChannels(collID)
	assert.Equal(t, nil, err)
	_, err = mgr.getVChannels(collID)
	assert.Equal(t, nil, err)
}

func TestChannelsMgrImpl_getDQLMsgStream(t *testing.T) {
	master := newMockGetChannelsService()
	query := newMockGetChannelsService()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgr(master.GetChannels, query.GetChannels, factory)
	defer mgr.removeAllDMLStream()

	collID := UniqueID(getUniqueIntGeneratorIns().get())
	_, err := mgr.getDQLStream(collID)
	assert.NotEqual(t, nil, err)

	err = mgr.createDQLStream(collID)
	assert.Equal(t, nil, err)

	_, err = mgr.getDQLStream(collID)
	assert.Equal(t, nil, err)
}

func TestChannelsMgrImpl_removeDQLMsgStream(t *testing.T) {
	master := newMockGetChannelsService()
	query := newMockGetChannelsService()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgr(master.GetChannels, query.GetChannels, factory)
	defer mgr.removeAllDMLStream()

	collID := UniqueID(getUniqueIntGeneratorIns().get())
	_, err := mgr.getDQLStream(collID)
	assert.NotEqual(t, nil, err)

	err = mgr.removeDQLStream(collID)
	assert.NotEqual(t, nil, err)

	err = mgr.createDQLStream(collID)
	assert.Equal(t, nil, err)

	_, err = mgr.getDQLStream(collID)
	assert.Equal(t, nil, err)

	err = mgr.removeDQLStream(collID)
	assert.Equal(t, nil, err)

	_, err = mgr.getDQLStream(collID)
	assert.NotEqual(t, nil, err)
}

func TestChannelsMgrImpl_removeAllDQLMsgStream(t *testing.T) {
	master := newMockGetChannelsService()
	query := newMockGetChannelsService()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgr(master.GetChannels, query.GetChannels, factory)
	defer mgr.removeAllDMLStream()

	num := 10
	for i := 0; i < num; i++ {
		collID := UniqueID(getUniqueIntGeneratorIns().get())
		err := mgr.createDQLStream(collID)
		assert.Equal(t, nil, err)
	}
}
