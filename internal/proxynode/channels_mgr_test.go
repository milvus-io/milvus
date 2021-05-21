package proxynode

import (
	"fmt"
	"math/rand"
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

type mockMaster struct {
	collectionID2Channels map[UniqueID]map[vChan]pChan
}

func newMockMaster() *mockMaster {
	return &mockMaster{
		collectionID2Channels: make(map[UniqueID]map[vChan]pChan),
	}
}

func genUniqueStr() string {
	l := rand.Uint64() % 100
	b := make([]byte, l)
	if _, err := rand.Read(b); err != nil {
		return ""
	}
	return fmt.Sprintf("%X", b)
}

func (m *mockMaster) GetChannels(collectionID UniqueID) (map[vChan]pChan, error) {
	channels, ok := m.collectionID2Channels[collectionID]
	if ok {
		return channels, nil
	}

	channels = make(map[vChan]pChan)
	l := rand.Uint64() % 10
	for i := 0; uint64(i) < l; i++ {
		channels[genUniqueStr()] = genUniqueStr()
	}

	m.collectionID2Channels[collectionID] = channels
	return channels, nil
}

func TestChannelsMgrImpl_getChannels(t *testing.T) {
	master := newMockMaster()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgr(master, factory)
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
	master := newMockMaster()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgr(master, factory)
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
	master := newMockMaster()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgr(master, factory)
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
	master := newMockMaster()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgr(master, factory)
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
	master := newMockMaster()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgr(master, factory)
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
	master := newMockMaster()
	factory := msgstream.NewSimpleMsgStreamFactory()
	mgr := newChannelsMgr(master, factory)
	defer mgr.removeAllDMLStream()

	num := 10
	for i := 0; i < num; i++ {
		collID := UniqueID(getUniqueIntGeneratorIns().get())
		err := mgr.createDMLMsgStream(collID)
		assert.Equal(t, nil, err)
	}
}
