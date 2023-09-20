package proxy

type (
	getVChannelsFuncType    = func(collectionID UniqueID) ([]vChan, error)
	removeDMLStreamFuncType = func(collectionID UniqueID) error
)

type mockChannelsMgr struct {
	channelsMgr
	getChannelsFunc func(collectionID UniqueID) ([]pChan, error)
	getVChannelsFuncType
	removeDMLStreamFuncType
}

func (m *mockChannelsMgr) getChannels(collectionID UniqueID) ([]pChan, error) {
	if m.getChannelsFunc != nil {
		return m.getChannelsFunc(collectionID)
	}
	return nil, nil
}

func (m *mockChannelsMgr) getVChannels(collectionID UniqueID) ([]vChan, error) {
	if m.getVChannelsFuncType != nil {
		return m.getVChannelsFuncType(collectionID)
	}
	return nil, nil
}

func (m *mockChannelsMgr) removeDMLStream(collectionID UniqueID) {
	if m.removeDMLStreamFuncType != nil {
		m.removeDMLStreamFuncType(collectionID)
	}
}

func newMockChannelsMgr() *mockChannelsMgr {
	return &mockChannelsMgr{}
}
