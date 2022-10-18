package proxy

type getVChannelsFuncType = func(collectionID UniqueID) ([]vChan, error)
type removeDMLStreamFuncType = func(collectionID UniqueID) error

type mockChannelsMgr struct {
	channelsMgr
	getVChannelsFuncType
	removeDMLStreamFuncType
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
