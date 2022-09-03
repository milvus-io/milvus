package allocator

type MockGIDAllocator struct {
	GIDAllocator
	AllocF    func(count uint32) (UniqueID, UniqueID, error)
	AllocOneF func() (UniqueID, error)
	UpdateIDF func() error
}

func (m MockGIDAllocator) Alloc(count uint32) (UniqueID, UniqueID, error) {
	return m.AllocF(count)
}

func (m MockGIDAllocator) AllocOne() (UniqueID, error) {
	return m.AllocOneF()
}

func (m MockGIDAllocator) UpdateID() error {
	return m.UpdateIDF()
}

func NewMockGIDAllocator() *MockGIDAllocator {
	return &MockGIDAllocator{}
}
