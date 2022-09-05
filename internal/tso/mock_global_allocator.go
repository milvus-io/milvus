package tso

import (
	"time"
)

type MockAllocator struct {
	Allocator
	InitializeF       func() error
	UpdateTSOF        func() error
	SetTSOF           func(tso uint64) error
	GenerateTSOF      func(count uint32) (uint64, error)
	ResetF            func()
	GetLastSavedTimeF func() time.Time
}

func (m MockAllocator) Initialize() error {
	return m.InitializeF()
}

func (m MockAllocator) UpdateTSO() error {
	return m.UpdateTSOF()
}

func (m MockAllocator) SetTSO(tso uint64) error {
	return m.SetTSOF(tso)
}

func (m MockAllocator) GenerateTSO(count uint32) (uint64, error) {
	return m.GenerateTSOF(count)
}

func (m MockAllocator) Reset() {
	m.ResetF()
}

func (m MockAllocator) GetLastSavedTime() time.Time {
	return m.GetLastSavedTimeF()
}

func NewMockAllocator() *MockAllocator {
	return &MockAllocator{}
}
