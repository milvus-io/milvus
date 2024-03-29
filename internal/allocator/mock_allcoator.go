// Code generated by mockery v2.32.4. DO NOT EDIT.

package allocator

import mock "github.com/stretchr/testify/mock"

// MockAllocator is an autogenerated mock type for the Interface type
type MockAllocator struct {
	mock.Mock
}

type MockAllocator_Expecter struct {
	mock *mock.Mock
}

func (_m *MockAllocator) EXPECT() *MockAllocator_Expecter {
	return &MockAllocator_Expecter{mock: &_m.Mock}
}

// Alloc provides a mock function with given fields: count
func (_m *MockAllocator) Alloc(count uint32) (int64, int64, error) {
	ret := _m.Called(count)

	var r0 int64
	var r1 int64
	var r2 error
	if rf, ok := ret.Get(0).(func(uint32) (int64, int64, error)); ok {
		return rf(count)
	}
	if rf, ok := ret.Get(0).(func(uint32) int64); ok {
		r0 = rf(count)
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func(uint32) int64); ok {
		r1 = rf(count)
	} else {
		r1 = ret.Get(1).(int64)
	}

	if rf, ok := ret.Get(2).(func(uint32) error); ok {
		r2 = rf(count)
	} else {
		r2 = ret.Error(2)
	}

	return r0, r1, r2
}

// MockAllocator_Alloc_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Alloc'
type MockAllocator_Alloc_Call struct {
	*mock.Call
}

// Alloc is a helper method to define mock.On call
//   - count uint32
func (_e *MockAllocator_Expecter) Alloc(count interface{}) *MockAllocator_Alloc_Call {
	return &MockAllocator_Alloc_Call{Call: _e.mock.On("Alloc", count)}
}

func (_c *MockAllocator_Alloc_Call) Run(run func(count uint32)) *MockAllocator_Alloc_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(uint32))
	})
	return _c
}

func (_c *MockAllocator_Alloc_Call) Return(_a0 int64, _a1 int64, _a2 error) *MockAllocator_Alloc_Call {
	_c.Call.Return(_a0, _a1, _a2)
	return _c
}

func (_c *MockAllocator_Alloc_Call) RunAndReturn(run func(uint32) (int64, int64, error)) *MockAllocator_Alloc_Call {
	_c.Call.Return(run)
	return _c
}

// AllocOne provides a mock function with given fields:
func (_m *MockAllocator) AllocOne() (int64, error) {
	ret := _m.Called()

	var r0 int64
	var r1 error
	if rf, ok := ret.Get(0).(func() (int64, error)); ok {
		return rf()
	}
	if rf, ok := ret.Get(0).(func() int64); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(int64)
	}

	if rf, ok := ret.Get(1).(func() error); ok {
		r1 = rf()
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockAllocator_AllocOne_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'AllocOne'
type MockAllocator_AllocOne_Call struct {
	*mock.Call
}

// AllocOne is a helper method to define mock.On call
func (_e *MockAllocator_Expecter) AllocOne() *MockAllocator_AllocOne_Call {
	return &MockAllocator_AllocOne_Call{Call: _e.mock.On("AllocOne")}
}

func (_c *MockAllocator_AllocOne_Call) Run(run func()) *MockAllocator_AllocOne_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run()
	})
	return _c
}

func (_c *MockAllocator_AllocOne_Call) Return(_a0 int64, _a1 error) *MockAllocator_AllocOne_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockAllocator_AllocOne_Call) RunAndReturn(run func() (int64, error)) *MockAllocator_AllocOne_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockAllocator creates a new instance of MockAllocator. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockAllocator(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockAllocator {
	mock := &MockAllocator{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
