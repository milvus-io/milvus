package querynodev2

import "github.com/stretchr/testify/mock"

type MockQueryHook struct {
	mock.Mock
}

type MockQueryHook_Expecter struct {
	mock *mock.Mock
}

func (_m *MockQueryHook) EXPECT() *MockQueryHook_Expecter {
	return &MockQueryHook_Expecter{mock: &_m.Mock}
}

// DeleteTuningConfig provides a mock function with given fields: _a0
func (_m *MockQueryHook) DeleteTuningConfig(_a0 string) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockQueryHook_DeleteTuningConfig_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'DeleteTuningConfig'
type MockQueryHook_DeleteTuningConfig_Call struct {
	*mock.Call
}

// DeleteTuningConfig is a helper method to define mock.On call
//   - _a0 string
func (_e *MockQueryHook_Expecter) DeleteTuningConfig(_a0 interface{}) *MockQueryHook_DeleteTuningConfig_Call {
	return &MockQueryHook_DeleteTuningConfig_Call{Call: _e.mock.On("DeleteTuningConfig", _a0)}
}

func (_c *MockQueryHook_DeleteTuningConfig_Call) Run(run func(_a0 string)) *MockQueryHook_DeleteTuningConfig_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *MockQueryHook_DeleteTuningConfig_Call) Return(_a0 error) *MockQueryHook_DeleteTuningConfig_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockQueryHook_DeleteTuningConfig_Call) RunAndReturn(run func(string) error) *MockQueryHook_DeleteTuningConfig_Call {
	_c.Call.Return(run)
	return _c
}

// Init provides a mock function with given fields: _a0
func (_m *MockQueryHook) Init(_a0 string) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockQueryHook_Init_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Init'
type MockQueryHook_Init_Call struct {
	*mock.Call
}

// Init is a helper method to define mock.On call
//   - _a0 string
func (_e *MockQueryHook_Expecter) Init(_a0 interface{}) *MockQueryHook_Init_Call {
	return &MockQueryHook_Init_Call{Call: _e.mock.On("Init", _a0)}
}

func (_c *MockQueryHook_Init_Call) Run(run func(_a0 string)) *MockQueryHook_Init_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *MockQueryHook_Init_Call) Return(_a0 error) *MockQueryHook_Init_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockQueryHook_Init_Call) RunAndReturn(run func(string) error) *MockQueryHook_Init_Call {
	_c.Call.Return(run)
	return _c
}

// InitTuningConfig provides a mock function with given fields: _a0
func (_m *MockQueryHook) InitTuningConfig(_a0 map[string]string) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(map[string]string) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockQueryHook_InitTuningConfig_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'InitTuningConfig'
type MockQueryHook_InitTuningConfig_Call struct {
	*mock.Call
}

// InitTuningConfig is a helper method to define mock.On call
//   - _a0 map[string]string
func (_e *MockQueryHook_Expecter) InitTuningConfig(_a0 interface{}) *MockQueryHook_InitTuningConfig_Call {
	return &MockQueryHook_InitTuningConfig_Call{Call: _e.mock.On("InitTuningConfig", _a0)}
}

func (_c *MockQueryHook_InitTuningConfig_Call) Run(run func(_a0 map[string]string)) *MockQueryHook_InitTuningConfig_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(map[string]string))
	})
	return _c
}

func (_c *MockQueryHook_InitTuningConfig_Call) Return(_a0 error) *MockQueryHook_InitTuningConfig_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockQueryHook_InitTuningConfig_Call) RunAndReturn(run func(map[string]string) error) *MockQueryHook_InitTuningConfig_Call {
	_c.Call.Return(run)
	return _c
}

// Run provides a mock function with given fields: _a0
func (_m *MockQueryHook) Run(_a0 map[string]interface{}) error {
	ret := _m.Called(_a0)

	var r0 error
	if rf, ok := ret.Get(0).(func(map[string]interface{}) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// MockQueryHook_Run_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Run'
type MockQueryHook_Run_Call struct {
	*mock.Call
}

// Run is a helper method to define mock.On call
//   - _a0 map[string]interface{}
func (_e *MockQueryHook_Expecter) Run(_a0 interface{}) *MockQueryHook_Run_Call {
	return &MockQueryHook_Run_Call{Call: _e.mock.On("Run", _a0)}
}

func (_c *MockQueryHook_Run_Call) Run(run func(_a0 map[string]interface{})) *MockQueryHook_Run_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(map[string]interface{}))
	})
	return _c
}

func (_c *MockQueryHook_Run_Call) Return(_a0 error) *MockQueryHook_Run_Call {
	_c.Call.Return(_a0)
	return _c
}

func (_c *MockQueryHook_Run_Call) RunAndReturn(run func(map[string]interface{}) error) *MockQueryHook_Run_Call {
	_c.Call.Return(run)
	return _c
}

type mockConstructorTestingTNewMockQueryHook interface {
	mock.TestingT
	Cleanup(func())
}

// NewMockQueryHook creates a new instance of MockQueryHook. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewMockQueryHook(t mockConstructorTestingTNewMockQueryHook) *MockQueryHook {
	mock := &MockQueryHook{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
