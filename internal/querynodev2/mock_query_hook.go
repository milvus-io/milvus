package querynodev2

import "github.com/stretchr/testify/mock"

type MockQueryHook struct {
	mock.Mock
}

type MockQueryHookExpecter struct {
	mock *mock.Mock
}

func (_m *MockQueryHook) EXPECT() *MockQueryHookExpecter {
	return &MockQueryHookExpecter{mock: &_m.Mock}
}

func (_m *MockQueryHook) Run(params map[string]any) error {
	ret := _m.Called(params)

	var r0 error
	if rf, ok := ret.Get(0).(func(params map[string]any) error); ok {
		r0 = rf(params)
	} else {
		r0 = ret.Error(0)
	}
	return r0
}

// Run is a helper method to define mock.On call
func (_e *MockQueryHookExpecter) Run(params any) *MockQueryHookRunCall {
	return &MockQueryHookRunCall{Call: _e.mock.On("Run", params)}
}

// MockQueryHook_Run_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Run'
type MockQueryHookRunCall struct {
	*mock.Call
}

func (_c *MockQueryHookRunCall) Run(run func(params map[string]any)) *MockQueryHookRunCall {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(map[string]any))
	})
	return _c
}

func (_c *MockQueryHookRunCall) Return(_a0 error) *MockQueryHookRunCall {
	_c.Call.Return(_a0)
	return _c
}

func (_m *MockQueryHook) Init(param string) error {
	ret := _m.Called(param)
	var r0 error
	if rf, ok := ret.Get(0).(func(string) error); ok {
		r0 = rf(param)
	} else {
		r0 = ret.Error(0)
	}
	return r0
}

// Init is a helper method to define mock.On call
func (_e *MockQueryHookExpecter) Init(params any) *MockQueryHookRunCall {
	return &MockQueryHookRunCall{Call: _e.mock.On("Init", params)}
}

// MockQueryHook_Init_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'Run'
type MockQueryHookInitCall struct {
	*mock.Call
}

func (_c *MockQueryHookInitCall) Run(run func(params string)) *MockQueryHookInitCall {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(string))
	})
	return _c
}

func (_c *MockQueryHookInitCall) Return(_a0 error) *MockQueryHookInitCall {
	_c.Call.Return(_a0)
	return _c
}
