// Code generated by mockery v2.53.3. DO NOT EDIT.

package broker

import (
	context "context"

	milvuspb "github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	mock "github.com/stretchr/testify/mock"

	rootcoordpb "github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
)

// MockBroker is an autogenerated mock type for the Broker type
type MockBroker struct {
	mock.Mock
}

type MockBroker_Expecter struct {
	mock *mock.Mock
}

func (_m *MockBroker) EXPECT() *MockBroker_Expecter {
	return &MockBroker_Expecter{mock: &_m.Mock}
}

// DescribeCollectionInternal provides a mock function with given fields: ctx, collectionID
func (_m *MockBroker) DescribeCollectionInternal(ctx context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error) {
	ret := _m.Called(ctx, collectionID)

	if len(ret) == 0 {
		panic("no return value specified for DescribeCollectionInternal")
	}

	var r0 *milvuspb.DescribeCollectionResponse
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, int64) (*milvuspb.DescribeCollectionResponse, error)); ok {
		return rf(ctx, collectionID)
	}
	if rf, ok := ret.Get(0).(func(context.Context, int64) *milvuspb.DescribeCollectionResponse); ok {
		r0 = rf(ctx, collectionID)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*milvuspb.DescribeCollectionResponse)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, int64) error); ok {
		r1 = rf(ctx, collectionID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockBroker_DescribeCollectionInternal_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'DescribeCollectionInternal'
type MockBroker_DescribeCollectionInternal_Call struct {
	*mock.Call
}

// DescribeCollectionInternal is a helper method to define mock.On call
//   - ctx context.Context
//   - collectionID int64
func (_e *MockBroker_Expecter) DescribeCollectionInternal(ctx interface{}, collectionID interface{}) *MockBroker_DescribeCollectionInternal_Call {
	return &MockBroker_DescribeCollectionInternal_Call{Call: _e.mock.On("DescribeCollectionInternal", ctx, collectionID)}
}

func (_c *MockBroker_DescribeCollectionInternal_Call) Run(run func(ctx context.Context, collectionID int64)) *MockBroker_DescribeCollectionInternal_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int64))
	})
	return _c
}

func (_c *MockBroker_DescribeCollectionInternal_Call) Return(_a0 *milvuspb.DescribeCollectionResponse, _a1 error) *MockBroker_DescribeCollectionInternal_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockBroker_DescribeCollectionInternal_Call) RunAndReturn(run func(context.Context, int64) (*milvuspb.DescribeCollectionResponse, error)) *MockBroker_DescribeCollectionInternal_Call {
	_c.Call.Return(run)
	return _c
}

// HasCollection provides a mock function with given fields: ctx, collectionID
func (_m *MockBroker) HasCollection(ctx context.Context, collectionID int64) (bool, error) {
	ret := _m.Called(ctx, collectionID)

	if len(ret) == 0 {
		panic("no return value specified for HasCollection")
	}

	var r0 bool
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, int64) (bool, error)); ok {
		return rf(ctx, collectionID)
	}
	if rf, ok := ret.Get(0).(func(context.Context, int64) bool); ok {
		r0 = rf(ctx, collectionID)
	} else {
		r0 = ret.Get(0).(bool)
	}

	if rf, ok := ret.Get(1).(func(context.Context, int64) error); ok {
		r1 = rf(ctx, collectionID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockBroker_HasCollection_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'HasCollection'
type MockBroker_HasCollection_Call struct {
	*mock.Call
}

// HasCollection is a helper method to define mock.On call
//   - ctx context.Context
//   - collectionID int64
func (_e *MockBroker_Expecter) HasCollection(ctx interface{}, collectionID interface{}) *MockBroker_HasCollection_Call {
	return &MockBroker_HasCollection_Call{Call: _e.mock.On("HasCollection", ctx, collectionID)}
}

func (_c *MockBroker_HasCollection_Call) Run(run func(ctx context.Context, collectionID int64)) *MockBroker_HasCollection_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int64))
	})
	return _c
}

func (_c *MockBroker_HasCollection_Call) Return(_a0 bool, _a1 error) *MockBroker_HasCollection_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockBroker_HasCollection_Call) RunAndReturn(run func(context.Context, int64) (bool, error)) *MockBroker_HasCollection_Call {
	_c.Call.Return(run)
	return _c
}

// ListDatabases provides a mock function with given fields: ctx
func (_m *MockBroker) ListDatabases(ctx context.Context) (*milvuspb.ListDatabasesResponse, error) {
	ret := _m.Called(ctx)

	if len(ret) == 0 {
		panic("no return value specified for ListDatabases")
	}

	var r0 *milvuspb.ListDatabasesResponse
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context) (*milvuspb.ListDatabasesResponse, error)); ok {
		return rf(ctx)
	}
	if rf, ok := ret.Get(0).(func(context.Context) *milvuspb.ListDatabasesResponse); ok {
		r0 = rf(ctx)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*milvuspb.ListDatabasesResponse)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(ctx)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockBroker_ListDatabases_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ListDatabases'
type MockBroker_ListDatabases_Call struct {
	*mock.Call
}

// ListDatabases is a helper method to define mock.On call
//   - ctx context.Context
func (_e *MockBroker_Expecter) ListDatabases(ctx interface{}) *MockBroker_ListDatabases_Call {
	return &MockBroker_ListDatabases_Call{Call: _e.mock.On("ListDatabases", ctx)}
}

func (_c *MockBroker_ListDatabases_Call) Run(run func(ctx context.Context)) *MockBroker_ListDatabases_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context))
	})
	return _c
}

func (_c *MockBroker_ListDatabases_Call) Return(_a0 *milvuspb.ListDatabasesResponse, _a1 error) *MockBroker_ListDatabases_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockBroker_ListDatabases_Call) RunAndReturn(run func(context.Context) (*milvuspb.ListDatabasesResponse, error)) *MockBroker_ListDatabases_Call {
	_c.Call.Return(run)
	return _c
}

// ShowCollectionIDs provides a mock function with given fields: ctx, dbNames
func (_m *MockBroker) ShowCollectionIDs(ctx context.Context, dbNames ...string) (*rootcoordpb.ShowCollectionIDsResponse, error) {
	_va := make([]interface{}, len(dbNames))
	for _i := range dbNames {
		_va[_i] = dbNames[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	if len(ret) == 0 {
		panic("no return value specified for ShowCollectionIDs")
	}

	var r0 *rootcoordpb.ShowCollectionIDsResponse
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, ...string) (*rootcoordpb.ShowCollectionIDsResponse, error)); ok {
		return rf(ctx, dbNames...)
	}
	if rf, ok := ret.Get(0).(func(context.Context, ...string) *rootcoordpb.ShowCollectionIDsResponse); ok {
		r0 = rf(ctx, dbNames...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*rootcoordpb.ShowCollectionIDsResponse)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, ...string) error); ok {
		r1 = rf(ctx, dbNames...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockBroker_ShowCollectionIDs_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ShowCollectionIDs'
type MockBroker_ShowCollectionIDs_Call struct {
	*mock.Call
}

// ShowCollectionIDs is a helper method to define mock.On call
//   - ctx context.Context
//   - dbNames ...string
func (_e *MockBroker_Expecter) ShowCollectionIDs(ctx interface{}, dbNames ...interface{}) *MockBroker_ShowCollectionIDs_Call {
	return &MockBroker_ShowCollectionIDs_Call{Call: _e.mock.On("ShowCollectionIDs",
		append([]interface{}{ctx}, dbNames...)...)}
}

func (_c *MockBroker_ShowCollectionIDs_Call) Run(run func(ctx context.Context, dbNames ...string)) *MockBroker_ShowCollectionIDs_Call {
	_c.Call.Run(func(args mock.Arguments) {
		variadicArgs := make([]string, len(args)-1)
		for i, a := range args[1:] {
			if a != nil {
				variadicArgs[i] = a.(string)
			}
		}
		run(args[0].(context.Context), variadicArgs...)
	})
	return _c
}

func (_c *MockBroker_ShowCollectionIDs_Call) Return(_a0 *rootcoordpb.ShowCollectionIDsResponse, _a1 error) *MockBroker_ShowCollectionIDs_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockBroker_ShowCollectionIDs_Call) RunAndReturn(run func(context.Context, ...string) (*rootcoordpb.ShowCollectionIDsResponse, error)) *MockBroker_ShowCollectionIDs_Call {
	_c.Call.Return(run)
	return _c
}

// ShowCollections provides a mock function with given fields: ctx, dbName
func (_m *MockBroker) ShowCollections(ctx context.Context, dbName string) (*milvuspb.ShowCollectionsResponse, error) {
	ret := _m.Called(ctx, dbName)

	if len(ret) == 0 {
		panic("no return value specified for ShowCollections")
	}

	var r0 *milvuspb.ShowCollectionsResponse
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string) (*milvuspb.ShowCollectionsResponse, error)); ok {
		return rf(ctx, dbName)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string) *milvuspb.ShowCollectionsResponse); ok {
		r0 = rf(ctx, dbName)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*milvuspb.ShowCollectionsResponse)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string) error); ok {
		r1 = rf(ctx, dbName)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockBroker_ShowCollections_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ShowCollections'
type MockBroker_ShowCollections_Call struct {
	*mock.Call
}

// ShowCollections is a helper method to define mock.On call
//   - ctx context.Context
//   - dbName string
func (_e *MockBroker_Expecter) ShowCollections(ctx interface{}, dbName interface{}) *MockBroker_ShowCollections_Call {
	return &MockBroker_ShowCollections_Call{Call: _e.mock.On("ShowCollections", ctx, dbName)}
}

func (_c *MockBroker_ShowCollections_Call) Run(run func(ctx context.Context, dbName string)) *MockBroker_ShowCollections_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(string))
	})
	return _c
}

func (_c *MockBroker_ShowCollections_Call) Return(_a0 *milvuspb.ShowCollectionsResponse, _a1 error) *MockBroker_ShowCollections_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockBroker_ShowCollections_Call) RunAndReturn(run func(context.Context, string) (*milvuspb.ShowCollectionsResponse, error)) *MockBroker_ShowCollections_Call {
	_c.Call.Return(run)
	return _c
}

// ShowPartitionsInternal provides a mock function with given fields: ctx, collectionID
func (_m *MockBroker) ShowPartitionsInternal(ctx context.Context, collectionID int64) ([]int64, error) {
	ret := _m.Called(ctx, collectionID)

	if len(ret) == 0 {
		panic("no return value specified for ShowPartitionsInternal")
	}

	var r0 []int64
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, int64) ([]int64, error)); ok {
		return rf(ctx, collectionID)
	}
	if rf, ok := ret.Get(0).(func(context.Context, int64) []int64); ok {
		r0 = rf(ctx, collectionID)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]int64)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, int64) error); ok {
		r1 = rf(ctx, collectionID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockBroker_ShowPartitionsInternal_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'ShowPartitionsInternal'
type MockBroker_ShowPartitionsInternal_Call struct {
	*mock.Call
}

// ShowPartitionsInternal is a helper method to define mock.On call
//   - ctx context.Context
//   - collectionID int64
func (_e *MockBroker_Expecter) ShowPartitionsInternal(ctx interface{}, collectionID interface{}) *MockBroker_ShowPartitionsInternal_Call {
	return &MockBroker_ShowPartitionsInternal_Call{Call: _e.mock.On("ShowPartitionsInternal", ctx, collectionID)}
}

func (_c *MockBroker_ShowPartitionsInternal_Call) Run(run func(ctx context.Context, collectionID int64)) *MockBroker_ShowPartitionsInternal_Call {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(context.Context), args[1].(int64))
	})
	return _c
}

func (_c *MockBroker_ShowPartitionsInternal_Call) Return(_a0 []int64, _a1 error) *MockBroker_ShowPartitionsInternal_Call {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockBroker_ShowPartitionsInternal_Call) RunAndReturn(run func(context.Context, int64) ([]int64, error)) *MockBroker_ShowPartitionsInternal_Call {
	_c.Call.Return(run)
	return _c
}

// NewMockBroker creates a new instance of MockBroker. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockBroker(t interface {
	mock.TestingT
	Cleanup(func())
}) *MockBroker {
	mock := &MockBroker{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
