// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package milvusclient

// This file adds mock methods that are missing from the auto-generated
// mock_milvus_server_test.go but required by the local proto replacement.

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

// GetRefreshExternalCollectionProgress provides a mock function with given fields: _a0, _a1
func (_m *MilvusServiceServer) GetRefreshExternalCollectionProgress(_a0 context.Context, _a1 *milvuspb.GetRefreshExternalCollectionProgressRequest) (*milvuspb.GetRefreshExternalCollectionProgressResponse, error) {
	ret := _m.Called(_a0, _a1)

	if len(ret) == 0 {
		panic("no return value specified for GetRefreshExternalCollectionProgress")
	}

	var r0 *milvuspb.GetRefreshExternalCollectionProgressResponse
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, *milvuspb.GetRefreshExternalCollectionProgressRequest) (*milvuspb.GetRefreshExternalCollectionProgressResponse, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(context.Context, *milvuspb.GetRefreshExternalCollectionProgressRequest) *milvuspb.GetRefreshExternalCollectionProgressResponse); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*milvuspb.GetRefreshExternalCollectionProgressResponse)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, *milvuspb.GetRefreshExternalCollectionProgressRequest) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ClientHeartbeat provides a mock function with given fields: _a0, _a1
func (_m *MilvusServiceServer) ClientHeartbeat(_a0 context.Context, _a1 *milvuspb.ClientHeartbeatRequest) (*milvuspb.ClientHeartbeatResponse, error) {
	ret := _m.Called(_a0, _a1)

	if len(ret) == 0 {
		panic("no return value specified for ClientHeartbeat")
	}

	var r0 *milvuspb.ClientHeartbeatResponse
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, *milvuspb.ClientHeartbeatRequest) (*milvuspb.ClientHeartbeatResponse, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(context.Context, *milvuspb.ClientHeartbeatRequest) *milvuspb.ClientHeartbeatResponse); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*milvuspb.ClientHeartbeatResponse)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, *milvuspb.ClientHeartbeatRequest) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// DeleteClientCommand provides a mock function with given fields: _a0, _a1
func (_m *MilvusServiceServer) DeleteClientCommand(_a0 context.Context, _a1 *milvuspb.DeleteClientCommandRequest) (*milvuspb.DeleteClientCommandResponse, error) {
	ret := _m.Called(_a0, _a1)

	if len(ret) == 0 {
		panic("no return value specified for DeleteClientCommand")
	}

	var r0 *milvuspb.DeleteClientCommandResponse
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, *milvuspb.DeleteClientCommandRequest) (*milvuspb.DeleteClientCommandResponse, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(context.Context, *milvuspb.DeleteClientCommandRequest) *milvuspb.DeleteClientCommandResponse); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*milvuspb.DeleteClientCommandResponse)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, *milvuspb.DeleteClientCommandRequest) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetClientTelemetry provides a mock function with given fields: _a0, _a1
func (_m *MilvusServiceServer) GetClientTelemetry(_a0 context.Context, _a1 *milvuspb.GetClientTelemetryRequest) (*milvuspb.GetClientTelemetryResponse, error) {
	ret := _m.Called(_a0, _a1)

	if len(ret) == 0 {
		panic("no return value specified for GetClientTelemetry")
	}

	var r0 *milvuspb.GetClientTelemetryResponse
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, *milvuspb.GetClientTelemetryRequest) (*milvuspb.GetClientTelemetryResponse, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(context.Context, *milvuspb.GetClientTelemetryRequest) *milvuspb.GetClientTelemetryResponse); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*milvuspb.GetClientTelemetryResponse)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, *milvuspb.GetClientTelemetryRequest) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// ListRefreshExternalCollectionJobs provides a mock function with given fields: _a0, _a1
func (_m *MilvusServiceServer) ListRefreshExternalCollectionJobs(_a0 context.Context, _a1 *milvuspb.ListRefreshExternalCollectionJobsRequest) (*milvuspb.ListRefreshExternalCollectionJobsResponse, error) {
	ret := _m.Called(_a0, _a1)

	if len(ret) == 0 {
		panic("no return value specified for ListRefreshExternalCollectionJobs")
	}

	var r0 *milvuspb.ListRefreshExternalCollectionJobsResponse
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, *milvuspb.ListRefreshExternalCollectionJobsRequest) (*milvuspb.ListRefreshExternalCollectionJobsResponse, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(context.Context, *milvuspb.ListRefreshExternalCollectionJobsRequest) *milvuspb.ListRefreshExternalCollectionJobsResponse); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*milvuspb.ListRefreshExternalCollectionJobsResponse)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, *milvuspb.ListRefreshExternalCollectionJobsRequest) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// PushClientCommand provides a mock function with given fields: _a0, _a1
func (_m *MilvusServiceServer) PushClientCommand(_a0 context.Context, _a1 *milvuspb.PushClientCommandRequest) (*milvuspb.PushClientCommandResponse, error) {
	ret := _m.Called(_a0, _a1)

	if len(ret) == 0 {
		panic("no return value specified for PushClientCommand")
	}

	var r0 *milvuspb.PushClientCommandResponse
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, *milvuspb.PushClientCommandRequest) (*milvuspb.PushClientCommandResponse, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(context.Context, *milvuspb.PushClientCommandRequest) *milvuspb.PushClientCommandResponse); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*milvuspb.PushClientCommandResponse)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, *milvuspb.PushClientCommandRequest) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// RefreshExternalCollection provides a mock function with given fields: _a0, _a1
func (_m *MilvusServiceServer) RefreshExternalCollection(_a0 context.Context, _a1 *milvuspb.RefreshExternalCollectionRequest) (*milvuspb.RefreshExternalCollectionResponse, error) {
	ret := _m.Called(_a0, _a1)

	if len(ret) == 0 {
		panic("no return value specified for RefreshExternalCollection")
	}

	var r0 *milvuspb.RefreshExternalCollectionResponse
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, *milvuspb.RefreshExternalCollectionRequest) (*milvuspb.RefreshExternalCollectionResponse, error)); ok {
		return rf(_a0, _a1)
	}
	if rf, ok := ret.Get(0).(func(context.Context, *milvuspb.RefreshExternalCollectionRequest) *milvuspb.RefreshExternalCollectionResponse); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*milvuspb.RefreshExternalCollectionResponse)
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, *milvuspb.RefreshExternalCollectionRequest) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
