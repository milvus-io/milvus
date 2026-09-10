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

package proxy

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
)

func TestStreamMethodAuthorizers_CoverAllStreams(t *testing.T) {
	fullMethodName := func(serviceName, streamName string) string {
		return "/" + serviceName + "/" + streamName
	}

	allowedMethods := map[string]struct{}{
		grpc_health_v1.Health_Watch_FullMethodName: {},
	}
	for fullMethod := range streamMethodAuthorizers {
		allowedMethods[fullMethod] = struct{}{}
	}

	for _, sd := range milvuspb.MilvusService_ServiceDesc.Streams {
		method := fullMethodName(milvuspb.MilvusService_ServiceDesc.ServiceName, sd.StreamName)
		_, ok := allowedMethods[method]
		assert.True(t, ok, "streaming method %s on MilvusService is not registered in streamMethodAuthorizers nor exempted", method)
	}

	streamMethods := make(map[string]struct{})
	for _, sd := range milvuspb.MilvusService_ServiceDesc.Streams {
		streamMethods[fullMethodName(milvuspb.MilvusService_ServiceDesc.ServiceName, sd.StreamName)] = struct{}{}
	}
	for fullMethod := range streamMethodAuthorizers {
		_, ok := streamMethods[fullMethod]
		assert.True(t, ok, "streamMethodAuthorizers key %s has no corresponding stream in MilvusService", fullMethod)
	}

	var healthStreams []string
	for _, sd := range grpc_health_v1.Health_ServiceDesc.Streams {
		healthStreams = append(healthStreams, sd.StreamName)
	}
	require.Equal(t, []string{"Watch"}, healthStreams)
	_, ok := allowedMethods[fullMethodName(grpc_health_v1.Health_ServiceDesc.ServiceName, "Watch")]
	assert.True(t, ok, "health Watch stream is not covered by the exemption set")
}
