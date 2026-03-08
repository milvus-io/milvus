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

package mock

import (
	"context"

	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
)

type GrpcHealthWatchServer struct {
	chanResult chan *grpc_health_v1.HealthCheckResponse
}

func NewGrpcHealthWatchServer() *GrpcHealthWatchServer {
	return &GrpcHealthWatchServer{
		chanResult: make(chan *grpc_health_v1.HealthCheckResponse, 1),
	}
}

func (m GrpcHealthWatchServer) Send(response *grpc_health_v1.HealthCheckResponse) error {
	m.chanResult <- response
	return nil
}

func (m GrpcHealthWatchServer) Chan() <-chan *grpc_health_v1.HealthCheckResponse {
	return m.chanResult
}

func (m GrpcHealthWatchServer) SetHeader(md metadata.MD) error {
	return nil
}

func (m GrpcHealthWatchServer) SendHeader(md metadata.MD) error {
	return nil
}

func (m GrpcHealthWatchServer) SetTrailer(md metadata.MD) {
}

func (m GrpcHealthWatchServer) Context() context.Context {
	return nil
}

func (m GrpcHealthWatchServer) SendMsg(msg interface{}) error {
	return nil
}

func (m GrpcHealthWatchServer) RecvMsg(msg interface{}) error {
	return nil
}
