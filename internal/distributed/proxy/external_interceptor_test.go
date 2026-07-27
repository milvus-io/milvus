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

package grpcproxy

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy"
	"github.com/milvus-io/milvus/internal/proxy/accesslog"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type mockExternalProxyHook struct {
	hookutil.DefaultHook
	called *atomic.Bool
}

func (m mockExternalProxyHook) Mock(context.Context, interface{}, string) (bool, interface{}, error) {
	m.called.Store(true)
	return true, "mocked", nil
}

func TestExternalProxyUnaryInterceptors_PrivilegeRunsBeforeHook(t *testing.T) {
	mockey.PatchConvey(t.Name(), t, func() {
		hookutil.InitOnceHook()
		hookCalled := &atomic.Bool{}
		hookutil.SetTestHook(mockExternalProxyHook{called: hookCalled})
		t.Cleanup(func() {
			hookutil.SetTestHook(&hookutil.DefaultHook{})
		})

		privilegeErr := errors.New("privilege denied")
		mockey.Mock(proxy.AuthenticationInterceptor).Return(context.Background(), nil).Build()
		mockey.Mock(proxy.PrivilegeInterceptor).To(func(ctx context.Context, req interface{}) (context.Context, error) {
			return ctx, privilegeErr
		}).Build()
		mockey.Mock(accesslog.UnaryAccessLogInterceptor).To(func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
			return handler(ctx, req)
		}).Build()

		chain := grpc_middleware.ChainUnaryServer(buildExternalProxyUnaryInterceptors(nil)...)
		handlerCalled := false
		resp, err := chain(
			context.Background(),
			&milvuspb.LoadCollectionRequest{},
			&grpc.UnaryServerInfo{FullMethod: milvuspb.MilvusService_LoadCollection_FullMethodName},
			func(ctx context.Context, req interface{}) (interface{}, error) {
				handlerCalled = true
				return "handler", nil
			},
		)

		assert.Nil(t, resp)
		assert.ErrorIs(t, err, privilegeErr)
		assert.False(t, hookCalled.Load())
		assert.False(t, handlerCalled)
	})
}

func TestExternalProxyStreamInterceptors_RecordMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics.RegisterGRPCMetrics(registry)
	interceptors := buildExternalProxyStreamInterceptors()
	assert.Len(t, interceptors, 2)
	chain := grpc_middleware.ChainStreamServer(interceptors...)

	handlerCalled := false
	err := chain(
		nil,
		&mockExternalServerStream{ctx: context.Background()},
		&grpc.StreamServerInfo{
			FullMethod:     milvuspb.MilvusService_CreateReplicateStream_FullMethodName,
			IsClientStream: true,
			IsServerStream: true,
		},
		func(srv interface{}, stream grpc.ServerStream) error {
			handlerCalled = true
			return nil
		},
	)

	assert.NoError(t, err)
	assert.True(t, handlerCalled)
	metricFamilies, err := registry.Gather()
	assert.NoError(t, err)
	assert.True(t, externalMetricHasLabels(metricFamilies, "milvus_grpc_server_handled_total", map[string]string{
		"grpc_type":    "bidi_stream",
		"grpc_service": "milvus.proto.milvus.MilvusService",
		"grpc_method":  "CreateReplicateStream",
		"grpc_code":    "OK",
		"node_id":      paramtable.GetStringNodeID(),
	}))
}

type mockExternalServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *mockExternalServerStream) Context() context.Context {
	return s.ctx
}

func externalMetricHasLabels(metricFamilies []*dto.MetricFamily, name string, labels map[string]string) bool {
	for _, metricFamily := range metricFamilies {
		if metricFamily.GetName() != name {
			continue
		}
		for _, metric := range metricFamily.GetMetric() {
			got := make(map[string]string, len(metric.GetLabel()))
			for _, label := range metric.GetLabel() {
				got[label.GetName()] = label.GetValue()
			}
			matched := true
			for key, value := range labels {
				if got[key] != value {
					matched = false
					break
				}
			}
			if matched {
				return true
			}
		}
	}
	return false
}
