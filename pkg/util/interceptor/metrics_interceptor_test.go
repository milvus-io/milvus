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

package interceptor

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
)

func TestMetricsInterceptors(t *testing.T) {
	metrics.RegisterGRPCMetrics(prometheus.NewRegistry())

	t.Run("unary server interceptor is non-nil", func(t *testing.T) {
		assert.NotNil(t, NewMetricsServerUnaryInterceptor())
	})
	t.Run("stream server interceptor is non-nil", func(t *testing.T) {
		assert.NotNil(t, NewMetricsStreamServerInterceptor())
	})
	t.Run("unary client interceptor is non-nil", func(t *testing.T) {
		assert.NotNil(t, NewMetricsClientUnaryInterceptor())
	})
	t.Run("stream client interceptor is non-nil", func(t *testing.T) {
		assert.NotNil(t, NewMetricsClientStreamInterceptor())
	})
}
