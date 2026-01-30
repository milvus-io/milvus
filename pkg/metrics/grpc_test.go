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

package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func TestRegisterGRPCMetrics(t *testing.T) {
	t.Run("register grpc metrics", func(t *testing.T) {
		r := prometheus.NewRegistry()
		assert.NotPanics(t, func() {
			RegisterGRPCMetrics(r)
		})

		// Verify that the metrics are registered
		assert.NotNil(t, GRPCServerMetric)
		assert.NotNil(t, GRPCClientMetric)
	})

	t.Run("metrics can be used after registration", func(t *testing.T) {
		r := prometheus.NewRegistry()
		RegisterGRPCMetrics(r)

		// Verify server metrics has interceptors
		assert.NotNil(t, GRPCServerMetric.UnaryServerInterceptor())
		assert.NotNil(t, GRPCServerMetric.StreamServerInterceptor())

		// Verify client metrics has interceptors
		assert.NotNil(t, GRPCClientMetric.UnaryClientInterceptor())
		assert.NotNil(t, GRPCClientMetric.StreamClientInterceptor())
	})
}
