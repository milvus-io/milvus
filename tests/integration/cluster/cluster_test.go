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

package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	management "github.com/milvus-io/milvus/internal/http"
	"github.com/milvus-io/milvus/tests/integration/cluster/process"
)

func TestGetOptionsAssignsDistinctMetricsPorts(t *testing.T) {
	cluster := &MiniClusterV3{
		extraEnv: map[string]string{
			management.ListenPortEnvKey: "19091",
		},
	}

	firstPort := metricsPortFromOptions(t, cluster.getOptions())
	secondPort := metricsPortFromOptions(t, cluster.getOptions())

	require.NotZero(t, firstPort)
	require.NotZero(t, secondPort)
	require.NotEqual(t, firstPort, secondPort)
}

func metricsPortFromOptions(t *testing.T, opts []process.Option) int {
	t.Helper()
	milvusProcess := &process.MilvusProcess{}
	for _, opt := range opts {
		opt(milvusProcess)
	}
	port, err := milvusProcess.GetMetricsPort()
	require.NoError(t, err)
	return port
}
