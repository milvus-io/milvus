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
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldObserveProxyFunctionCall(t *testing.T) {
	assert.False(t, ShouldObserveProxyFunctionCall("DropCollection"))
	assert.True(t, ShouldObserveProxyFunctionCall("CreateCollection"))
}

func TestCleanupProxyCollectionMetricsCleansProxyFunctionCall(t *testing.T) {
	ProxyFunctionCall.Reset()
	t.Cleanup(ProxyFunctionCall.Reset)

	const (
		nodeID           = int64(1)
		nodeIDLabel      = "1"
		dbName           = "test_db"
		targetCollection = "target_collection"
		otherCollection  = "other_collection"
	)

	ProxyFunctionCall.WithLabelValues(nodeIDLabel, "Search", SuccessLabel, dbName, targetCollection).Inc()
	ProxyFunctionCall.WithLabelValues(nodeIDLabel, "Insert", FailSystemLabel, dbName, targetCollection).Inc()
	ProxyFunctionCall.WithLabelValues(nodeIDLabel, "Search", SuccessLabel, dbName, otherCollection).Inc()

	CleanupProxyCollectionMetrics(nodeID, dbName, targetCollection)

	expected := `
# HELP milvus_proxy_req_count count of operation executed
# TYPE milvus_proxy_req_count counter
milvus_proxy_req_count{collection_name="other_collection",db_name="test_db",function_name="Search",node_id="1",status="success"} 1
`
	require.NoError(t, testutil.CollectAndCompare(
		ProxyFunctionCall,
		strings.NewReader(expected),
		"milvus_proxy_req_count",
	))
}
