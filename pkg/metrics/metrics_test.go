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
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func TestRegisterMetrics(t *testing.T) {
	assert.NotPanics(t, func() {
		r := prometheus.NewRegistry()
		// Make sure it doesn't panic.
		RegisterMixCoord(r)
		RegisterDataNode(r)
		RegisterProxy(r)
		RegisterQueryNode(r)
		RegisterMetaMetrics(r)
		RegisterStorageMetrics(r)
		RegisterMsgStreamMetrics(r)
		RegisterCGOMetrics(r)
		RegisterStreamingServiceClient(r)
		RegisterStreamingNode(r)
		RegisterLoggingMetrics(r)
	})
}

func TestGetRegisterer(t *testing.T) {
	register := GetRegisterer()
	assert.NotNil(t, register)
	assert.Equal(t, prometheus.DefaultRegisterer, register)
	r := prometheus.NewRegistry()
	Register(r)
	register = GetRegisterer()
	assert.NotNil(t, register)
	assert.Equal(t, r, register)
}

func TestRegisterRuntimeInfo(t *testing.T) {
	g := &errgroup.Group{}
	g.Go(func() error {
		RegisterMetaType("etcd")
		return nil
	})
	g.Go(func() error {
		RegisterMQType("pulsar")
		return nil
	})
	g.Wait()

	infoMutex.Lock()
	defer infoMutex.Unlock()
	assert.Equal(t, "etcd", metaType)
	assert.Equal(t, "pulsar", mqType)
}

// TestCleanupQueryNodeCollectionMetrics tests that CleanupQueryNodeCollectionMetrics
// correctly cleans up all metrics for a given nodeID and collectionID.
func TestCleanupQueryNodeCollectionMetrics(t *testing.T) {
	nodeID := int64(1)
	collectionID := int64(100)
	nodeIDStr := "1"
	collectionIDStr := "100"

	// Set up some metrics for the collection
	// QueryNodeConsumerMsgCount: nodeID, msgType, collectionID
	QueryNodeConsumerMsgCount.WithLabelValues(nodeIDStr, "insert", collectionIDStr).Add(10)
	// QueryNodeConsumeTimeTickLag: nodeID, msgType, collectionID
	QueryNodeConsumeTimeTickLag.WithLabelValues(nodeIDStr, "insert", collectionIDStr).Set(5)
	// QueryNodeNumEntities: database, collectionName, nodeID, collectionID, segmentState
	QueryNodeNumEntities.WithLabelValues("default", "test_collection", nodeIDStr, collectionIDStr, "growing").Set(100)
	// QueryNodeEntitiesSize: nodeID, collectionID, segmentState
	QueryNodeEntitiesSize.WithLabelValues(nodeIDStr, collectionIDStr, "growing").Set(1024)
	// QueryNodeNumSegments: nodeID, collectionID, segmentState, segmentLevel
	QueryNodeNumSegments.WithLabelValues(nodeIDStr, collectionIDStr, "sealed", "L1").Set(5)
	// QueryNodeSQCount: nodeID, queryType, status, requestScope, collectionID
	QueryNodeSQCount.WithLabelValues(nodeIDStr, "search", "success", "default", collectionIDStr).Add(50)
	// QueryNodeLevelZeroSize: nodeID, collectionID, channelName
	QueryNodeLevelZeroSize.WithLabelValues(nodeIDStr, collectionIDStr, "ch1").Set(256)

	// Set up metrics for a different collection (should not be cleaned up)
	otherCollectionIDStr := "200"
	QueryNodeConsumerMsgCount.WithLabelValues(nodeIDStr, "insert", otherCollectionIDStr).Add(20)
	QueryNodeNumEntities.WithLabelValues("default", "other_collection", nodeIDStr, otherCollectionIDStr, "growing").Set(200)

	// Helper function to count metrics
	countCounterMetrics := func(vec *prometheus.CounterVec) int {
		ch := make(chan prometheus.Metric, 100)
		vec.Collect(ch)
		close(ch)
		count := 0
		for range ch {
			count++
		}
		return count
	}

	countGaugeMetrics := func(vec *prometheus.GaugeVec) int {
		ch := make(chan prometheus.Metric, 100)
		vec.Collect(ch)
		close(ch)
		count := 0
		for range ch {
			count++
		}
		return count
	}

	// Record counts before cleanup
	consumerCountBefore := countCounterMetrics(QueryNodeConsumerMsgCount)
	numEntitiesBefore := countGaugeMetrics(QueryNodeNumEntities)

	// Clean up metrics for the target collection
	CleanupQueryNodeCollectionMetrics(nodeID, collectionID)

	// Verify that the target collection's metrics are cleaned up
	// and other collection's metrics still exist
	consumerCountAfter := countCounterMetrics(QueryNodeConsumerMsgCount)
	numEntitiesAfter := countGaugeMetrics(QueryNodeNumEntities)

	// At least one metric should be removed from each
	assert.Less(t, consumerCountAfter, consumerCountBefore)
	assert.Less(t, numEntitiesAfter, numEntitiesBefore)

	// Other collection's metrics should still exist
	assert.Greater(t, consumerCountAfter, 0)
	assert.Greater(t, numEntitiesAfter, 0)
}

// TestDeletePartialMatch test deletes all metrics where the variable labels contain all of those
// passed in as labels based on DeletePartialMatch API
func TestDeletePartialMatch(t *testing.T) {
	baseVec := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "test",
			Help: "helpless",
		},
		[]string{"l1", "l2", "l3"},
	)

	baseVec.WithLabelValues("l1-1", "l2-1", "l3-1").Inc()
	baseVec.WithLabelValues("l1-2", "l2-2", "l3-2").Inc()
	baseVec.WithLabelValues("l1-2", "l2-3", "l3-3").Inc()

	baseVec.WithLabelValues("l1-3", "l2-3", "l3-3").Inc()
	baseVec.WithLabelValues("l1-3", "l2-3", "").Inc()
	baseVec.WithLabelValues("l1-3", "l2-4", "l3-4").Inc()

	baseVec.WithLabelValues("l1-4", "l2-5", "l3-5").Inc()
	baseVec.WithLabelValues("l1-4", "l2-5", "l3-6").Inc()
	baseVec.WithLabelValues("l1-5", "l2-6", "l3-6").Inc()

	getMetricsCount := func() int {
		chs := make(chan prometheus.Metric, 10)
		baseVec.Collect(chs)
		return len(chs)
	}

	// the prefix is matched which has one labels
	if got, want := baseVec.DeletePartialMatch(prometheus.Labels{"l1": "l1-2"}), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	assert.Equal(t, 7, getMetricsCount())

	// the prefix is matched which has two labels
	if got, want := baseVec.DeletePartialMatch(prometheus.Labels{"l1": "l1-3", "l2": "l2-3"}), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	assert.Equal(t, 5, getMetricsCount())

	// the first and latest labels are matched
	if got, want := baseVec.DeletePartialMatch(prometheus.Labels{"l1": "l1-1", "l3": "l3-1"}), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	assert.Equal(t, 4, getMetricsCount())

	// the middle labels are matched
	if got, want := baseVec.DeletePartialMatch(prometheus.Labels{"l2": "l2-5"}), 2; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	assert.Equal(t, 2, getMetricsCount())

	// the middle labels and suffix labels are matched
	if got, want := baseVec.DeletePartialMatch(prometheus.Labels{"l2": "l2-6", "l3": "l3-6"}), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	assert.Equal(t, 1, getMetricsCount())

	// all labels are matched
	if got, want := baseVec.DeletePartialMatch(prometheus.Labels{"l1": "l1-3", "l2": "l2-4", "l3": "l3-4"}), 1; got != want {
		t.Errorf("got %v, want %v", got, want)
	}
	assert.Equal(t, 0, getMetricsCount())
}

func queryNodeCollectionCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		QueryNodeConsumeTimeTickLag,
		QueryNodeConsumerMsgCount,
		QueryNodeSkippedInsertFieldCount,
		QueryNodeNumSegments,
		QueryNodeSQCount,
		QueryNodeSearchFTSNumTokens,
		QueryNodeSearchHitSegmentNum,
		QueryNodeSegmentFilterHitSegmentNum,
		QueryNodeSegmentFilterSkippedSegmentNum,
		QueryNodeSegmentFilterTotalSegmentNum,
		QueryNodeSegmentPruneRatio,
		QueryNodeSegmentPruneBias,
		QueryNodeSegmentPruneLatency,
		QueryNodeNumEntities,
		QueryNodeEntitiesSize,
		QueryNodeLevelZeroSize,
		QueryNodePartialResultCount,
		QueryNodeTwoStageFilterLatency,
		QueryNodeTwoStageSearchLatency,
		QueryNodeTwoStageSearchFallbackCount,
		QueryNodeGlobalRefineCount,
	}
}

func observeQueryNodeCollection(nodeID, collectionID string) {
	QueryNodeConsumeTimeTickLag.WithLabelValues(nodeID, InsertLabel, collectionID).Set(1)
	QueryNodeConsumerMsgCount.WithLabelValues(nodeID, InsertLabel, collectionID).Add(1)
	QueryNodeSkippedInsertFieldCount.WithLabelValues(nodeID, collectionID).Add(1)
	QueryNodeNumSegments.WithLabelValues(nodeID, collectionID, SealedSegmentLabel, "L1").Set(1)
	QueryNodeSQCount.WithLabelValues(nodeID, SearchLabel, SuccessLabel, Leader, collectionID).Add(1)
	QueryNodeSearchFTSNumTokens.WithLabelValues(nodeID, collectionID, "1").Observe(1)
	QueryNodeSearchHitSegmentNum.WithLabelValues(nodeID, collectionID, SearchLabel).Observe(1)
	QueryNodeSegmentFilterHitSegmentNum.WithLabelValues(nodeID, collectionID, SearchLabel).Observe(1)
	QueryNodeSegmentFilterSkippedSegmentNum.WithLabelValues(nodeID, collectionID, SearchLabel).Observe(1)
	QueryNodeSegmentFilterTotalSegmentNum.WithLabelValues(nodeID, collectionID, SearchLabel).Observe(1)
	QueryNodeSegmentPruneRatio.WithLabelValues(nodeID, collectionID, "x").Set(1)
	QueryNodeSegmentPruneBias.WithLabelValues(nodeID, collectionID, "x").Set(1)
	QueryNodeSegmentPruneLatency.WithLabelValues(nodeID, collectionID, "x").Observe(1)
	QueryNodeNumEntities.WithLabelValues("db", "coll", nodeID, collectionID, GrowingSegmentLabel).Set(1)
	QueryNodeEntitiesSize.WithLabelValues(nodeID, collectionID, GrowingSegmentLabel).Set(1)
	QueryNodeLevelZeroSize.WithLabelValues(nodeID, collectionID, "ch").Set(1)
	QueryNodePartialResultCount.WithLabelValues(nodeID, SearchLabel, collectionID).Add(1)
	QueryNodeTwoStageFilterLatency.WithLabelValues(nodeID, collectionID).Observe(1)
	QueryNodeTwoStageSearchLatency.WithLabelValues(nodeID, collectionID).Observe(1)
	QueryNodeTwoStageSearchFallbackCount.WithLabelValues(nodeID, collectionID, "x").Add(1)
	QueryNodeGlobalRefineCount.WithLabelValues(nodeID, collectionID).Add(1)
}

func proxyCollectionCollectors() []prometheus.Collector {
	return []prometheus.Collector{
		ProxyReceivedNQ,
		ProxySearchVectors,
		ProxyInsertVectors,
		ProxyUpsertVectors,
		ProxyDeleteVectors,
		ProxySQLatency,
		ProxyCollectionSQLatency,
		ProxyMutationLatency,
		ProxyCollectionMutationLatency,
		ProxyFunctionCall,
		ProxyReceiveBytes,
		ProxyRetrySearchCount,
		ProxyRetrySearchResultInsufficientCount,
		ProxyRecallSearchCount,
		ProxySearchSparseNumNonZeros,
		ProxyFunctionlatency,
		ProxyScannedRemoteMB,
		ProxyScannedTotalMB,
	}
}

// observeProxyCollection emits the label combinations the proxy actually uses
// (internal/proxy/impl.go), including the ones the pre-#52690 enumeration did
// not cover. The deprecated per-collection metrics are emitted for search,
// hybrid_search, query and upsert_query / delete and upsert; the byte/scanned
// counters for every msg_type including hybrid_search. Observing a single
// combination per metric cannot catch a stale cleanup enumeration, so this
// deliberately mirrors the full emitted space.
func observeProxyCollection(nodeID, db, collection string) {
	ProxyReceivedNQ.WithLabelValues(nodeID, SearchLabel, db, collection).Add(1)
	ProxyReceivedNQ.WithLabelValues(nodeID, QueryLabel, db, collection).Add(1)
	ProxySearchVectors.WithLabelValues(nodeID, db, collection).Add(1)
	ProxyInsertVectors.WithLabelValues(nodeID, db, collection).Add(1)
	ProxyUpsertVectors.WithLabelValues(nodeID, db, collection).Add(1)
	ProxyDeleteVectors.WithLabelValues(nodeID, db, collection).Add(1)
	ProxySQLatency.WithLabelValues(nodeID, SearchLabel, db, collection).Observe(1)
	ProxySQLatency.WithLabelValues(nodeID, HybridSearchLabel, db, collection).Observe(1)
	ProxyCollectionSQLatency.WithLabelValues(nodeID, SearchLabel, db, collection).Observe(1)
	ProxyCollectionSQLatency.WithLabelValues(nodeID, HybridSearchLabel, db, collection).Observe(1)
	ProxyMutationLatency.WithLabelValues(nodeID, DeleteLabel, db, collection).Observe(1)
	ProxyMutationLatency.WithLabelValues(nodeID, UpsertLabel, db, collection).Observe(1)
	ProxyCollectionMutationLatency.WithLabelValues(nodeID, DeleteLabel, db, collection).Observe(1)
	ProxyCollectionMutationLatency.WithLabelValues(nodeID, UpsertLabel, db, collection).Observe(1)
	ProxyFunctionCall.WithLabelValues(nodeID, "x", SuccessLabel, CauseNA, db, collection).Add(1)
	ProxyReceiveBytes.WithLabelValues(nodeID, DeleteLabel, db, collection).Add(1)
	ProxyReceiveBytes.WithLabelValues(nodeID, HybridSearchLabel, db, collection).Add(1)
	ProxyRetrySearchCount.WithLabelValues(nodeID, SearchLabel, db, collection).Add(1)
	ProxyRetrySearchResultInsufficientCount.WithLabelValues(nodeID, SearchLabel, db, collection).Add(1)
	ProxyRecallSearchCount.WithLabelValues(nodeID, SearchLabel, db, collection).Add(1)
	ProxySearchSparseNumNonZeros.WithLabelValues(nodeID, collection, SearchLabel, "1").Observe(1)
	ProxyFunctionlatency.WithLabelValues(nodeID, collection, "x", "x", "x").Observe(1)
	ProxyScannedRemoteMB.WithLabelValues(nodeID, DeleteLabel, db, collection).Add(1)
	ProxyScannedRemoteMB.WithLabelValues(nodeID, HybridSearchLabel, db, collection).Add(1)
	ProxyScannedTotalMB.WithLabelValues(nodeID, DeleteLabel, db, collection).Add(1)
	ProxyScannedTotalMB.WithLabelValues(nodeID, HybridSearchLabel, db, collection).Add(1)
}

func leftoverCollectionSeries(t *testing.T, collectors []prometheus.Collector, label, value string) []string {
	t.Helper()
	r := prometheus.NewRegistry()
	for _, c := range collectors {
		r.MustRegister(c)
	}
	mfs, err := r.Gather()
	assert.NoError(t, err)
	var names []string
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == label && lp.GetValue() == value {
					names = append(names, mf.GetName())
					break
				}
			}
		}
	}
	return names
}

func TestCleanupQueryNodeCollectionMetricsDropsEveryCollectionSeries(t *testing.T) {
	nodeID := int64(7)
	collectionID := int64(424242)
	otherID := int64(424243)
	node := "7"
	coll := "424242"
	other := "424243"

	observeQueryNodeCollection(node, coll)
	observeQueryNodeCollection(node, other)

	before := leftoverCollectionSeries(t, queryNodeCollectionCollectors(), collectionIDLabelName, coll)
	assert.NotEmpty(t, before)

	CleanupQueryNodeCollectionMetrics(nodeID, collectionID)

	assert.Empty(t, leftoverCollectionSeries(t, queryNodeCollectionCollectors(), collectionIDLabelName, coll))
	assert.NotEmpty(t, leftoverCollectionSeries(t, queryNodeCollectionCollectors(), collectionIDLabelName, other))

	CleanupQueryNodeCollectionMetrics(nodeID, otherID)
}

func TestCleanupProxyCollectionMetricsDropsEveryCollectionSeries(t *testing.T) {
	nodeID := int64(7)
	db := "metric_cleanup_db"
	coll := "metric_cleanup_coll"
	other := "metric_cleanup_other"

	observeProxyCollection("7", db, coll)
	observeProxyCollection("7", db, other)

	before := leftoverCollectionSeries(t, proxyCollectionCollectors(), collectionName, coll)
	assert.NotEmpty(t, before)

	CleanupProxyCollectionMetrics(nodeID, db, coll)

	assert.Empty(t, leftoverCollectionSeries(t, proxyCollectionCollectors(), collectionName, coll))
	assert.NotEmpty(t, leftoverCollectionSeries(t, proxyCollectionCollectors(), collectionName, other))

	CleanupProxyCollectionMetrics(nodeID, db, other)
}

func TestCleanupProxyDBMetricsDropsEveryDBSeries(t *testing.T) {
	nodeID := int64(7)
	dbA := "metric_cleanup_dba"
	dbB := "metric_cleanup_dbb"

	observeProxyCollection("7", dbA, "coll_a")
	observeProxyCollection("7", dbB, "coll_b")

	before := leftoverCollectionSeries(t, proxyCollectionCollectors(), databaseLabelName, dbA)
	assert.NotEmpty(t, before)

	CleanupProxyDBMetrics(nodeID, dbA)

	assert.Empty(t, leftoverCollectionSeries(t, proxyCollectionCollectors(), databaseLabelName, dbA))
	assert.NotEmpty(t, leftoverCollectionSeries(t, proxyCollectionCollectors(), databaseLabelName, dbB))

	CleanupProxyDBMetrics(nodeID, dbB)
}

// funcBody returns the body (including braces) of the first function with the
// given name in src, scanning balanced braces so nested { } in Labels{} blocks
// do not truncate it early.
func funcBody(src, name string) string {
	start := strings.Index(src, "func "+name)
	if start < 0 {
		return ""
	}
	brace := strings.Index(src[start:], "{")
	if brace < 0 {
		return ""
	}
	brace += start
	depth := 0
	for i := brace; i < len(src); i++ {
		switch src[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return src[brace : i+1]
			}
		}
	}
	return src[brace:]
}

// TestProxyCollectionScopedMetricsAreCleanedOnCollectionAndDBDrop is the
// structural regression guard for #52690 (and the #52682 failure mode): every
// proxy metric Vec labeled with db_name + collection_name must be cleaned by
// BOTH cleanup paths. A per-value enumeration that goes stale is caught by the
// behaviour tests above; a metric that forgets to hook into cleanup at all is
// only caught here. Both functions are parsed from proxy_metrics.go so the
// check follows the declarations instead of duplicating the collector list.
func TestProxyCollectionScopedMetricsAreCleanedOnCollectionAndDBDrop(t *testing.T) {
	src, err := os.ReadFile("proxy_metrics.go")
	assert.NoError(t, err)
	text := string(src)

	collectionBody := funcBody(text, "CleanupProxyCollectionMetrics")
	dbBody := funcBody(text, "CleanupProxyDBMetrics")
	assert.NotEmpty(t, collectionBody, "CleanupProxyCollectionMetrics not found in proxy_metrics.go")
	assert.NotEmpty(t, dbBody, "CleanupProxyDBMetrics not found in proxy_metrics.go")

	vecRe := regexp.MustCompile(`(?s)(\w+)\s*=\s*prometheus\.New\w+Vec\(.*?\[\]string\{([^}]*)\}`)
	for _, m := range vecRe.FindAllStringSubmatch(text, -1) {
		metricName, labels := m[1], m[2]
		if !strings.Contains(labels, "databaseLabelName") || !strings.Contains(labels, "collectionName") {
			continue // not collection-scoped (e.g. ProxySearchSparseNumNonZeros, ProxyFunctionlatency)
		}
		assert.Contains(t, collectionBody, metricName,
			"collection-scoped metric %s must be cleaned on collection drop", metricName)
		assert.Contains(t, dbBody, metricName,
			"collection-scoped metric %s must be cleaned on db drop", metricName)
	}
}
