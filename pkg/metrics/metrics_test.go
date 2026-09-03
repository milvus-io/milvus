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
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// proxyCollectionCollectors mirrors the production cleanup list, so a metric
// added there is automatically observed and asserted on here.
func proxyCollectionCollectors() []prometheus.Collector {
	scoped := proxyCollectionScopedMetrics()
	collectors := make([]prometheus.Collector, 0, len(scoped))
	for _, m := range scoped {
		collectors = append(collectors, m.(prometheus.Collector))
	}
	return collectors
}

// observeProxyCollection touches every (metric, label value) combination the
// proxy emits for a collection. Observing a single combination per metric is
// not enough: cleanup used to enumerate label values with Delete() and passed
// such a test while leaking every value the test did not happen to pick.
func observeProxyCollection(nodeID, db, collection string) {
	for _, queryType := range []string{SearchLabel, HybridSearchLabel, QueryLabel, UpsertQueryLabel} {
		ProxyReceivedNQ.WithLabelValues(nodeID, queryType, db, collection).Add(1)
		ProxySQLatency.WithLabelValues(nodeID, queryType, db, collection).Observe(1)
		ProxyCollectionSQLatency.WithLabelValues(nodeID, queryType, db, collection).Observe(1)
		ProxyRetrySearchCount.WithLabelValues(nodeID, queryType, db, collection).Add(1)
		ProxyRetrySearchResultInsufficientCount.WithLabelValues(nodeID, queryType, db, collection).Add(1)
		ProxyRecallSearchCount.WithLabelValues(nodeID, queryType, db, collection).Add(1)
		ProxySearchSparseNumNonZeros.WithLabelValues(nodeID, db, collection, queryType, "1").Observe(1)
		ProxyResourceGroupSQLatency.WithLabelValues(nodeID, queryType, db, collection, "rg").Observe(1)
	}
	for _, msgType := range []string{InsertLabel, DeleteLabel, UpsertLabel, SearchLabel, HybridSearchLabel, QueryLabel} {
		ProxyMutationLatency.WithLabelValues(nodeID, msgType, db, collection).Observe(1)
		ProxyCollectionMutationLatency.WithLabelValues(nodeID, msgType, db, collection).Observe(1)
		ProxyReceiveBytes.WithLabelValues(nodeID, msgType, db, collection).Add(1)
		ProxyScannedRemoteMB.WithLabelValues(nodeID, msgType, db, collection).Add(1)
		ProxyScannedTotalMB.WithLabelValues(nodeID, msgType, db, collection).Add(1)
	}
	ProxySearchVectors.WithLabelValues(nodeID, db, collection).Add(1)
	ProxyInsertVectors.WithLabelValues(nodeID, db, collection).Add(1)
	ProxyUpsertVectors.WithLabelValues(nodeID, db, collection).Add(1)
	ProxyDeleteVectors.WithLabelValues(nodeID, db, collection).Add(1)
	ProxyFunctionCall.WithLabelValues(nodeID, "x", SuccessLabel, CauseNA, db, collection).Add(1)
	ProxyFunctionlatency.WithLabelValues(nodeID, db, collection, "x", "x", "x").Observe(1)
}

// leftoverCollectionSeries returns the distinct metric names that still hold a
// series carrying label=value.
func leftoverCollectionSeries(t *testing.T, collectors []prometheus.Collector, label, value string) []string {
	t.Helper()
	r := prometheus.NewRegistry()
	for _, c := range collectors {
		r.MustRegister(c)
	}
	mfs, err := r.Gather()
	assert.NoError(t, err)
	seen := make(map[string]struct{})
	var names []string
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() != label || lp.GetValue() != value {
					continue
				}
				if _, ok := seen[mf.GetName()]; !ok {
					seen[mf.GetName()] = struct{}{}
					names = append(names, mf.GetName())
				}
				break
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
	assert.Len(t, before, len(proxyCollectionScopedMetrics()),
		"observeProxyCollection must observe every metric in proxyCollectionScopedMetrics")

	CleanupProxyCollectionMetrics(nodeID, db, coll)

	assert.Empty(t, leftoverCollectionSeries(t, proxyCollectionCollectors(), collectionName, coll))
	assert.NotEmpty(t, leftoverCollectionSeries(t, proxyCollectionCollectors(), collectionName, other))

	CleanupProxyCollectionMetrics(nodeID, db, other)
}

func TestCleanupProxyDBMetricsDropsEveryCollectionSeries(t *testing.T) {
	nodeID := int64(7)
	db := "metric_db_cleanup_db"
	otherDB := "metric_db_cleanup_other_db"

	observeProxyCollection("7", db, "coll_a")
	observeProxyCollection("7", db, "coll_b")
	observeProxyCollection("7", otherDB, "coll_a")

	before := leftoverCollectionSeries(t, proxyCollectionCollectors(), databaseLabelName, db)
	assert.Len(t, before, len(proxyCollectionScopedMetrics()))

	CleanupProxyDBMetrics(nodeID, db)

	assert.Empty(t, leftoverCollectionSeries(t, proxyCollectionCollectors(), databaseLabelName, db))
	assert.NotEmpty(t, leftoverCollectionSeries(t, proxyCollectionCollectors(), databaseLabelName, otherDB))

	CleanupProxyDBMetrics(nodeID, otherDB)
}

// TestCollectionScopedMetricsAreComplete parses the metric declarations and
// fails when a collection labeled metric is not wired into the matching drop
// cleanup, which is how these series leaked in the first place.
func TestCollectionScopedMetricsAreComplete(t *testing.T) {
	t.Run("proxy", func(t *testing.T) {
		assertCleanupCoversLabels(t, "proxy_metrics.go", "proxyCollectionScopedMetrics",
			map[string]string{
				// ProxyReportValue is db-scoped only (no collection_name label), so it
				// cannot join proxyCollectionScopedMetrics(): CleanupProxyCollectionMetrics
				// passes collection_name too and would then match nothing for it. It is
				// cleaned separately in CleanupProxyDBMetrics.
				"ProxyReportValue": "db-scoped only, cleaned in CleanupProxyDBMetrics",
			},
			"databaseLabelName", "collectionName")
	})
	t.Run("querynode", func(t *testing.T) {
		assertCleanupCoversLabels(t, "querynode_metrics.go", "CleanupQueryNodeCollectionMetrics",
			nil, "collectionIDLabelName")
	})
}

// assertCleanupCoversLabels asserts every prometheus vec declared in file whose
// variable label set overlaps requiredLabels is cleaned up by the named
// function:
//   - a vec carrying all required labels must be referenced by funcName;
//   - a vec carrying only some of them (e.g. collection_name without db_name)
//     is flagged as a gap unless it is allow-listed in allowList.
//
// Labels are matched by the identifier used in the declaration (for example
// "collectionName"), not by the string it expands to; string-literal label
// elements are resolved through the label-name constants in metrics.go. Scope is
// the single named file -- a vec declared in another file of the package is
// invisible to this guard.
func assertCleanupCoversLabels(t *testing.T, file, funcName string, allowList map[string]string, requiredLabels ...string) {
	t.Helper()
	labelIdentByValue := labelIdentifiers(t)

	parsed, err := parser.ParseFile(token.NewFileSet(), file, nil, 0)
	require.NoError(t, err)

	referenced := make(map[string]struct{})
	var declared []string
	var partial []string

	ast.Inspect(parsed, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.FuncDecl:
			if node.Name.Name != funcName {
				return true
			}
			ast.Inspect(node.Body, func(inner ast.Node) bool {
				if ident, ok := inner.(*ast.Ident); ok {
					referenced[ident.Name] = struct{}{}
				}
				return true
			})
		case *ast.ValueSpec:
			if len(node.Names) != 1 || len(node.Values) != 1 {
				return true
			}
			labels, ok := vecVariableLabels(node.Values[0], labelIdentByValue)
			if !ok {
				return true
			}
			hasAny, hasAll := false, true
			for _, required := range requiredLabels {
				_, has := labels[required]
				hasAny = hasAny || has
				hasAll = hasAll && has
			}
			if !hasAny {
				return true
			}
			name := node.Names[0].Name
			if hasAll {
				declared = append(declared, name)
				return true
			}
			if _, allowed := allowList[name]; !allowed {
				partial = append(partial, name)
			}
		}
		return true
	})

	require.NotEmpty(t, declared, "no metric declaration matched %v in %s", requiredLabels, file)
	for _, name := range declared {
		_, ok := referenced[name]
		assert.True(t, ok, "%s is labeled with %v but is never cleaned up in %s", name, requiredLabels, funcName)
	}
	for _, name := range partial {
		assert.Failf(t, "metric carries only some required labels", "%s is labeled with only some of %v but is not cleaned up in %s", name, requiredLabels, funcName)
	}
}

// labelIdentifiers parses the label-name constants declared in metrics.go and
// returns a map from a constant's string value to its identifier (for example
// "collection_name" -> "collectionName"), so a vec whose labels are written as
// string literals resolves to the same identifiers the guard matches against.
func labelIdentifiers(t *testing.T) map[string]string {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), "metrics.go", nil, 0)
	require.NoError(t, err)

	result := make(map[string]string)
	ast.Inspect(parsed, func(n ast.Node) bool {
		decl, ok := n.(*ast.GenDecl)
		if !ok || decl.Tok != token.CONST {
			return true
		}
		for _, spec := range decl.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for i, name := range vs.Names {
				if i >= len(vs.Values) {
					continue
				}
				lit, ok := vs.Values[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				if val, err := strconv.Unquote(lit.Value); err == nil {
					result[val] = name.Name
				}
			}
		}
		return true
	})
	return result
}

// vecVariableLabels returns the variable label names of a
// `prometheus.NewXxxVec(opts, []string{...})` expression. Identifiers are kept
// as-is; string literals are resolved to their identifier via labelIdentByValue.
func vecVariableLabels(expr ast.Expr, labelIdentByValue map[string]string) (map[string]struct{}, bool) {
	call, ok := expr.(*ast.CallExpr)
	if !ok || len(call.Args) == 0 {
		return nil, false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || !strings.HasSuffix(sel.Sel.Name, "Vec") {
		return nil, false
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok || pkg.Name != "prometheus" {
		return nil, false
	}
	lit, ok := call.Args[len(call.Args)-1].(*ast.CompositeLit)
	if !ok {
		return nil, false
	}
	if _, ok := lit.Type.(*ast.ArrayType); !ok {
		return nil, false
	}
	labels := make(map[string]struct{}, len(lit.Elts))
	for _, elt := range lit.Elts {
		switch e := elt.(type) {
		case *ast.Ident:
			labels[e.Name] = struct{}{}
		case *ast.BasicLit:
			if e.Kind == token.STRING {
				if val, err := strconv.Unquote(e.Value); err == nil {
					if ident, ok := labelIdentByValue[val]; ok {
						labels[ident] = struct{}{}
					} else {
						labels[val] = struct{}{}
					}
				}
			}
		}
	}
	return labels, true
}
