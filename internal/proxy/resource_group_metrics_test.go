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
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/scheduler"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestObserveResourceGroupSQLatencyEmitsNoSeriesForAnUnscopedRequest(t *testing.T) {
	paramtable.Init()
	metrics.ProxyResourceGroupSQLatency.Reset()

	observeResourceGroupSQLatency(context.Background(), metrics.SearchLabel, "db", "coll", 12)

	assert.Equal(t, 0, collectSeries(t, metrics.ProxyResourceGroupSQLatency),
		"a request nothing scoped to a resource group must leave no per-resource-group series")
}

func TestObserveResourceGroupSQLatencyAttributesToTheScopeRoutingUsed(t *testing.T) {
	paramtable.Init()
	metrics.ProxyResourceGroupSQLatency.Reset()

	ctx := extension.WithQueryResourceGroup(context.Background(), "rg-a")
	observeResourceGroupSQLatency(ctx, metrics.SearchLabel, "db", "coll", 17)

	h, err := metrics.ProxyResourceGroupSQLatency.GetMetricWithLabelValues(
		paramtable.GetStringNodeID(), metrics.SearchLabel, "db", "coll", "rg-a")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), histogramCount(t, h),
		"the completed search must land in the series of the resource group that served it")
	assert.Equal(t, 1, collectSeries(t, metrics.ProxyResourceGroupSQLatency))
}

func collectSeries(t *testing.T, c prometheus.Collector) int {
	t.Helper()
	ch := make(chan prometheus.Metric, 64)
	go func() {
		c.Collect(ch)
		close(ch)
	}()
	n := 0
	for range ch {
		n++
	}
	return n
}

func histogramCount(t *testing.T, o prometheus.Observer) uint64 {
	t.Helper()
	m, ok := o.(prometheus.Metric)
	require.True(t, ok, "a histogram child must also be a Metric")
	var pb dto.Metric
	require.NoError(t, m.Write(&pb))
	return pb.GetHistogram().GetSampleCount()
}

// scopingHook is a request hook that pins every request to one resource group
// from Before, the way a deployment that routes queries per resource group
// does.
type scopingHook struct {
	hookutil.DefaultHook
	resourceGroup string
}

func (h scopingHook) Before(ctx context.Context, _ interface{}, _ string) (context.Context, error) {
	return extension.WithQueryResourceGroup(ctx, h.resourceGroup), nil
}

// The context a hook's Before returns is the context the handler runs under,
// so a resource group the hook pins reaches the search task - and from there
// the shard-leader routing - with no plumbing of its own.
func TestSearchRunsScopedToTheResourceGroupTheHookNamed(t *testing.T) {
	hookutil.InitOnceHook()
	hookutil.SetTestHook(scopingHook{resourceGroup: "rg-a"})
	defer hookutil.SetTestHook(hookutil.DefaultHook{})

	var scope string
	seen := false
	defer mockey.Mock((*scheduler.BaseTaskQueue).Enqueue).To(func(_ *scheduler.BaseTaskQueue, t taskmodel.Task) error {
		scope = extension.QueryResourceGroupFromContext(t.TraceCtx())
		seen = true
		return nil
	}).Build().UnPatch()
	defer mockey.Mock((*taskmodel.TaskCondition).WaitToFinish).Return(nil).Build().UnPatch()

	node := &Proxy{sched: &scheduler.TaskScheduler{DqQueue: &scheduler.DqTaskQueue{}}}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	_, err := HookInterceptor(context.Background(),
		&milvuspb.SearchRequest{DbName: "db", CollectionName: "coll"}, "user",
		milvuspb.MilvusService_Search_FullMethodName,
		func(ctx context.Context, req any) (any, error) {
			return node.Search(ctx, req.(*milvuspb.SearchRequest))
		})
	require.NoError(t, err)
	require.True(t, seen, "the search must have reached the task queue")
	assert.Equal(t, "rg-a", scope,
		"the task must run scoped to the resource group the hook named, or routing ignores it")
}

func TestSearchTaskCarriesNoScopeWithTheDefaultHook(t *testing.T) {
	hookutil.InitOnceHook()
	hookutil.SetTestHook(hookutil.DefaultHook{})

	scope := "unset"
	defer mockey.Mock((*scheduler.BaseTaskQueue).Enqueue).To(func(_ *scheduler.BaseTaskQueue, t taskmodel.Task) error {
		scope = extension.QueryResourceGroupFromContext(t.TraceCtx())
		return nil
	}).Build().UnPatch()
	defer mockey.Mock((*taskmodel.TaskCondition).WaitToFinish).Return(nil).Build().UnPatch()

	node := &Proxy{sched: &scheduler.TaskScheduler{DqQueue: &scheduler.DqTaskQueue{}}}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	_, err := HookInterceptor(context.Background(),
		&milvuspb.SearchRequest{DbName: "db", CollectionName: "coll"}, "user",
		milvuspb.MilvusService_Search_FullMethodName,
		func(ctx context.Context, req any) (any, error) {
			return node.Search(ctx, req.(*milvuspb.SearchRequest))
		})
	require.NoError(t, err)
	assert.Equal(t, "", scope, "a stock binary must route across every replica of the collection")
}

func counterValue(t *testing.T, c *prometheus.CounterVec, labels ...string) float64 {
	t.Helper()
	m, err := c.GetMetricWithLabelValues(labels...)
	require.NoError(t, err)
	var pb dto.Metric
	require.NoError(t, m.Write(&pb))
	return pb.GetCounter().GetValue()
}

func TestObserveResourceGroupSearchVectorsEmitsNoSeriesForAnUnscopedRequest(t *testing.T) {
	paramtable.Init()
	metrics.ProxyResourceGroupSearchVectors.Reset()

	observeResourceGroupSearchVectors(context.Background(), "db", "coll", 3)

	assert.Equal(t, 0, collectSeries(t, metrics.ProxyResourceGroupSearchVectors))
}

func TestObserveResourceGroupSearchVectorsCountsAgainstTheScope(t *testing.T) {
	paramtable.Init()
	metrics.ProxyResourceGroupSearchVectors.Reset()

	ctx := extension.WithQueryResourceGroup(context.Background(), "rg-a")
	observeResourceGroupSearchVectors(ctx, "db", "coll", 3)
	observeResourceGroupSearchVectors(ctx, "db", "coll", 2)

	assert.Equal(t, float64(5), counterValue(t, metrics.ProxyResourceGroupSearchVectors,
		paramtable.GetStringNodeID(), "db", "coll", "rg-a"))
	assert.Equal(t, 1, collectSeries(t, metrics.ProxyResourceGroupSearchVectors))
}

func TestObserveResourceGroupRequestEmitsNoSeriesForAnUnscopedRequest(t *testing.T) {
	paramtable.Init()
	metrics.ProxyResourceGroupFunctionCall.Reset()

	observeResourceGroupRequest(context.Background(), milvuspb.MilvusService_Search_FullMethodName,
		&milvuspb.SearchRequest{DbName: "db", CollectionName: "coll"},
		&milvuspb.SearchResults{Status: merr.Success()}, nil)

	assert.Equal(t, 0, collectSeries(t, metrics.ProxyResourceGroupFunctionCall))
}

func TestObserveResourceGroupRequestCountsTotalAndOutcome(t *testing.T) {
	paramtable.Init()
	metrics.ProxyResourceGroupFunctionCall.Reset()
	nodeID := paramtable.GetStringNodeID()
	ctx := extension.WithQueryResourceGroup(context.Background(), "rg-a")
	req := &milvuspb.SearchRequest{DbName: "db", CollectionName: "coll"}

	observeResourceGroupRequest(ctx, milvuspb.MilvusService_Search_FullMethodName, req,
		&milvuspb.SearchResults{Status: merr.Success()}, nil)
	observeResourceGroupRequest(ctx, milvuspb.MilvusService_Search_FullMethodName, req,
		&milvuspb.SearchResults{Status: merr.Status(merr.WrapErrServiceInternal("boom"))}, nil)

	assert.Equal(t, float64(2), counterValue(t, metrics.ProxyResourceGroupFunctionCall,
		nodeID, "Search", metrics.TotalLabel, metrics.CauseNA, "db", "coll", "rg-a"))
	assert.Equal(t, float64(1), counterValue(t, metrics.ProxyResourceGroupFunctionCall,
		nodeID, "Search", metrics.SuccessLabel, metrics.CauseNA, "db", "coll", "rg-a"))
	assert.Equal(t, float64(1), counterValue(t, metrics.ProxyResourceGroupFunctionCall,
		nodeID, "Search", metrics.FailLabel, metrics.CauseSystem, "db", "coll", "rg-a"))
	assert.Equal(t, 3, collectSeries(t, metrics.ProxyResourceGroupFunctionCall))
}

func TestObserveResourceGroupRequestIgnoresANameWithoutAMethod(t *testing.T) {
	paramtable.Init()
	metrics.ProxyResourceGroupFunctionCall.Reset()
	ctx := extension.WithQueryResourceGroup(context.Background(), "rg-a")

	observeResourceGroupRequest(ctx, "", &milvuspb.SearchRequest{}, nil, nil)
	observeResourceGroupRequest(ctx, "/milvus.proto.milvus.MilvusService/", &milvuspb.SearchRequest{}, nil, nil)

	assert.Equal(t, 0, collectSeries(t, metrics.ProxyResourceGroupFunctionCall))
}

// A hook whose After refuses the result, after Before scoped the request.
type scopingHookRefusingAfter struct {
	scopingHook
}

func (h scopingHookRefusingAfter) After(context.Context, interface{}, error, string) error {
	return errors.New("refused after")
}

func searchThroughHook(t *testing.T) {
	t.Helper()
	_, _ = HookInterceptor(context.Background(),
		&milvuspb.SearchRequest{DbName: "db", CollectionName: "coll"}, "user",
		milvuspb.MilvusService_Search_FullMethodName,
		func(ctx context.Context, req any) (any, error) {
			return &milvuspb.SearchResults{Status: merr.Success()}, nil
		})
}

// The resource group a hook pins in Before is only visible on the context the
// handler runs under, so the interceptor is where a scoped request is counted.
func TestHookInterceptorCountsAScopedRequestAgainstItsResourceGroup(t *testing.T) {
	paramtable.Init()
	metrics.ProxyResourceGroupFunctionCall.Reset()
	hookutil.InitOnceHook()
	hookutil.SetTestHook(scopingHook{resourceGroup: "rg-a"})
	defer hookutil.SetTestHook(hookutil.DefaultHook{})

	searchThroughHook(t)

	nodeID := paramtable.GetStringNodeID()
	assert.Equal(t, float64(1), counterValue(t, metrics.ProxyResourceGroupFunctionCall,
		nodeID, "Search", metrics.TotalLabel, metrics.CauseNA, "db", "coll", "rg-a"))
	assert.Equal(t, float64(1), counterValue(t, metrics.ProxyResourceGroupFunctionCall,
		nodeID, "Search", metrics.SuccessLabel, metrics.CauseNA, "db", "coll", "rg-a"))
}

func TestHookInterceptorCountsARefusedAfterAsARejection(t *testing.T) {
	paramtable.Init()
	metrics.ProxyResourceGroupFunctionCall.Reset()
	hookutil.InitOnceHook()
	hookutil.SetTestHook(scopingHookRefusingAfter{scopingHook{resourceGroup: "rg-a"}})
	defer hookutil.SetTestHook(hookutil.DefaultHook{})

	searchThroughHook(t)

	assert.Equal(t, float64(1), counterValue(t, metrics.ProxyResourceGroupFunctionCall,
		paramtable.GetStringNodeID(), "Search", metrics.RejectedLabel, metrics.CauseUser, "db", "coll", "rg-a"),
		"the client sees the hook's refusal, so the scoped request is counted as refused")
}

func TestHookInterceptorLeavesNoResourceGroupSeriesWithTheDefaultHook(t *testing.T) {
	paramtable.Init()
	metrics.ProxyResourceGroupFunctionCall.Reset()
	hookutil.InitOnceHook()
	hookutil.SetTestHook(hookutil.DefaultHook{})

	searchThroughHook(t)

	assert.Equal(t, 0, collectSeries(t, metrics.ProxyResourceGroupFunctionCall),
		"a stock binary scopes nothing, so it must expose no per-resource-group series")
}
