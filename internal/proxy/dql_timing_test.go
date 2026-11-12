// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"time"

	"github.com/bytedance/mockey"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/stage"
)

func requestStageSum(t *testing.T, op, path, cohort, name, result string) float64 {
	t.Helper()
	m := &dto.Metric{}
	require.NoError(t, metrics.QueryRequestStageDuration.WithLabelValues(op, path, cohort, name, result).(prometheus.Metric).Write(m))
	return m.GetHistogram().GetSampleSum()
}

func TestDQLTimingAccumulatesRetryAttempts(t *testing.T) {
	_, timer := startDQL(context.Background(), "Search")
	timer.path = "unloaded"
	timer.switchStage(dqlStageExecution, timer.started.Add(10*time.Millisecond))
	timer.switchStage(dqlStageRetryWait, timer.started.Add(1010*time.Millisecond))
	timer.switchStage(dqlStageReadiness, timer.started.Add(1210*time.Millisecond))
	timer.switchStage(dqlStageExecution, timer.started.Add(1230*time.Millisecond))
	before := [4]float64{}
	for i, s := range []string{"total", "readiness", "execution", "retry_wait"} {
		before[i] = requestStageSum(t, "Search", "unloaded", "gt1s", s, "success")
	}
	timer.finish(timer.started.Add(1260*time.Millisecond), stage.Success)
	got := [4]float64{}
	for i, s := range []string{"total", "readiness", "execution", "retry_wait"} {
		got[i] = requestStageSum(t, "Search", "unloaded", "gt1s", s, "success") - before[i]
	}
	require.InDelta(t, 1.26, got[0], 1e-9)
	require.InDelta(t, 0.03, got[1], 1e-9)
	require.InDelta(t, 1.03, got[2], 1e-9)
	require.InDelta(t, 0.2, got[3], 1e-9)
	require.InDelta(t, got[0], got[1]+got[2]+got[3], 1e-9)
}

func TestDQLTimingCountsEmbeddedErrorBeforeExecution(t *testing.T) {
	_, timer := startDQL(context.Background(), "Query")
	timer.path = "loading"
	before := requestStageSum(t, "Query", "loading", "le1s", "execution", "error")
	metric := metrics.QueryRequestStageDuration.WithLabelValues("Query", "loading", "le1s", "execution", "error").(prometheus.Metric)
	beforeMetric := &dto.Metric{}
	require.NoError(t, metric.Write(beforeMetric))
	timer.End(merr.Status(merr.WrapErrCollectionNotLoaded(1)), nil)
	afterMetric := &dto.Metric{}
	require.NoError(t, metric.Write(afterMetric))
	require.Equal(t, beforeMetric.GetHistogram().GetSampleCount()+1, afterMetric.GetHistogram().GetSampleCount())
	require.Equal(t, before, requestStageSum(t, "Query", "loading", "le1s", "execution", "error"))
}

func TestDQLPathKeepsColdestKnownObservation(t *testing.T) {
	ctx, timer := startDQL(context.Background(), "Search")
	setDQLPath(ctx, stage.PathLoading)
	setDQLPath(ctx, stage.PathUnloaded)
	setDQLPath(ctx, stage.PathReady)
	setDQLPath(ctx, "")
	setDQLPath(ctx, "unexpected")
	require.Equal(t, stage.PathUnloaded, timer.path)
	timer.finish(time.Now(), stage.Success)
}

func TestDQLRetryRecordsCoordPathAndReadiness(t *testing.T) {
	enableAutoLoad(t)
	coord := mocks.NewMockMixCoordClient(t)
	coord.EXPECT().EnsureCollectionReady(mock.Anything, mock.Anything).Return(
		&commonpb.Status{ExtraInfo: map[string]string{stage.ReadinessPathKey: stage.PathUnloaded}}, nil).Once()
	coord.EXPECT().EnsureCollectionReady(mock.Anything, mock.Anything).Return(
		&commonpb.Status{ExtraInfo: map[string]string{stage.ReadinessPathKey: stage.PathReady}}, nil).Once()
	node := &Proxy{metaCache: mockSearchCollectionMeta(t, 100, []string{"v0"}), mixCoord: coord}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	ctx, timer := startDQL(context.Background(), "Search")
	calls := 0
	err := node.retryDQL(ctx, "db", "collection", nil, func(context.Context) (bool, error) {
		calls++
		if calls == 1 {
			return false, merr.Wrap(merr.ErrQueryViewNotReady, "view replaced by balance")
		}
		return false, nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, calls)
	// The retry found the collection loaded, but this request waited for the load.
	require.Equal(t, stage.PathUnloaded, timer.path)
	require.True(t, timer.executionStarted)
	require.Equal(t, dqlStageNone, timer.currentStage)
	timer.End(merr.Success(), nil)
}

func TestDQLAutoLoadRetryAccumulatesAttemptTiming(t *testing.T) {
	enableAutoLoad(t)
	node := &Proxy{metaCache: mockSearchCollectionMeta(t, 100, []string{"v0"})}
	node.UpdateStateCode(commonpb.StateCode_Healthy)
	readinessCalls := 0
	ready := mockey.Mock((*Proxy).ensureCollectionReady).To(
		func(*Proxy, context.Context, string, string) error {
			readinessCalls++
			if readinessCalls == 1 {
				time.Sleep(5 * time.Millisecond)
			} else {
				time.Sleep(10 * time.Millisecond)
			}
			return nil
		}).Build()
	defer ready.UnPatch()

	ctx, timing := startDQL(context.Background(), "Query")
	defer timing.End(merr.Success(), nil)
	executeCalls := 0
	err := node.retryDQL(ctx, "db", "collection", nil, func(context.Context) (bool, error) {
		executeCalls++
		if executeCalls == 1 {
			time.Sleep(20 * time.Millisecond)
			return false, merr.Wrap(merr.ErrQueryViewNotReady, "collection released")
		}
		time.Sleep(30 * time.Millisecond)
		return false, nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, readinessCalls)
	require.Equal(t, 2, executeCalls)
	require.Equal(t, dqlStageNone, timing.currentStage)
	require.GreaterOrEqual(t, timing.stageDurations[dqlStageReadiness], 15*time.Millisecond)
	require.GreaterOrEqual(t, timing.stageDurations[dqlStageExecution], 50*time.Millisecond)
	// retry.Handle sleeps at least its default 200ms before the second attempt.
	require.GreaterOrEqual(t, timing.stageDurations[dqlStageRetryWait], 200*time.Millisecond)
}
