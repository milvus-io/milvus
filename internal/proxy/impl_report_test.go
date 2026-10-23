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
	"strconv"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/proxy/scheduler"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/ratelimitutil"
)

type upsertReportExtension struct {
	hookutil.DefaultExtension
	value int
	calls int
}

func (e *upsertReportExtension) Report(any) int {
	e.calls++
	return e.value
}

func TestProxy_Upsert_ReportValue(t *testing.T) {
	paramtable.Init()
	originalExtension := hookutil.GetExtension()
	t.Cleanup(func() { hookutil.SetTestExtension(originalExtension) })
	originalRateCollector := rateCol
	var err error
	rateCol, err = ratelimitutil.NewRateCollector(ratelimitutil.DefaultWindow, ratelimitutil.DefaultGranularity, false)
	require.NoError(t, err)
	t.Cleanup(func() { rateCol = originalRateCollector })

	for _, tc := range []struct {
		name         string
		value        int
		enqueueErr   error
		taskErr      error
		resultErr    error
		wantSuccess  bool
		wantReports  int
		wantIncrease float64
	}{
		{name: "positive", value: 10, wantSuccess: true, wantReports: 2, wantIncrease: 20},
		{name: "zero", wantSuccess: true, wantReports: 2},
		{name: "enqueue failure", value: 10, enqueueErr: merr.ErrServiceTooManyRequests},
		{name: "task failure", value: 10, taskErr: merr.ErrServiceUnavailable},
		{name: "result failure", value: 10, resultErr: merr.ErrServiceUnavailable, wantReports: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			extension := &upsertReportExtension{value: tc.value}
			hookutil.SetTestExtension(extension)
			t.Cleanup(func() { hookutil.SetTestExtension(originalExtension) })

			dbName := t.Name()
			cache := NewMockCache(t)
			cache.EXPECT().GetCollectionSchema(mock.Anything, dbName, "collection").Return(nil, nil).Twice()
			node := &Proxy{
				metaCache: cache,
				sched:     &scheduler.TaskScheduler{DmQueue: &scheduler.DmTaskQueue{}},
			}
			node.UpdateStateCode(commonpb.StateCode_Healthy)

			enqueue := mockey.Mock((*scheduler.DmTaskQueue).Enqueue).To(func(_ *scheduler.DmTaskQueue, queued taskmodel.Task) error {
				if tc.enqueueErr != nil {
					return tc.enqueueErr
				}
				if tc.resultErr != nil {
					queued.(*upsertTask).Result().Status = merr.Status(tc.resultErr)
				}
				queued.Notify(tc.taskErr)
				return nil
			}).Build()
			t.Cleanup(func() { enqueue.UnPatch() })
			upsertMsg := mockey.Mock((*upsertTask).UpsertMsg).Return(&msgstream.UpsertMsg{
				InsertMsg: &msgstream.InsertMsg{},
				DeleteMsg: &msgstream.DeleteMsg{},
			}).Build()
			t.Cleanup(func() { upsertMsg.UnPatch() })

			ctx := context.Background()
			labels := []string{paramtable.GetStringNodeID(), hookutil.OpTypeUpsert, dbName, GetCurUserFromContextOrDefault(ctx)}
			counter := metrics.ProxyReportValue.WithLabelValues(labels...)
			t.Cleanup(func() { metrics.ProxyReportValue.DeleteLabelValues(labels...) })
			before := testutil.ToFloat64(counter)
			for i := 0; i < 2; i++ {
				previous := testutil.ToFloat64(counter)
				result, err := node.Upsert(ctx, &milvuspb.UpsertRequest{
					DbName: dbName, CollectionName: "collection", NumRows: 1,
				})
				require.NoError(t, err)
				require.Equal(t, tc.wantSuccess, merr.Ok(result.GetStatus()))
				if tc.wantSuccess && tc.value > 0 {
					require.Equal(t, strconv.Itoa(tc.value), result.GetStatus().GetExtraInfo()["report_value"])
					require.Equal(t, float64(tc.value), testutil.ToFloat64(counter)-previous)
				} else {
					require.NotContains(t, result.GetStatus().GetExtraInfo(), "report_value")
					require.Equal(t, previous, testutil.ToFloat64(counter))
				}
			}
			require.Equal(t, tc.wantReports, extension.calls)
			require.Equal(t, tc.wantIncrease, testutil.ToFloat64(counter)-before)
		})
	}
}
