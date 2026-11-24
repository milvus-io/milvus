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
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/ratelimitutil"
)

type queryUsageReporter struct {
	hookutil.DefaultExtension
	reports []any
}

func (r *queryUsageReporter) Report(report any) int {
	r.reports = append(r.reports, report)
	return 0
}

func TestQueryReportsOneUsageValue(t *testing.T) {
	paramtable.Init()
	previousRateCollector := rateCol
	var err error
	rateCol, err = ratelimitutil.NewRateCollector(ratelimitutil.DefaultWindow, ratelimitutil.DefaultGranularity, false)
	require.NoError(t, err)
	t.Cleanup(func() { rateCol = previousRateCollector })

	mockey.PatchConvey("only a successful Query emits usage", t, func() {
		node := &Proxy{}
		request := &milvuspb.QueryRequest{DbName: "db", CollectionName: "collection"}
		reporter := &queryUsageReporter{}
		oldReporter := extension.RequestUsageReporter()
		extension.SetRequestUsageReporter(reporter)
		t.Cleanup(func() { extension.SetRequestUsageReporter(oldReporter) })
		mockey.Mock((*Proxy).ensureCollectionReady).Return(nil).Build()
		usageBytes := int64(73)
		var execErr error
		var cancel context.CancelFunc
		mockey.Mock((*Proxy).executeQuery).To(func(_ *Proxy, _ context.Context, _ *milvuspb.QueryRequest, metered bool) (*milvuspb.QueryResults, int64, error) {
			require.True(t, metered)
			if cancel != nil {
				cancel()
			}
			return &milvuspb.QueryResults{Status: merr.Status(execErr)}, usageBytes, execErr
		}).Build()

		response, err := node.Query(t.Context(), request)
		require.NoError(t, err)
		require.True(t, merr.Ok(response.GetStatus()))
		require.Len(t, reporter.reports, 1)
		report := reporter.reports[0].(map[string]any)
		require.Len(t, report, 4)
		require.Equal(t, int64(73), report["bytes"])

		execErr = merr.WrapErrServiceInternalMsg("missing query read usage")
		response, err = node.Query(t.Context(), request)
		require.NoError(t, err)
		require.False(t, merr.Ok(response.GetStatus()))
		require.Len(t, reporter.reports, 1)

		execErr = nil
		ctx, cancelQuery := context.WithCancel(t.Context())
		cancel = cancelQuery
		response, err = node.Query(ctx, request)
		require.NoError(t, err)
		require.False(t, merr.Ok(response.GetStatus()))
		require.Len(t, reporter.reports, 1)

		cancel = nil
		request.QueryParams = []*commonpb.KeyValuePair{{Key: IteratorField, Value: "true"}}
		for i, pageBytes := range []int64{90, 20} {
			request.GuaranteeTimestamp = uint64(i) * 100
			usageBytes = pageBytes
			response, err = node.Query(t.Context(), request)
			require.NoError(t, err)
			require.True(t, merr.Ok(response.GetStatus()))
			require.Len(t, reporter.reports, i+2)
			page := reporter.reports[i+1].(map[string]any)
			require.Equal(t, pageBytes, page["bytes"])
		}
	})
}
