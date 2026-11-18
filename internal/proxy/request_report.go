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
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// A request keeps its measurements across attempts until the outer RPC completes.
type requestReport struct {
	reporter       hook.Extension
	op, db, user   string
	writeBytes     int64
	readCount      int64
	collectionSize *int64
}

func newRequestReport(ctx context.Context, op, db string) *requestReport {
	reporter := extension.RequestUsageReporter()
	if reporter == nil {
		return nil
	}
	if db == "" {
		db = GetCurDBNameFromContextOrDefault(ctx)
	}
	return &requestReport{reporter: reporter, op: op, db: db, user: GetCurUserFromContextOrDefault(ctx)}
}

// Existing extensions report each attempt at its original boundary.
func reportLegacy(status *commonpb.Status, info map[string]any) {
	value := hookutil.GetExtension().Report(info)
	SetReportValue(status, value)
	if merr.Ok(status) {
		op := info[hookutil.OpTypeKey].(string)
		db := info[hookutil.DatabaseKey].(string)
		user := info[hookutil.UsernameKey].(string)
		metrics.ProxyReportValue.WithLabelValues(paramtable.GetStringNodeID(), op, db, user).Add(float64(value))
	}
}

func (r *requestReport) finish(ctx context.Context, status *commonpb.Status) *commonpb.Status {
	if r == nil || r.op == "" {
		return status
	}
	op := r.op
	r.op = ""
	if status == nil || ctx.Err() != nil || !merr.Ok(status) {
		return status
	}
	switch op {
	case hookutil.OpTypeInsert, hookutil.OpTypeUpsert, hookutil.OpTypeDelete:
		r.add(ctx, op, map[string]any{hookutil.WriteBytesKey: r.writeBytes}, 1)
		if r.readCount == 0 {
			return status
		}
		// Filtered Delete validated the lookup's size before writing any tombstones.
		op = hookutil.OpTypeQuery
	}
	if r.collectionSize == nil {
		return merr.Status(merr.WrapErrServiceInternalMsg("query plan missing collection size for request usage"))
	}
	count, copies := r.readCount, int64(1)
	if count > 1 && *r.collectionSize > int64(math.MaxInt)/count {
		// Report accepts an int increment. Identical query items have the
		// same size, so one item's result can be added with its multiplicity.
		count, copies = 1, count
	}
	r.add(ctx, op, map[string]any{
		hookutil.ReadCountKey:              count,
		hookutil.CollectionStorageBytesKey: *r.collectionSize,
	}, copies)
	return status
}

func (r *requestReport) add(ctx context.Context, op string, info map[string]any, copies int64) {
	info[hookutil.OpTypeKey] = op
	info[hookutil.DatabaseKey] = r.db
	info[hookutil.UsernameKey] = r.user
	bytes := r.reporter.Report(info)
	if bytes < 0 {
		mlog.RatedWarn(ctx, 1, "request extension returned negative usage", mlog.String("operation", op), mlog.Int("bytes", bytes))
		return
	}
	metrics.ProxyReportValue.WithLabelValues(paramtable.GetStringNodeID(), op, r.db, r.user).Add(float64(bytes) * float64(copies))
}
