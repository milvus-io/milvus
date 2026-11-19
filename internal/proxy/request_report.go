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
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

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

// shouldReportUsage checks the final outcome before publishing byte measurements.
func shouldReportUsage(ctx context.Context, status *commonpb.Status) bool {
	return status != nil && merr.Ok(status) && ctx.Err() == nil
}

func reportReadUsage(ctx context.Context, reporter hook.Extension, op, db string, status *commonpb.Status, collectionBytes *int64, count int64) *commonpb.Status {
	if reporter == nil || !shouldReportUsage(ctx, status) {
		return status
	}
	if collectionBytes == nil {
		return merr.Status(merr.WrapErrServiceInternalMsg("query plan missing collection size for request usage"))
	}
	copies := int64(1)
	if count > 1 && *collectionBytes > int64(math.MaxInt)/count {
		// Report returns an int. Report one item and apply its multiplicity to the counter.
		copies, count = count, 1
	}
	reportUsage(ctx, reporter, op, db, map[string]any{
		hookutil.CollectionStorageBytesKey: *collectionBytes,
		hookutil.ReadCountKey:              count,
	}, copies)
	return status
}

func reportUsage(ctx context.Context, reporter hook.Extension, op, db string, info map[string]any, copies int64) {
	if db == "" {
		db = GetCurDBNameFromContextOrDefault(ctx)
	}
	user := GetCurUserFromContextOrDefault(ctx)
	info[hookutil.OpTypeKey] = op
	info[hookutil.DatabaseKey] = db
	info[hookutil.UsernameKey] = user
	bytes := reporter.Report(info)
	if bytes < 0 {
		mlog.RatedWarn(ctx, 1, "request extension returned negative usage", mlog.String("operation", op), mlog.Int("bytes", bytes))
		return
	}
	metrics.ProxyReportValue.WithLabelValues(paramtable.GetStringNodeID(), op, db, user).Add(float64(bytes) * float64(copies))
}
