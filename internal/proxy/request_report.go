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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/extension"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// A hook may opt into one report per successful external request. The
// callback pins its configuration across task retries and config reloads.
// Existing hooks keep reporting each attempt at its original boundary.
type requestReport struct {
	publish        extension.RequestReport
	info           map[string]any
	collectionSize *int64
}

func newRequestReport() *requestReport {
	publish := hookutil.NewRequestReport()
	if publish == nil {
		return nil
	}
	return &requestReport{publish: publish}
}

// Legacy reports are emitted immediately. An opt-in hook sees only the final
// successful attempt after the outer RPC has settled its cancellation status.
func (r *requestReport) record(status *commonpb.Status, info map[string]any) {
	if r != nil {
		r.info = info
		return
	}
	op := info[hookutil.OpTypeKey].(string)
	db := info[hookutil.DatabaseKey].(string)
	user := info[hookutil.UsernameKey].(string)
	value := hookutil.GetExtension().Report(info)
	SetReportValue(status, value)
	if merr.Ok(status) {
		metrics.ProxyReportValue.WithLabelValues(paramtable.GetStringNodeID(), op, db, user).Add(float64(value))
	}
}

func (r *requestReport) finish(ctx context.Context, status *commonpb.Status) *commonpb.Status {
	if r == nil || r.info == nil {
		return status
	}
	info := r.info
	r.info = nil
	if status == nil || ctx.Err() != nil || !merr.Ok(status) {
		return status
	}
	db := info[hookutil.DatabaseKey].(string)
	user := info[hookutil.UsernameKey].(string)
	op := info[hookutil.OpTypeKey].(string)
	if r.collectionSize != nil {
		info[hookutil.CollectionStorageBytesKey] = *r.collectionSize
	}
	value, err := r.publish(info)
	extraRead, _ := info[hookutil.ExtraReadReportValueKey].(int)
	if err == nil && (value < 0 || extraRead < 0) {
		err = merr.WrapErrServiceInternalMsg("request reporting hook returned a negative counter increment")
	}
	if err != nil {
		mlog.Error(ctx, "failed to report request usage", mlog.String("operation", op), mlog.Err(err))
		switch op {
		case hookutil.OpTypeInsert, hookutil.OpTypeUpsert, hookutil.OpTypeDelete:
			// A committed mutation cannot be rolled back or safely retried.
			return status
		default:
			return merr.Status(merr.Wrap(err, "report request usage"))
		}
	}
	node := paramtable.GetStringNodeID()
	metrics.ProxyReportValue.WithLabelValues(node, op, db, user).Add(float64(value))
	if extraRead > 0 {
		metrics.ProxyReportValue.WithLabelValues(node, hookutil.OpTypeQuery, db, user).Add(float64(extraRead))
	}
	return status
}
