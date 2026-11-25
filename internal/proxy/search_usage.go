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

	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/querymeter"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func reportSearchUsage(ctx context.Context, reporter hook.Extension, usage *querymeter.SearchUsage, result *milvuspb.SearchResults, op, db string) {
	if usage == nil || !merr.Ok(result.GetStatus()) {
		return
	}
	if err := ctx.Err(); err != nil {
		result.Status = merr.Status(err)
		return
	}
	returned, err := storage.LogicalSearchResultBytes(result.GetResults(), usage.ResultSchema())
	if err != nil {
		result.Status = merr.Status(err)
		return
	}
	bytes, err := usage.Bytes(returned)
	if err != nil {
		result.Status = merr.Status(err)
		return
	}
	user := GetCurUserFromContextOrDefault(ctx)
	value := reporter.Report(map[string]any{
		hookutil.OpTypeKey:   op,
		hookutil.DatabaseKey: db,
		hookutil.UsernameKey: user,
		hookutil.BytesKey:    bytes,
	})
	SetReportValue(result.GetStatus(), value)
	metrics.ProxyReportValue.WithLabelValues(paramtable.GetStringNodeID(), op, db, user).Add(float64(value))
}
