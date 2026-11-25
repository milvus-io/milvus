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
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func reportMutationUsage(ctx context.Context, status *commonpb.Status, reporter hook.Extension, op, db string, report map[string]any) {
	success := merr.Ok(status)
	if reporter != nil {
		if !success || ctx.Err() != nil {
			return
		}
	} else {
		reporter = hookutil.GetExtension()
	}
	user := GetCurUserFromContextOrDefault(ctx)
	report[hookutil.OpTypeKey] = op
	report[hookutil.DatabaseKey] = db
	report[hookutil.UsernameKey] = user
	value := reporter.Report(report)
	SetReportValue(status, value)
	if success {
		metrics.ProxyReportValue.WithLabelValues(paramtable.GetStringNodeID(), op, db, user).Add(float64(value))
	}
}
