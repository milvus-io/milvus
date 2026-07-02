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

package cgo

import (
	"context"
	"strconv"

	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// init wires the observability side-effect for unregistered segcore error codes.
// merr (a leaf package) detects the drift but cannot import metrics/mlog itself
// (both depend on merr), so it exposes a hook that this cgo-boundary package --
// which already links the C++ core and may import metrics/mlog -- fills in:
//   - a per-code counter, so the drift is alertable;
//   - a rate-limited WARN, so it is visible in logs without flooding them.
func init() {
	merr.RegisterUnmappedSegcoreCodeObserver(func(code int32) {
		metrics.UnmappedSegcoreCodeTotal.WithLabelValues(strconv.Itoa(int(code))).Inc()
		mlog.RatedWarn(context.TODO(), rate.Limit(0.1),
			"unmapped segcore error code crossed the cgo boundary; degraded to a non-retriable "+
				"system error -- register it in pkg/util/merr/segcore.go",
			mlog.Int32("code", code))
	})
}
