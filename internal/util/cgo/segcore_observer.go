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

	// UnexpectedError(2001) is the "could not classify this" bucket, so the
	// code alone says nothing; the C++ source location that raised it is the
	// actionable part, and AssertInfo/ThrowInfo put it in the message.
	//
	// A site showing up here is either a genuine Milvus bug or a condition
	// that is really driven by external input (corrupt file, full disk, OOM)
	// and should carry a specific retriable code instead. Both are worth a
	// look, which is why the counter is per-origin rather than a single total.
	merr.RegisterUnexpectedSegcoreOriginObserver(func(origin string) {
		if origin == "" {
			// No location in the message (e.g. a bare std::exception caught at
			// the boundary). Keep one bucket for these rather than dropping
			// them: a large unknown share means the message plumbing lost the
			// location, which is itself worth knowing.
			origin = "unknown"
		}
		metrics.UnexpectedSegcoreOriginTotal.WithLabelValues(origin).Inc()
		mlog.RatedWarn(context.TODO(), rate.Limit(0.1),
			"segcore reported an unclassified internal error (2001); if this origin is driven by "+
				"external input it should carry a specific code instead",
			mlog.String("origin", origin))
	})
}
