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

package indexmanager

import (
	"context"

	"github.com/cockroachdb/errors"

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/internal/pkindex/manifest"
	"github.com/milvus-io/milvus/internal/streamingnode/server/pkindex/index"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

// The classes of a fatal error, see ErrorClass and the pkindex_fatal_total metric.
const (
	ErrorClassCorrupted   = "corrupted"
	ErrorClassIO          = "io"
	ErrorClassClosed      = "closed"
	ErrorClassCanceled    = "canceled"
	ErrorClassUnavailable = "unavailable"
	ErrorClassOther       = "other"
)

// ErrorClass classifies an error by the markers of the pkindex packages, the
// most specific first. The recovery and the install task record it in the
// fatal metric and use it to decide whether the WAL becomes unavailable.
func ErrorClass(err error) string {
	switch {
	case errors.Is(err, pkroot.ErrCorrupted):
		return ErrorClassCorrupted
	case errors.Is(err, engine.ErrClosed):
		return ErrorClassClosed
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return ErrorClassCanceled
	case errors.Is(err, pkroot.ErrIO):
		return ErrorClassIO
	case errors.Is(err, pkroot.ErrUnavailable):
		return ErrorClassUnavailable
	default:
		return ErrorClassOther
	}
}

// installTask installs the committed table set of one Index and marks it
// ready. A transient error retries with backoff until the task is canceled
// or the Index closes; a fatal one (a missing or corrupted object) goes to
// OnFatal.
type installTask struct {
	idx     index.Index
	params  func() Params
	onFatal func(error)
	logger  *mlog.Logger
}

// Execute implements nodescheduler.Task. The scheduler cancels ctx when the
// handle of the task is canceled. It returns nil after a fatal error, which
// OnFatal and the log line here report, and after the Index closed.
func (t *installTask) Execute(ctx context.Context) error {
	p := t.params()
	attempts := 0
	err := retry.Do(ctx, func() error {
		err := t.idx.Install(ctx)
		if err != nil && (manifest.IsFatal(err) || errors.Is(err, engine.ErrClosed)) {
			return retry.Unrecoverable(err)
		}
		if err != nil {
			attempts++
			fields := []mlog.Field{mlog.FieldVChannel(t.idx.VChannel()), mlog.Int("attempts", attempts), mlog.Err(err)}
			if attempts == 1 {
				t.logger.Warn(ctx, "primary key index install failed, retrying", fields...)
			} else {
				t.logger.Debug(ctx, "primary key index install failed, retrying", fields...)
			}
		}
		return err
	}, retry.AttemptAlways(), retry.Sleep(p.BackoffBase), retry.MaxSleepTime(p.MaxBackoff))
	if err == nil || ctx.Err() != nil || errors.Is(err, engine.ErrClosed) {
		return nil
	}
	class := ErrorClass(err)
	t.logger.Error(ctx, "primary key index install failed, the WAL becomes unavailable", mlog.FieldVChannel(t.idx.VChannel()), mlog.String("class", class), mlog.Err(err))
	metrics.StreamingNodePKIndexFatalTotal.WithLabelValues(paramtable.GetStringNodeID(), class).Inc()
	t.onFatal(err)
	return nil
}
