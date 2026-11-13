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

package policy

import (
	"context"
	"time"

	"golang.org/x/time/rate"

	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

type DeleteCleanupThresholds struct {
	MaxLogs  int64
	Ratio    float64
	MaxBytes int64
}

type deleteCleanupReason uint8

const (
	belowDeleteThresholds deleteCleanupReason = iota
	tooManyDeltaLogs
	tooManyDeletedRows
	tooManyDeltaBytes
)

func (thresholds DeleteCleanupThresholds) reason(stats *datapb.Statistics, rows int64) deleteCleanupReason {
	switch {
	case stats.GetDeltaBinlogCount() > thresholds.MaxLogs:
		return tooManyDeltaLogs
	case float64(stats.GetDeleteNumRows())/float64(rows) >= thresholds.Ratio:
		return tooManyDeletedRows
	case stats.GetDeltaBinlogSize() > thresholds.MaxBytes:
		return tooManyDeltaBytes
	default:
		return belowDeleteThresholds
	}
}

func (thresholds DeleteCleanupThresholds) Match(stats *datapb.Statistics, rows int64) bool {
	return thresholds.reason(stats, rows) != belowDeleteThresholds
}

func HasTooManyDeletions(ctx context.Context, segment *meta.SegmentInfo) bool {
	thresholds := DeleteCleanupThresholds{
		MaxLogs:  Params.DataCoordCfg.SingleCompactionDeltalogMaxNum.GetAsInt64(),
		Ratio:    Params.DataCoordCfg.SingleCompactionRatioThreshold.GetAsFloat(),
		MaxBytes: Params.DataCoordCfg.SingleCompactionDeltaLogMaxSize.GetAsInt64(),
	}
	stats := segment.EnsureStats()
	switch thresholds.reason(stats, segment.GetNumOfRows()) {
	case tooManyDeltaLogs:
		mlog.RatedInfo(ctx, rate.Limit(1), "delta logs file count exceeds threshold",
			mlog.FieldSegmentID(segment.GetID()),
			mlog.Int64("delta log count", stats.GetDeltaBinlogCount()),
			mlog.Int64("file number threshold", thresholds.MaxLogs),
		)
	case tooManyDeletedRows:
		mlog.RatedInfo(ctx, rate.Limit(1), "deleted entities rows proportion exceeds threshold",
			mlog.FieldSegmentID(segment.GetID()),
			mlog.Int64("number of rows", segment.GetNumOfRows()),
			mlog.Int64("deleted rows", stats.GetDeleteNumRows()),
			mlog.Float64("proportion threshold", thresholds.Ratio),
		)
	case tooManyDeltaBytes:
		mlog.RatedInfo(ctx, rate.Limit(1), "total delete entries size exceeds threshold",
			mlog.FieldSegmentID(segment.GetID()),
			mlog.Int64("numRows", segment.GetNumOfRows()),
			mlog.Int64("delete entries size", stats.GetDeltaBinlogSize()),
			mlog.Int64("size threshold", thresholds.MaxBytes),
		)
	default:
		return false
	}
	return true
}

func IsStrictlyExpired(earliest uint64, cutoff time.Time) bool {
	return tsoutil.PhysicalTime(earliest).Before(cutoff)
}

func ExpiredRowsExceedThreshold(stats *datapb.Statistics, commitTs, horizon uint64, ratio float64, maxBytes int64) (bool, float64, int64) {
	var fraction float64
	if commitTs != 0 {
		if commitTs < horizon {
			fraction = 1
		}
	} else {
		quantiles := stats.GetTimestampQuantiles()
		qualifying := 0
		for _, q := range quantiles {
			if q <= 0 || uint64(q) >= horizon {
				break
			}
			qualifying++
		}
		// Shift the estimate down one bucket because expired rows may have
		// smaller binlogs than the segment-wide average used below.
		if qualifying >= 2 {
			fraction = float64(qualifying-1) / float64(len(quantiles))
		}
	}
	approxBytes := int64(fraction * float64(stats.GetInsertBinlogSize()))
	return fraction >= ratio || approxBytes > maxBytes, fraction, approxBytes
}

func IsTTLFieldExpired(quantiles []int64, now time.Time, ratio float64) bool {
	if len(quantiles) == 0 {
		return false
	}
	// Expiration quantiles are at 20%, 40%, 60%, 80%, and 100%. The
	// 0.01 offset preserves the legacy bucket selection around boundaries.
	bucket := min(max(int((ratio+0.01)/0.2)-1, 0), len(quantiles)-1)
	expiry := quantiles[bucket]
	return expiry > 0 && now.UnixMicro() >= expiry
}
