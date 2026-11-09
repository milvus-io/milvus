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

package initcore

import (
	"math"

	"github.com/milvus-io/milvus/pkg/v3/util/hardware"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// loonReaderAuto is the value of common.storage.readerThreadPoolSize and
// common.storage.indexBuildReadWindowBytes that asks for a hardware-derived
// setting. Negative values were rejected before it existed, so introducing it
// does not change the meaning of any explicit configuration.
const loonReaderAuto = -1

// arrowDefaultRangeSizeLimitBytes is arrow's CacheOptions::Defaults()
// range_size_limit, used when common.arrow.reader.rangeSizeLimitBytes is 0.
const arrowDefaultRangeSizeLimitBytes = 32 << 20

// arrowDefaultIOThreadPoolCapacity is arrow's built-in IO pool size
// (kDefaultNumIoThreads), which the pool keeps when
// common.arrow.ioThreadPoolCoefficient is 0.
const arrowDefaultIOThreadPoolCapacity = 8

// autoIndexBuildReadWindowRangesPerIOThread is how many coalesced read
// ranges one automatically sized read window holds per arrow IO thread. One
// round feeds every IO thread one range; the second one keeps them busy while
// the reader moves between rounds.
const autoIndexBuildReadWindowRangesPerIOThread = 2

// loonReaderAutoApplies reports whether the automatic settings apply to this
// process. They only do on a dedicated DataNode: the reader pool is a
// process-wide singleton and the read window is buffered per running build
// task, so in a process shared with QueryNode (standalone, mixed roles) the
// automatic value keeps the sequential behavior, 0, instead.
func loonReaderAutoApplies() bool {
	return paramtable.GetRole() == typeutil.DataNodeRole
}

// ResolveLoonReaderThreadPoolSize returns the milvus-storage reader thread
// pool size to apply for common.storage.readerThreadPoolSize: an explicit
// value in [0, 1024] as is, and the automatic value (-1) as one thread per
// CPU core on a DataNode, 0 elsewhere.
func ResolveLoonReaderThreadPoolSize(params *paramtable.ComponentParam) (int64, error) {
	poolSize := params.CommonCfg.StorageReaderThreadPoolSize.GetAsInt64()
	if poolSize == loonReaderAuto {
		if !loonReaderAutoApplies() {
			return 0, nil
		}
		return min(int64(max(1, hardware.GetCPUNum())), maxStorageReaderThreadPoolSize), nil
	}
	if poolSize < 0 || poolSize > maxStorageReaderThreadPoolSize {
		return 0, merr.WrapErrParameterInvalidMsg(
			"%s must be -1 (auto) or in [0, %d], got %d",
			params.CommonCfg.StorageReaderThreadPoolSize.Key, maxStorageReaderThreadPoolSize, poolSize)
	}
	return poolSize, nil
}

// ResolveIndexBuildReadWindowBytes returns the index-build read window to
// apply for common.storage.indexBuildReadWindowBytes: an explicit value in
// [0, 4GB] as is (0 keeps milvus-storage's 32MB), and the automatic value
// (-1) on a DataNode as enough bytes for every arrow IO thread to have
// autoIndexBuildReadWindowRangesPerIOThread coalesced ranges in flight,
// capped at 4GB. The arrow IO pool is the real ceiling on concurrent object
// storage reads, so a larger window buys no extra concurrency, while a window
// spanning a single row group (the 32MB default) leaves all but one IO thread
// idle. Elsewhere the automatic value is 0.
func ResolveIndexBuildReadWindowBytes(params *paramtable.ComponentParam) (int64, error) {
	window := params.CommonCfg.IndexBuildReadWindowBytes.GetAsInt64()
	if window == loonReaderAuto {
		if !loonReaderAutoApplies() {
			return 0, nil
		}
		rangeSize := params.CommonCfg.ArrowReaderRangeSizeLimitBytes.GetAsInt64()
		if rangeSize <= 0 {
			rangeSize = arrowDefaultRangeSizeLimitBytes
		}
		return autoIndexBuildReadWindowBytes(arrowIOThreadsForReadWindow(), rangeSize), nil
	}
	if window < 0 || window > maxIndexBuildReadWindowBytes {
		return 0, merr.WrapErrParameterInvalidMsg(
			"%s must be -1 (auto) or in [0, %d], got %d",
			params.CommonCfg.IndexBuildReadWindowBytes.Key, maxIndexBuildReadWindowBytes, window)
	}
	return window, nil
}

// arrowIOThreadsForReadWindow returns the arrow IO pool size the automatic
// read window is sized for: the configured capacity, or arrow's built-in
// default when the coefficient is 0 and the pool keeps arrow's size.
func arrowIOThreadsForReadWindow() int64 {
	if threads := ResolveArrowIOThreadPoolCapacity(); threads > 0 {
		return int64(threads)
	}
	return arrowDefaultIOThreadPoolCapacity
}

// autoIndexBuildReadWindowBytes sizes the window for ioThreads arrow IO
// threads reading ranges of rangeSize bytes, without overflowing.
func autoIndexBuildReadWindowBytes(ioThreads, rangeSize int64) int64 {
	perRound := max(ioThreads, 1) * autoIndexBuildReadWindowRangesPerIOThread
	if rangeSize > math.MaxInt64/perRound {
		return maxIndexBuildReadWindowBytes
	}
	return min(perRound*rangeSize, maxIndexBuildReadWindowBytes)
}
