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

package importutilv2

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestOption_GetTimeout(t *testing.T) {
	const delta = 3 * time.Second

	options := []*commonpb.KeyValuePair{{Key: Timeout, Value: "300s"}}
	ts, err := GetTimeoutTs(options)
	assert.NoError(t, err)
	pt := tsoutil.PhysicalTime(ts)
	assert.WithinDuration(t, time.Now().Add(300*time.Second), pt, delta)

	options = []*commonpb.KeyValuePair{{Key: Timeout, Value: "1.5h"}}
	ts, err = GetTimeoutTs(options)
	assert.NoError(t, err)
	pt = tsoutil.PhysicalTime(ts)
	assert.WithinDuration(t, time.Now().Add(90*time.Minute), pt, delta)

	options = []*commonpb.KeyValuePair{{Key: Timeout, Value: "1h45m"}}
	ts, err = GetTimeoutTs(options)
	assert.NoError(t, err)
	pt = tsoutil.PhysicalTime(ts)
	assert.WithinDuration(t, time.Now().Add(105*time.Minute), pt, delta)

	options = []*commonpb.KeyValuePair{{Key: Timeout, Value: "invalidTime"}}
	_, err = GetTimeoutTs(options)
	assert.Error(t, err)
}

func TestOption_ParseTimeRange(t *testing.T) {
	s, e, err := ParseTimeRange(nil)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), s)
	assert.Equal(t, uint64(math.MaxUint64), e)

	startTs := tsoutil.ComposeTSByTime(time.Now())
	options := []*commonpb.KeyValuePair{{Key: StartTs, Value: fmt.Sprintf("%d", startTs)}}
	s, e, err = ParseTimeRange(options)
	assert.NoError(t, err)
	assert.Equal(t, startTs, s)
	assert.Equal(t, uint64(math.MaxUint64), e)

	endTs := tsoutil.ComposeTSByTime(time.Now())
	options = []*commonpb.KeyValuePair{{Key: EndTs, Value: fmt.Sprintf("%d", endTs)}}
	s, e, err = ParseTimeRange(options)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), s)
	assert.Equal(t, endTs, e)

	options = []*commonpb.KeyValuePair{{Key: EndTs, Value: "&%#$%^&%^&$%^&&"}}
	_, _, err = ParseTimeRange(options)
	assert.ErrorIs(t, err, merr.ErrImportFailed)

	physicalTs := time.Now().UnixMilli()
	options = []*commonpb.KeyValuePair{{Key: EndTs, Value: fmt.Sprintf("%d", physicalTs)}}
	_, _, err = ParseTimeRange(options)
	assert.ErrorIs(t, err, merr.ErrImportFailed)

	options = []*commonpb.KeyValuePair{{Key: StartTs, Value: "0"}}
	_, _, err = ParseTimeRange(options)
	assert.ErrorIs(t, err, merr.ErrImportFailed)
}

func TestOption_SkipDiskQuotaCheck(t *testing.T) {
	// Neither backup nor l0_import, should return false
	options := []*commonpb.KeyValuePair{}
	assert.False(t, SkipDiskQuotaCheck(options))

	// backup = true, skip_disk_quota_check = true
	options = []*commonpb.KeyValuePair{
		{Key: BackupFlag, Value: "true"},
		{Key: SkipDQC, Value: "true"},
	}
	assert.True(t, SkipDiskQuotaCheck(options))

	// backup = true, skip_disk_quota_check = false
	options = []*commonpb.KeyValuePair{
		{Key: BackupFlag, Value: "true"},
		{Key: SkipDQC, Value: "false"},
	}
	assert.False(t, SkipDiskQuotaCheck(options))

	// l0_import = true, skip_disk_quota_check = true
	options = []*commonpb.KeyValuePair{
		{Key: L0Import, Value: "true"},
		{Key: SkipDQC, Value: "true"},
	}
	assert.True(t, SkipDiskQuotaCheck(options))

	// l0_import = true, skip_disk_quota_check = false
	options = []*commonpb.KeyValuePair{
		{Key: L0Import, Value: "true"},
		{Key: SkipDQC, Value: "false"},
	}
	assert.False(t, SkipDiskQuotaCheck(options))

	// backup = false, l0_import = true, skip_disk_quota_check = true
	options = []*commonpb.KeyValuePair{
		{Key: BackupFlag, Value: "false"},
		{Key: L0Import, Value: "true"},
		{Key: SkipDQC, Value: "true"},
	}
	assert.True(t, SkipDiskQuotaCheck(options))

	// backup = true, l0_import = false, skip_disk_quota_check = true
	options = []*commonpb.KeyValuePair{
		{Key: BackupFlag, Value: "true"},
		{Key: L0Import, Value: "false"},
		{Key: SkipDQC, Value: "true"},
	}
	assert.True(t, SkipDiskQuotaCheck(options))

	// backup = false, l0_import = false, skip_disk_quota_check = true
	options = []*commonpb.KeyValuePair{
		{Key: BackupFlag, Value: "false"},
		{Key: L0Import, Value: "false"},
		{Key: SkipDQC, Value: "true"},
	}
	assert.False(t, SkipDiskQuotaCheck(options))

	// backup = true, l0_import = true, skip_disk_quota_check = true
	options = []*commonpb.KeyValuePair{
		{Key: BackupFlag, Value: "true"},
		{Key: L0Import, Value: "true"},
		{Key: SkipDQC, Value: "true"},
	}
	assert.True(t, SkipDiskQuotaCheck(options))
}

func TestOption_GetCSVSep(t *testing.T) {
	options := []*commonpb.KeyValuePair{}
	r, err := GetCSVSep(options)
	assert.NoError(t, err)
	assert.Equal(t, ',', r)

	options = []*commonpb.KeyValuePair{
		{Key: CSVSep, Value: "|"},
	}
	r, err = GetCSVSep(options)
	assert.NoError(t, err)
	assert.Equal(t, '|', r)

	unsupportedSep := []rune{0, '\n', '\r', '"', 0xFFFD}
	for _, sep := range unsupportedSep {
		options = []*commonpb.KeyValuePair{
			{Key: CSVSep, Value: string(sep)},
		}
		_, err = GetCSVSep(options)
		assert.Error(t, err)
	}
}

func TestOption_GetCSVNullKey(t *testing.T) {
	options := []*commonpb.KeyValuePair{}
	nullKey, err := GetCSVNullKey(options)
	assert.NoError(t, err)
	assert.Equal(t, "", nullKey)

	options = []*commonpb.KeyValuePair{
		{Key: CSVNullKey, Value: "ABC"},
	}
	nullKey, err = GetCSVNullKey(options)
	assert.NoError(t, err)
	assert.Equal(t, "ABC", nullKey)
}

func TestOption_GetStorageVersion(t *testing.T) {
	// Test case 1: No storage_version option set, should return StorageV1 by default
	options := []*commonpb.KeyValuePair{}
	version, err := GetStorageVersion(options)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), version) // StorageV1 = 0

	// Test case 2: storage_version set to "2", should return StorageV2
	options = []*commonpb.KeyValuePair{
		{Key: StorageVersion, Value: "2"},
	}
	version, err = GetStorageVersion(options)
	assert.NoError(t, err)
	assert.Equal(t, int64(2), version) // StorageV2 = 2
}

func TestSimple(t *testing.T) {
	// Simple test to verify the test environment works
	assert.Equal(t, 1, 1)
	assert.Equal(t, "test", "test")
}

func TestIsAutoCommit(t *testing.T) {
	// default true when key absent
	assert.True(t, IsAutoCommit(nil))
	assert.True(t, IsAutoCommit([]*commonpb.KeyValuePair{}))

	// explicit true
	opts := []*commonpb.KeyValuePair{{Key: AutoCommitKey, Value: "true"}}
	assert.True(t, IsAutoCommit(opts))

	// explicit false
	opts = []*commonpb.KeyValuePair{{Key: AutoCommitKey, Value: "false"}}
	assert.False(t, IsAutoCommit(opts))
}

func TestValidateNoDuplicateKeys(t *testing.T) {
	assert.NoError(t, ValidateNoDuplicateKeys(nil))
	assert.NoError(t, ValidateNoDuplicateKeys(Options{
		{Key: "backup", Value: "true"},
		{Key: "l0_import", Value: "false"},
	}))

	err := ValidateNoDuplicateKeys(Options{
		{Key: "backup", Value: "false"},
		{Key: "backup", Value: "true"},
	})
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Contains(t, err.Error(), "backup")

	// ParseTimeRange matches keys with strings.EqualFold, so case variants of one
	// target key are duplicates too: accepting them leaves getTimestamp to pick a
	// value by map iteration order.
	err = ValidateNoDuplicateKeys(Options{
		{Key: StartTs, Value: "1"},
		{Key: "START_TS", Value: "2"},
	})
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	assert.Contains(t, err.Error(), "START_TS")

	// U+017F folds to "s" but is unchanged by strings.ToLower, so a ToLower-based
	// dedup would admit this pair while getTimestamp still matches both.
	err = ValidateNoDuplicateKeys(Options{
		{Key: StartTs, Value: "1"},
		{Key: "ſtart_ts", Value: "2"},
	})
	assert.ErrorIs(t, err, merr.ErrParameterInvalid)

	// The start_ts/startTs alias pair differs in length, so EqualFold never
	// matches them against each other and both remain legal in one request.
	assert.NoError(t, ValidateNoDuplicateKeys(Options{
		{Key: StartTs, Value: "1"},
		{Key: StartTs2, Value: "2"},
	}))
}

// Nothing bounds how many options an ImportV2 request carries -- the only ceiling
// is the 256 MiB gRPC body, and 50k keys fit in under 1 MB. This check runs first
// in proxy PreExecute, ahead of even the collection lookup, on the task pool shared
// with insert and delete, and again in datacoord.
//
// Measured under the -N -l build these tests use: linear ~30ms, pairwise ~28s. The
// one-second bound therefore leaves ~30x headroom for a slow CI box while still
// sitting ~28x below what a reintroduced quadratic scan costs.
func TestValidateNoDuplicateKeys_ScalesLinearly(t *testing.T) {
	const n = 50000
	opts := make(Options, 0, n)
	for i := 0; i < n; i++ {
		opts = append(opts, &commonpb.KeyValuePair{Key: fmt.Sprintf("k%08d", i), Value: "v"})
	}

	start := time.Now()
	assert.NoError(t, ValidateNoDuplicateKeys(opts))
	elapsed := time.Since(start)

	assert.Less(t, elapsed, time.Second,
		"validating %d options took %s -- a pairwise scan is back", n, elapsed)
}
