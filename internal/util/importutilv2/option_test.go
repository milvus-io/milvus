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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func TestSnapshotExternalSourceOptions(t *testing.T) {
	base := Options{{Key: BackupFlag, Value: "true"}, {Key: SourceType, Value: SourceTypeSnapshot}}
	spec := &commonpb.KeyValuePair{Key: ExternalSpec, Value: `{"extfs":{"access_key_id":"reader","access_key_value":"secret"}}`}
	assert.NoError(t, ValidateSnapshotSourceRequest(append(base, spec)))
	for _, options := range []Options{
		{spec},
		{{Key: BackupFlag, Value: "true"}, spec},
		append(base, &commonpb.KeyValuePair{Key: ExternalSpec}),
		append(base, &commonpb.KeyValuePair{Key: ExternalSpec, Value: strings.Repeat("x", 64*1024+1)}),
		append(base, spec, spec),
		append(base, &commonpb.KeyValuePair{Key: SnapshotSourceURI, Value: "s3://source/key"}),
	} {
		assert.Error(t, ValidateSnapshotSourceRequest(options))
	}
	internal := append(base, spec, &commonpb.KeyValuePair{Key: SnapshotSourceURI, Value: "s3://source/key"})
	assert.NoError(t, ValidateSnapshotSourceOptions(internal))
	assert.Error(t, ValidateSnapshotSourceOptions(append(base, &commonpb.KeyValuePair{Key: SnapshotSourceURI, Value: "s3://source/key"})))
	assert.Error(t, ValidateSnapshotSourceOptions(Options{{Key: BackupFlag, Value: "true"}, {Key: StorageVersion, Value: "invalid"}}))
	assert.Error(t, ValidateSnapshotSourceRequest(internal))
	redacted := RedactOptions(append(internal, nil, &commonpb.KeyValuePair{Key: EZK, Value: "key-secret"}))
	assert.NotContains(t, fmt.Sprint(redacted), "secret")
	assert.Contains(t, spec.Value, "secret", "redaction must not change live options")
}

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

func TestOption_ValidateSnapshotSourceOptions(t *testing.T) {
	valid := Options{
		{Key: BackupFlag, Value: "true"},
		{Key: SourceType, Value: SourceTypeSnapshot},
	}
	assert.NoError(t, ValidateSnapshotSourceOptions(valid))
	assert.True(t, IsSnapshotSource(valid))
	// Public requests cannot silently turn a former partition selection into
	// a whole-snapshot import. Persisted tasks already have their file scope.
	oldOptions := append(append(Options(nil), valid...), &commonpb.KeyValuePair{Key: "source_partition_name", Value: "p1"})
	assert.ErrorIs(t, ValidateSnapshotSourceRequest(oldOptions), merr.ErrImportFailed)
	assert.NoError(t, ValidateSnapshotSourceOptions(oldOptions))
	assert.NoError(t, ValidateSnapshotSourceOptions(Options{
		{Key: BackupFlag, Value: "true"},
		{Key: SourceType, Value: SourceTypeSnapshot},
		{Key: EZK, Value: "encoded-key"},
	}))
	assert.NoError(t, ValidateSnapshotSourceOptions(Options{
		{Key: BackupFlag, Value: "true"},
		{Key: StorageVersion, Value: "2"},
	}))

	tests := []struct {
		name    string
		options Options
		err     error
	}{
		{
			name: "legacy StorageV3 backup source",
			options: Options{
				{Key: BackupFlag, Value: "true"},
				{Key: StorageVersion, Value: "3"},
			},
			err: merr.ErrImportFailed,
		},
		{
			name: "unknown source type",
			options: Options{
				{Key: BackupFlag, Value: "true"},
				{Key: SourceType, Value: "directory"},
			},
			err: merr.ErrImportFailed,
		},
		{
			name: "duplicate contract option",
			options: Options{
				{Key: BackupFlag, Value: "true"},
				{Key: SourceType, Value: SourceTypeSnapshot},
				{Key: SourceType, Value: "directory"},
			},
			err: merr.ErrImportFailed,
		},
		{
			name: "snapshot source without backup",
			options: Options{
				{Key: SourceType, Value: SourceTypeSnapshot},
			},
			err: merr.ErrImportFailed,
		},
		{
			name: "snapshot source with l0 import",
			options: Options{
				{Key: BackupFlag, Value: "true"},
				{Key: SourceType, Value: SourceTypeSnapshot},
				{Key: L0Import, Value: "true"},
			},
			err: merr.ErrImportFailed,
		},
		{
			name: "snapshot source with storage version",
			options: Options{
				{Key: BackupFlag, Value: "true"},
				{Key: SourceType, Value: SourceTypeSnapshot},
				{Key: StorageVersion, Value: "3"},
			},
			err: merr.ErrImportFailed,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := ValidateSnapshotSourceOptions(test.options)
			assert.ErrorIs(t, err, test.err)
		})
	}
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
