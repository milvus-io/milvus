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
	"strconv"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

const (
	// Timeout specifies the timeout duration for import, such as "300s", "1.5h" or "1h45m".
	Timeout = "timeout"

	// SkipDQC indicates whether to bypass the disk quota check, default to false.
	SkipDQC = "skip_disk_quota_check"

	// CSVSep specifies the delimiter used for importing CSV files.
	CSVSep = "sep"

	// CSVNullKey specifies the null key used when importing CSV files.
	CSVNullKey = "nullkey"
)

// Options for backup-restore mode.
const (
	// BackupFlag indicates whether the import is in backup-restore mode, default to false.
	BackupFlag = "backup"

	// L0Import indicates whether to import l0 segments only.
	L0Import = "l0_import"

	// StartTs StartTs2 EndTs EndTs2 are used to filter data during backup-restore import.
	StartTs  = "start_ts"
	StartTs2 = "startTs"
	EndTs    = "end_ts"
	EndTs2   = "endTs"
)

type Options []*commonpb.KeyValuePair

func GetTimeoutTs(options Options) (uint64, error) {
	var timeoutTs uint64 = math.MaxUint64
	timeoutStr, err := funcutil.GetAttrByKeyFromRepeatedKV(Timeout, options)
	if err == nil {
		var dur time.Duration
		dur, err = time.ParseDuration(timeoutStr)
		if err != nil {
			return 0, fmt.Errorf("parse timeout failed, err=%w", err)
		}
		curTs := tsoutil.GetCurrentTime()
		timeoutTs = tsoutil.AddPhysicalDurationOnTs(curTs, dur)
	}
	return timeoutTs, nil
}

func ParseTimeRange(options Options) (uint64, uint64, error) {
	importOptions := funcutil.KeyValuePair2Map(options)
	getTimestamp := func(defaultValue uint64, targetKeys ...string) (uint64, error) {
		for _, targetKey := range targetKeys {
			for key, value := range importOptions {
				if strings.EqualFold(key, targetKey) {
					ts, err := strconv.ParseUint(value, 10, 64)
					if err != nil {
						return 0, merr.WrapErrImportFailed(fmt.Sprintf("parse %s failed, value=%s, err=%s", targetKey, value, err))
					}
					if !tsoutil.IsValidHybridTs(ts) {
						return 0, merr.WrapErrImportFailed(fmt.Sprintf("%s is not a valid hybrid timestamp, value=%s", targetKey, value))
					}
					return ts, nil
				}
			}
		}
		return defaultValue, nil
	}
	tsStart, err := getTimestamp(0, StartTs, StartTs2)
	if err != nil {
		return 0, 0, err
	}
	tsEnd, err := getTimestamp(math.MaxUint64, EndTs, EndTs2)
	if err != nil {
		return 0, 0, err
	}
	if tsStart > tsEnd {
		return 0, 0, merr.WrapErrImportFailed(
			fmt.Sprintf("start_ts shouldn't be larger than end_ts, start_ts:%d, end_ts:%d", tsStart, tsEnd))
	}
	return tsStart, tsEnd, nil
}

func IsBackup(options Options) bool {
	isBackup, err := funcutil.GetAttrByKeyFromRepeatedKV(BackupFlag, options)
	if err != nil || strings.ToLower(isBackup) != "true" {
		return false
	}
	return true
}

func IsL0Import(options Options) bool {
	isL0Import, err := funcutil.GetAttrByKeyFromRepeatedKV(L0Import, options)
	if err != nil || strings.ToLower(isL0Import) != "true" {
		return false
	}
	return true
}

// SkipDiskQuotaCheck indicates whether the import skips the disk quota check.
// This option should only be enabled during backup restoration.
func SkipDiskQuotaCheck(options Options) bool {
	if !IsBackup(options) {
		return false
	}
	skip, err := funcutil.GetAttrByKeyFromRepeatedKV(SkipDQC, options)
	if err != nil || strings.ToLower(skip) != "true" {
		return false
	}
	return true
}

func GetCSVSep(options Options) (rune, error) {
	sep, err := funcutil.GetAttrByKeyFromRepeatedKV(CSVSep, options)
	unsupportedSep := []rune{0, '\n', '\r', '"', 0xFFFD}
	defaultSep := ','
	if err != nil || len(sep) == 0 {
		return defaultSep, nil
	} else if lo.Contains(unsupportedSep, []rune(sep)[0]) {
		return 0, merr.WrapErrImportFailed(fmt.Sprintf("unsupported csv separator: %s", sep))
	}
	return []rune(sep)[0], nil
}

func GetCSVNullKey(options Options) (string, error) {
	nullKey, err := funcutil.GetAttrByKeyFromRepeatedKV(CSVNullKey, options)
	defaultNullKey := ""
	if err != nil || len(nullKey) == 0 {
		return defaultNullKey, nil
	}
	return nullKey, nil
}
