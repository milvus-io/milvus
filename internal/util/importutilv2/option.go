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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	storage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
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

// AutoCommitKey is the option key for enabling/disabling auto-commit of import jobs.
const AutoCommitKey = "auto_commit"

// Options for backup-restore mode.
const (
	// BackupFlag indicates whether the import is in backup-restore mode, default to false.
	BackupFlag = "backup"

	// SourceType selects a metadata source contract for backup import.
	SourceType = "source_type"

	// SourceTypeSnapshot makes ImportFile.paths[0] an exact snapshot metadata path.
	SourceTypeSnapshot = "snapshot"

	// ExternalSpec supplies request-scoped source credentials for snapshot import.
	ExternalSpec = "external_spec"

	// SnapshotSourceURI is coordinator-owned: expanded manifests contain object
	// keys, so retries need the original storage identity without rereading metadata.
	SnapshotSourceURI = "_snapshot_source_uri"

	// L0Import indicates whether to import l0 segments only.
	L0Import = "l0_import"

	// StorageVersion indicates the storage version to use for import.
	// Type: int64
	// storage v2: 2
	// storage v1: others or not set
	StorageVersion = "storage_version"

	// StartTs StartTs2 EndTs EndTs2 are used to filter data during backup-restore import.
	StartTs  = "start_ts"
	StartTs2 = "startTs"
	EndTs    = "end_ts"
	EndTs2   = "endTs"

	// EZK is the base64-encoded encryption zone key for reading encrypted backup data.
	EZK = "ezk"
)

type Options []*commonpb.KeyValuePair

func GetTimeoutTs(options Options) (uint64, error) {
	var timeoutTs uint64 = math.MaxUint64
	timeoutStr, err := funcutil.GetAttrByKeyFromRepeatedKV(Timeout, options)
	if err == nil {
		var dur time.Duration
		dur, err = time.ParseDuration(timeoutStr)
		if err != nil {
			return 0, merr.Wrap(err, "parse timeout failed")
		}
		curTs := tsoutil.ComposeTSByTime(time.Now())
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
						return 0, merr.WrapErrImportFailedMsg("parse %s failed, value=%s, err=%s", targetKey, value, err)
					}
					if !tsoutil.IsValidHybridTs(ts) {
						return 0, merr.WrapErrImportFailedMsg("%s is not a valid hybrid timestamp, value=%s", targetKey, value)
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

// IsSnapshotSource reports whether the request uses snapshot metadata as its
// backup source. The option is intentionally opt-in so StorageV1 and StorageV2
// path-based backup imports retain their existing behavior.
func IsSnapshotSource(options Options) bool {
	sourceType, err := funcutil.GetAttrByKeyFromRepeatedKV(SourceType, options)
	return err == nil && strings.EqualFold(strings.TrimSpace(sourceType), SourceTypeSnapshot)
}

// ValidateSnapshotSourceOptions rejects combinations that would make the
// source contract ambiguous. Snapshot metadata is the sole authority for the
// source storage version. L0 import remains a separate contract; CMEK content
// is validated later against the source schema and supplied EZK.
func ValidateSnapshotSourceOptions(options Options) error {
	contractKeys := []string{SourceType, BackupFlag, L0Import, StorageVersion, EZK, ExternalSpec, SnapshotSourceURI}
	counts := make(map[string]int, len(contractKeys))
	hasSnapshotContractOption := false
	for _, option := range options {
		if option == nil {
			continue
		}
		if option.GetKey() == SourceType || option.GetKey() == ExternalSpec || option.GetKey() == SnapshotSourceURI {
			hasSnapshotContractOption = true
		}
		if lo.Contains(contractKeys, option.GetKey()) {
			counts[option.GetKey()]++
		}
	}
	if hasSnapshotContractOption {
		for _, key := range contractKeys {
			if counts[key] > 1 {
				return merr.WrapErrImportFailedMsg("duplicate snapshot-source option: %s", key)
			}
		}
	}

	sourceType, sourceTypeErr := funcutil.GetAttrByKeyFromRepeatedKV(SourceType, options)
	if value, err := funcutil.GetAttrByKeyFromRepeatedKV(ExternalSpec, options); err == nil {
		if !IsSnapshotSource(options) {
			return merr.WrapErrImportFailedMsg("external_spec requires source_type=snapshot")
		}
		if strings.TrimSpace(value) == "" || len(value) > 64*1024 {
			return merr.WrapErrImportFailedMsg("external_spec must be nonempty and at most 64 KiB")
		}
	}
	if _, err := funcutil.GetAttrByKeyFromRepeatedKV(SnapshotSourceURI, options); err == nil && !HasExternalSource(options) {
		return merr.WrapErrImportFailedMsg("snapshot source URI requires external_spec")
	}
	if sourceTypeErr != nil {
		if IsBackup(options) {
			storageVersion, err := GetStorageVersion(options)
			if err != nil {
				return err
			}
			if storageVersion == storage.StorageV3 {
				return merr.WrapErrImportFailedMsg(
					"StorageV3 backup import requires %s=%s and must not specify %s",
					SourceType,
					SourceTypeSnapshot,
					StorageVersion,
				)
			}
		}
		return nil
	}
	if !strings.EqualFold(strings.TrimSpace(sourceType), SourceTypeSnapshot) {
		return merr.WrapErrImportFailedMsg("unsupported %s: %s", SourceType, sourceType)
	}
	if !IsBackup(options) {
		return merr.WrapErrImportFailedMsg("%s=%s requires %s=true",
			SourceType, SourceTypeSnapshot, BackupFlag)
	}
	if IsL0Import(options) {
		return merr.WrapErrImportFailedMsg("%s=%s does not support %s=true",
			SourceType, SourceTypeSnapshot, L0Import)
	}
	if _, err := funcutil.GetAttrByKeyFromRepeatedKV(StorageVersion, options); err == nil {
		return merr.WrapErrImportFailedMsg("%s must not be specified with %s=%s",
			StorageVersion, SourceType, SourceTypeSnapshot)
	}
	return nil
}

func HasExternalSource(options Options) bool {
	_, err := funcutil.GetAttrByKeyFromRepeatedKV(ExternalSpec, options)
	return err == nil
}

// ValidateSnapshotSourceRequest additionally rejects internal routing options
// supplied by a public caller. Persisted tasks use ValidateSnapshotSourceOptions.
func ValidateSnapshotSourceRequest(options Options) error {
	// Do not silently broaden a request written for the earlier partition
	// selector. Already-expanded tasks still use ValidateSnapshotSourceOptions
	// and keep their immutable file inventory rather than re-expanding it.
	if _, err := funcutil.GetAttrByKeyFromRepeatedKV("source_partition_name", options); err == nil {
		return merr.WrapErrImportFailedMsg("source_partition_name is not supported; snapshot import reads all source partitions")
	}
	if _, err := funcutil.GetAttrByKeyFromRepeatedKV(SnapshotSourceURI, options); err == nil {
		return merr.WrapErrImportFailedMsg("%s is reserved for internal use", SnapshotSourceURI)
	}
	return ValidateSnapshotSourceOptions(options)
}

// RedactOptions returns a logging-only copy; never mutate credentials in live
// task options, which are also used by retries and the second import phase.
func RedactOptions(options Options) Options {
	result := make(Options, 0, len(options))
	for _, option := range options {
		if option == nil {
			continue
		}
		value := option.GetValue()
		if strings.EqualFold(option.GetKey(), ExternalSpec) || strings.EqualFold(option.GetKey(), EZK) {
			value = "<redacted>"
		}
		result = append(result, &commonpb.KeyValuePair{Key: option.GetKey(), Value: value})
	}
	return result
}

func IsL0Import(options Options) bool {
	isL0Import, err := funcutil.GetAttrByKeyFromRepeatedKV(L0Import, options)
	if err != nil || strings.ToLower(isL0Import) != "true" {
		return false
	}
	return true
}

func GetStorageVersion(options Options) (int64, error) {
	storageVersion, err := funcutil.GetAttrByKeyFromRepeatedKV(StorageVersion, options)
	if err != nil {
		// not set, use storage v1 by default
		return storage.StorageV1, nil
	}
	version, err := strconv.ParseInt(storageVersion, 10, 64)
	if err != nil {
		return 0, merr.WrapErrImportFailedMsg("parse storage_version failed, value=%s, err=%s", storageVersion, err)
	}
	switch version {
	case storage.StorageV2:
		return storage.StorageV2, nil
	case storage.StorageV3:
		return storage.StorageV3, nil
	case storage.StorageV1:
		fallthrough
	default:
		return storage.StorageV1, nil
	}
}

// SkipDiskQuotaCheck indicates whether the import skips the disk quota check.
// This option should only be enabled during backup restoration.
func SkipDiskQuotaCheck(options Options) bool {
	if !IsBackup(options) && !IsL0Import(options) {
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
		return 0, merr.WrapErrImportFailedMsg("unsupported csv separator: %s", sep)
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

func GetEZK(options Options) (string, error) {
	ezk, err := funcutil.GetAttrByKeyFromRepeatedKV(EZK, options)
	if err != nil || len(ezk) == 0 {
		return "", nil
	}
	return ezk, nil
}

// IsAutoCommit parses the auto_commit option. Defaults to true if absent.
func IsAutoCommit(options Options) bool {
	val, err := funcutil.GetAttrByKeyFromRepeatedKV(AutoCommitKey, options)
	if err != nil || strings.ToLower(val) != "false" {
		return true
	}
	return false
}
