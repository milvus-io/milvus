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

package datacoord

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/samber/lo"
	"golang.org/x/time/rate"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/importutilv2"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

func getZeroTime() time.Time {
	var t time.Time
	return t
}

func parseBuildIDFromFilePath(key string) (UniqueID, error) {
	ss := strings.Split(key, "/")
	if strings.HasSuffix(key, "/") {
		return strconv.ParseInt(ss[len(ss)-2], 10, 64)
	}
	return strconv.ParseInt(ss[len(ss)-1], 10, 64)
}

func getFieldBinlogs(id UniqueID, binlogs []*datapb.FieldBinlog) *datapb.FieldBinlog {
	for _, binlog := range binlogs {
		if id == binlog.GetFieldID() {
			return binlog
		}
	}
	return nil
}

func mergeFieldBinlogs(currentBinlogs []*datapb.FieldBinlog, newBinlogs []*datapb.FieldBinlog) []*datapb.FieldBinlog {
	for _, newBinlog := range newBinlogs {
		fieldBinlogs := getFieldBinlogs(newBinlog.GetFieldID(), currentBinlogs)
		if fieldBinlogs == nil {
			currentBinlogs = append(currentBinlogs, newBinlog)
		} else {
			if len(fieldBinlogs.ChildFields) == 0 {
				fieldBinlogs.ChildFields = newBinlog.GetChildFields()
			}
			if fieldBinlogs.Format == "" {
				fieldBinlogs.Format = newBinlog.GetFormat()
			}
			fieldBinlogs.Binlogs = append(fieldBinlogs.Binlogs, newBinlog.Binlogs...)
		}
	}
	return currentBinlogs
}

// filterDuplicateFieldBinlogs removes FieldBinlog entries from newLogs whose (fieldID, logID)
// pairs already exist in existingLogs. Used to make crash-replay idempotent when the same
// set of binlog results may be applied twice (e.g. backfill task completion after a datacoord
// restart between the etcd write and the task state transition).
func filterDuplicateFieldBinlogs(existingLogs, newLogs []*datapb.FieldBinlog) []*datapb.FieldBinlog {
	if len(existingLogs) == 0 || len(newLogs) == 0 {
		return newLogs
	}
	existing := make(map[int64]map[int64]struct{}, len(existingLogs))
	for _, fb := range existingLogs {
		logIDs, ok := existing[fb.GetFieldID()]
		if !ok {
			logIDs = make(map[int64]struct{})
			existing[fb.GetFieldID()] = logIDs
		}
		for _, b := range fb.GetBinlogs() {
			logIDs[b.GetLogID()] = struct{}{}
		}
	}
	result := make([]*datapb.FieldBinlog, 0, len(newLogs))
	for _, fb := range newLogs {
		existingSet, hasField := existing[fb.GetFieldID()]
		if !hasField {
			result = append(result, fb)
			continue
		}
		filteredBinlogs := make([]*datapb.Binlog, 0, len(fb.GetBinlogs()))
		for _, b := range fb.GetBinlogs() {
			if _, dup := existingSet[b.GetLogID()]; !dup {
				filteredBinlogs = append(filteredBinlogs, b)
			}
		}
		if len(filteredBinlogs) > 0 {
			result = append(result, &datapb.FieldBinlog{
				FieldID:     fb.GetFieldID(),
				ChildFields: fb.GetChildFields(),
				Format:      fb.GetFormat(),
				Binlogs:     filteredBinlogs,
			})
		}
	}
	return result
}

func getBinLogIDs(segment *SegmentInfo, fieldID int64) []int64 {
	binlogIDs := make([]int64, 0)
	for _, fieldBinLog := range segment.GetBinlogs() {
		if fieldBinLog.GetFieldID() == fieldID {
			for _, binLog := range fieldBinLog.GetBinlogs() {
				binlogIDs = append(binlogIDs, binLog.GetLogID())
			}
			break
		}
	}
	return binlogIDs
}

func getTotalBinlogRows(segment *SegmentInfo, fieldID int64) int64 {
	var total int64
	for _, fieldBinLog := range segment.GetBinlogs() {
		if fieldBinLog.GetFieldID() == fieldID {
			for _, binLog := range fieldBinLog.GetBinlogs() {
				total += binLog.EntriesNum
			}
		}
	}
	return total
}

func CheckCheckPointsHealth(meta *meta) error {
	for channel, cp := range meta.GetChannelCheckpoints() {
		collectionID := funcutil.GetCollectionIDFromVChannel(channel)
		if collectionID == -1 {
			mlog.RatedWarn(context.TODO(), rate.Limit(60), "can't parse collection id from vchannel, skip check cp lag", mlog.FieldVChannel(channel))
			continue
		}
		if meta.GetCollection(collectionID) == nil {
			mlog.RatedWarn(context.TODO(), rate.Limit(60), "corresponding the collection doesn't exists, skip check cp lag", mlog.FieldVChannel(channel))
			continue
		}
		ts, _ := tsoutil.ParseTS(cp.Timestamp)
		lag := time.Since(ts)
		if lag > paramtable.Get().DataCoordCfg.ChannelCheckpointMaxLag.GetAsDuration(time.Second) {
			return merr.WrapErrChannelCPExceededMaxLag(channel, fmt.Sprintf("checkpoint lag: %f(min)", lag.Minutes()))
		}
	}
	return nil
}

func createStorageConfig() *indexpb.StorageConfig {
	var storageConfig *indexpb.StorageConfig

	if Params.CommonCfg.StorageType.GetValue() == "local" {
		storageConfig = &indexpb.StorageConfig{
			RootPath:    Params.LocalStorageCfg.Path.GetValue(),
			StorageType: Params.CommonCfg.StorageType.GetValue(),
			// External collections may reference an s3:// source even when the
			// primary storage is local, so the connection cap still applies.
			MaxConnections:              uint32(Params.MinioCfg.MaxConnections.GetAsInt()),
			TalonMode:                   Params.CommonCfg.StorageTalonMode.GetAsUint32(),
			TalonSmallReadThreshold:     Params.CommonCfg.StorageTalonSmallReadThreshold.GetAsUint32(),
			TalonCoordinator:            Params.CommonCfg.StorageTalonCoordinator.GetValue(),
			TalonBlockSize:              Params.CommonCfg.StorageTalonBlockSize.GetAsUint32(),
			TalonMaxIdlePerAddr:         Params.CommonCfg.StorageTalonMaxIdlePerAddr.GetAsUint32(),
			TalonEnableForExternalTable: Params.CommonCfg.StorageTalonEnableForExternalTable.GetAsBool(),
		}
	} else {
		storageConfig = &indexpb.StorageConfig{
			Address:                     Params.MinioCfg.Address.GetValue(),
			AccessKeyID:                 Params.MinioCfg.AccessKeyID.GetValue(),
			SecretAccessKey:             Params.MinioCfg.SecretAccessKey.GetValue(),
			UseSSL:                      Params.MinioCfg.UseSSL.GetAsBool(),
			SslCACert:                   Params.MinioCfg.SslCACert.GetValue(),
			BucketName:                  Params.MinioCfg.BucketName.GetValue(),
			RootPath:                    Params.MinioCfg.RootPath.GetValue(),
			UseIAM:                      Params.MinioCfg.UseIAM.GetAsBool(),
			IAMEndpoint:                 Params.MinioCfg.IAMEndpoint.GetValue(),
			StorageType:                 Params.CommonCfg.StorageType.GetValue(),
			Region:                      Params.MinioCfg.Region.GetValue(),
			UseVirtualHost:              Params.MinioCfg.UseVirtualHost.GetAsBool(),
			CloudProvider:               Params.MinioCfg.CloudProvider.GetValue(),
			RequestTimeoutMs:            Params.MinioCfg.RequestTimeoutMs.GetAsInt64(),
			MaxConnections:              uint32(Params.MinioCfg.MaxConnections.GetAsInt()),
			GcpCredentialJSON:           Params.MinioCfg.GcpCredentialJSON.GetValue(),
			SslTlsMinVersion:            Params.MinioCfg.SslTLSMinVersion.GetValue(),
			UseCrc32CChecksum:           Params.MinioCfg.UseCRC32C.GetAsBool(),
			TalonMode:                   Params.CommonCfg.StorageTalonMode.GetAsUint32(),
			TalonSmallReadThreshold:     Params.CommonCfg.StorageTalonSmallReadThreshold.GetAsUint32(),
			TalonCoordinator:            Params.CommonCfg.StorageTalonCoordinator.GetValue(),
			TalonBlockSize:              Params.CommonCfg.StorageTalonBlockSize.GetAsUint32(),
			TalonMaxIdlePerAddr:         Params.CommonCfg.StorageTalonMaxIdlePerAddr.GetAsUint32(),
			TalonEnableForExternalTable: Params.CommonCfg.StorageTalonEnableForExternalTable.GetAsBool(),
		}
	}

	return storageConfig
}

func saturatingAdd(values ...int64) int64 {
	var result int64
	for _, value := range values {
		if value > math.MaxInt64-result {
			return math.MaxInt64
		}
		result += value
	}
	return result
}

func saturatingMul(left, right int64) int64 {
	if left <= 0 || right <= 0 {
		return 0
	}
	if left > math.MaxInt64/right {
		return math.MaxInt64
	}
	return left * right
}

func ceilDiv(value, divisor int64) int64 {
	if value <= 0 {
		return 0
	}
	return 1 + (value-1)/divisor
}

func enableSortCompaction() bool {
	return paramtable.Get().DataCoordCfg.EnableSortCompaction.GetAsBool() && paramtable.Get().DataCoordCfg.EnableCompaction.GetAsBool()
}

// stringifyBinlogs is used for logging, it's not used for other purposes.
func stringifyBinlogs(binlogs []*datapb.FieldBinlog) []string {
	strs := make([]string, 0, len(binlogs))
	byIDs := lo.GroupBy(binlogs, func(binlog *datapb.FieldBinlog) int64 {
		return binlog.GetFieldID()
	})
	for _, binlogs := range byIDs {
		fieldsStrs := make([]string, 0, len(binlogs))
		for _, binlog := range binlogs {
			for _, b := range binlog.GetBinlogs() {
				fieldsStrs = append(fieldsStrs,
					fmt.Sprintf("l%d(e%d,m%d,t%d-%d)", b.LogID, b.EntriesNum, b.MemorySize, b.TimestampFrom, b.TimestampTo),
				)
			}
		}
		strs = append(strs, fmt.Sprintf("f%d:%s", binlogs[0].GetFieldID(), strings.Join(fieldsStrs, "|")))
	}
	return strs
}

func isFlushed(segment *SegmentInfo) bool {
	return segment.GetState() == commonpb.SegmentState_Flushed
}

func isFlush(segment *SegmentInfo) bool {
	return segment.GetState() == commonpb.SegmentState_Flushed || segment.GetState() == commonpb.SegmentState_Flushing
}

// chanPartSegments is an internal result struct, which is aggregates of SegmentInfos with same collectionID, partitionID and channelName
type chanPartSegments struct {
	collectionID UniqueID
	partitionID  UniqueID
	channelName  string
	segments     []*SegmentInfo
}

// Return None nil slice
func GetReadPluginContext(options importutilv2.Options) []*commonpb.KeyValuePair {
	importEzk, _ := importutilv2.GetEZK(options)
	return hookutil.GetReadStoragePluginContext(importEzk)
}

func WrapPluginContext(collectionID int64, properties []*commonpb.KeyValuePair, msg proto.Message) {
	pluginContext := hookutil.GetStoragePluginContext(properties, collectionID)
	if pluginContext == nil {
		return
	}

	switch msg := msg.(type) {
	case *datapb.CompactionPlan:
		plan := msg
		plan.PluginContext = append(plan.PluginContext, pluginContext...)
	case *workerpb.CreateJobRequest:
		job := msg
		job.PluginContext = append(job.PluginContext, pluginContext...)
	case *workerpb.AnalyzeRequest:
		job := msg
		job.PluginContext = append(job.PluginContext, pluginContext...)
	case *workerpb.CreateStatsRequest:
		job := msg
		job.PluginContext = append(job.PluginContext, pluginContext...)
	case *datapb.ImportRequest:
		job := msg
		job.PluginContext = append(job.PluginContext, pluginContext...)
	case *datapb.PreImportRequest:
		job := msg
		job.PluginContext = append(job.PluginContext, pluginContext...)
	case *datapb.ReshardTaskRequest:
		job := msg
		job.PluginContext = append(job.PluginContext, pluginContext...)
	case *datapb.ImportTaskV3Request:
		job := msg
		job.PluginContext = append(job.PluginContext, pluginContext...)
	default:
		return
	}
}

func buildL0V3DeltaLogEntries(segmentID int64, deltalogs []*datapb.FieldBinlog) ([]packed.DeltaLogEntry, error) {
	entries := make([]packed.DeltaLogEntry, 0)
	for _, fieldBinlog := range deltalogs {
		for _, binlog := range fieldBinlog.GetBinlogs() {
			path := binlog.GetLogPath()
			if path == "" {
				return nil, merr.WrapErrServiceInternalMsg("L0 V3 compaction result missing deltalog path for segment %d, logID %d", segmentID, binlog.GetLogID())
			}
			entries = append(entries, packed.DeltaLogEntry{
				Path:       path,
				NumEntries: binlog.GetEntriesNum(),
			})
		}
	}
	return entries, nil
}

// Deprecated: use toMsgPositionWithWALNames
func toMsgPosition(channel string, startPositions []*commonpb.KeyDataPair) *msgpb.MsgPosition {
	for _, sp := range startPositions {
		if sp.GetKey() != funcutil.ToPhysicalChannel(channel) {
			continue
		}
		return &msgpb.MsgPosition{
			ChannelName: channel,
			MsgID:       sp.GetData(),
		}
	}
	return nil
}

func toMsgPositionWithWALNames(channel string, startPositions []*commonpb.KeyDataPair, channelWALNames map[string]commonpb.WALName) *msgpb.MsgPosition {
	for _, sp := range startPositions {
		pChannel := funcutil.ToPhysicalChannel(channel)
		if sp.GetKey() != pChannel {
			continue
		}
		return &msgpb.MsgPosition{
			ChannelName: channel,
			MsgID:       sp.GetData(),
			WALName:     channelWALNames[pChannel],
		}
	}
	return nil
}

func isFlushState(state commonpb.SegmentState) bool {
	return state == commonpb.SegmentState_Flushing || state == commonpb.SegmentState_Flushed
}

func GetIndexType(indexParams []*commonpb.KeyValuePair) string {
	for _, param := range indexParams {
		if param.Key == common.IndexTypeKey {
			return param.Value
		}
	}
	return invalidIndex
}

const (
	NullNodeID = -1
)

func needDoTextIndex(segment *SegmentInfo, fieldIDs []UniqueID, allowUnsorted bool) bool {
	if !isFlush(segment) || segment.GetLevel() == datapb.SegmentLevel_L0 {
		return false
	}
	if !allowUnsorted && !segment.GetIsSorted() && !segment.GetIsSortedByNamespace() {
		return false
	}

	for _, fieldID := range fieldIDs {
		if segment.GetTextStatsLogs() == nil {
			return true
		}
		if segment.GetTextStatsLogs()[fieldID] == nil {
			return true
		}
	}
	return false
}

func needDoJSONKeyIndex(segment *SegmentInfo, fieldIDs []UniqueID, allowUnsorted bool) bool {
	if !isFlush(segment) || segment.GetLevel() == datapb.SegmentLevel_L0 {
		return false
	}
	if !allowUnsorted && !segment.GetIsSorted() && !segment.GetIsSortedByNamespace() {
		return false
	}

	for _, fieldID := range fieldIDs {
		if segment.GetJsonKeyStats() == nil {
			return true
		}
		if segment.GetJsonKeyStats()[fieldID] == nil {
			return true
		}
		// if the data format version is less than the current version, we need to do the stats task again
		// because the data format is updated, the old data format need to be converted to the new data format
		if segment.GetJsonKeyStats()[fieldID].GetJsonKeyStatsDataFormat() < common.JSONStatsDataFormatVersion {
			return true
		}
	}
	return false
}

func canBuildExternalJSONKeyIndex(segment *SegmentInfo) bool {
	return segment.GetStorageVersion() == storage.StorageV3 && segment.GetManifestPath() != ""
}
