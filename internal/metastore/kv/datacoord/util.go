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
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/segmentutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func ValidateSegment(segment *datapb.SegmentInfo) error {
	log := log.With(
		zap.Int64("collection", segment.GetCollectionID()),
		zap.Int64("partition", segment.GetPartitionID()),
		zap.Int64("segment", segment.GetID()))
	// check stats log and bin log size match

	// check L0 Segment
	if segment.GetLevel() == datapb.SegmentLevel_L0 {
		// L0 segment should only have delta logs
		if len(segment.GetBinlogs()) > 0 || len(segment.GetStatslogs()) > 0 {
			log.Warn("find invalid segment while L0 segment get more than delta logs",
				zap.Any("binlogs", segment.GetBinlogs()),
				zap.Any("stats", segment.GetBinlogs()),
			)
			return fmt.Errorf("segment can not be saved because of L0 segment get more than delta logs: collection %v, segment %v",
				segment.GetCollectionID(), segment.GetID())
		}
		return nil
	}
	// check L1 and Legacy Segment

	if len(segment.GetBinlogs()) == 0 && len(segment.GetStatslogs()) == 0 {
		return nil
	}

	if len(segment.GetBinlogs()) == 0 || len(segment.GetStatslogs()) == 0 {
		log.Warn("find segment binlog or statslog was empty",
			zap.Any("binlogs", segment.GetBinlogs()),
			zap.Any("stats", segment.GetBinlogs()),
		)
		return fmt.Errorf("segment can not be saved because of binlog file or stat log file lack: collection %v, segment %v",
			segment.GetCollectionID(), segment.GetID())
	}

	// if segment not merge status log(growing or new flushed by old version)
	// segment num of binlog should same with statslogs.
	binlogNum := len(segment.GetBinlogs()[0].GetBinlogs())
	statslogNum := len(segment.GetStatslogs()[0].GetBinlogs())

	if len(segment.GetCompactionFrom()) == 0 && statslogNum != binlogNum && !hasSpecialStatslog(segment) {
		log.Warn("find invalid segment while bin log size didn't match stat log size",
			zap.Any("binlogs", segment.GetBinlogs()),
			zap.Any("stats", segment.GetStatslogs()),
		)
		return fmt.Errorf("segment can not be saved because of binlog file not match stat log number: collection %v, segment %v",
			segment.GetCollectionID(), segment.GetID())
	}

	return nil
}

func hasSpecialStatslog(segment *datapb.SegmentInfo) bool {
	for _, statslog := range segment.GetStatslogs()[0].GetBinlogs() {
		logidx := fmt.Sprint(statslog.LogID)
		if logidx == storage.CompoundStatsType.LogIdx() {
			return true
		}
	}
	return false
}

func buildBinlogKvsWithLogID(collectionID, partitionID, segmentID typeutil.UniqueID,
	binlogs, deltalogs, statslogs []*datapb.FieldBinlog,
) (map[string]string, error) {
	// all the FieldBinlog will only have logid
	kvs, err := buildBinlogKvs(collectionID, partitionID, segmentID, binlogs, deltalogs, statslogs)
	if err != nil {
		return nil, err
	}

	return kvs, nil
}

func buildSegmentAndBinlogsKvs(segment *datapb.SegmentInfo) (map[string]string, error) {
	noBinlogsSegment, binlogs, deltalogs, statslogs := CloneSegmentWithExcludeBinlogs(segment)
	// `segment` is not mutated above. Also, `noBinlogsSegment` is a cloned version of `segment`.
	segmentutil.ReCalcRowCount(segment, noBinlogsSegment)

	// save binlogs separately
	kvs, err := buildBinlogKvsWithLogID(noBinlogsSegment.CollectionID, noBinlogsSegment.PartitionID, noBinlogsSegment.ID, binlogs, deltalogs, statslogs)
	if err != nil {
		return nil, err
	}

	// save segment info
	k, v, err := buildSegmentKv(noBinlogsSegment)
	if err != nil {
		return nil, err
	}
	kvs[k] = v

	return kvs, nil
}

func buildBinlogKeys(segment *datapb.SegmentInfo) []string {
	var keys []string
	// binlog
	for _, binlog := range segment.Binlogs {
		key := buildFieldBinlogPath(segment.CollectionID, segment.PartitionID, segment.ID, binlog.FieldID)
		keys = append(keys, key)
	}

	// deltalog
	for _, deltalog := range segment.Deltalogs {
		key := buildFieldDeltalogPath(segment.CollectionID, segment.PartitionID, segment.ID, deltalog.FieldID)
		keys = append(keys, key)
	}

	// statslog
	for _, statslog := range segment.Statslogs {
		key := buildFieldStatslogPath(segment.CollectionID, segment.PartitionID, segment.ID, statslog.FieldID)
		keys = append(keys, key)
	}
	return keys
}

func resetBinlogFields(segment *datapb.SegmentInfo) {
	segment.Binlogs = nil
	segment.Deltalogs = nil
	segment.Statslogs = nil
}

func cloneLogs(binlogs []*datapb.FieldBinlog) []*datapb.FieldBinlog {
	var res []*datapb.FieldBinlog
	for _, log := range binlogs {
		res = append(res, proto.Clone(log).(*datapb.FieldBinlog))
	}
	return res
}

func buildBinlogKvs(collectionID, partitionID, segmentID typeutil.UniqueID, binlogs, deltalogs, statslogs []*datapb.FieldBinlog) (map[string]string, error) {
	kv := make(map[string]string)

	checkLogID := func(fieldBinlog *datapb.FieldBinlog) error {
		for _, binlog := range fieldBinlog.GetBinlogs() {
			if binlog.GetLogID() == 0 {
				return fmt.Errorf("invalid log id, binlog:%v", binlog)
			}
			if binlog.GetLogPath() != "" {
				return fmt.Errorf("fieldBinlog no need to store logpath, binlog:%v", binlog)
			}
		}
		return nil
	}

	// binlog kv
	for _, binlog := range binlogs {
		if err := checkLogID(binlog); err != nil {
			return nil, err
		}
		binlogBytes, err := proto.Marshal(binlog)
		if err != nil {
			return nil, fmt.Errorf("marshal binlogs failed, collectionID:%d, segmentID:%d, fieldID:%d, error:%w", collectionID, segmentID, binlog.FieldID, err)
		}
		key := buildFieldBinlogPath(collectionID, partitionID, segmentID, binlog.FieldID)
		kv[key] = string(binlogBytes)
	}

	// deltalog
	for _, deltalog := range deltalogs {
		if err := checkLogID(deltalog); err != nil {
			return nil, err
		}
		binlogBytes, err := proto.Marshal(deltalog)
		if err != nil {
			return nil, fmt.Errorf("marshal deltalogs failed, collectionID:%d, segmentID:%d, fieldID:%d, error:%w", collectionID, segmentID, deltalog.FieldID, err)
		}
		key := buildFieldDeltalogPath(collectionID, partitionID, segmentID, deltalog.FieldID)
		kv[key] = string(binlogBytes)
	}

	// statslog
	for _, statslog := range statslogs {
		if err := checkLogID(statslog); err != nil {
			return nil, err
		}
		binlogBytes, err := proto.Marshal(statslog)
		if err != nil {
			return nil, fmt.Errorf("marshal statslogs failed, collectionID:%d, segmentID:%d, fieldID:%d, error:%w", collectionID, segmentID, statslog.FieldID, err)
		}
		key := buildFieldStatslogPath(collectionID, partitionID, segmentID, statslog.FieldID)
		kv[key] = string(binlogBytes)
	}

	return kv, nil
}

func CloneSegmentWithExcludeBinlogs(segment *datapb.SegmentInfo) (*datapb.SegmentInfo, []*datapb.FieldBinlog, []*datapb.FieldBinlog, []*datapb.FieldBinlog) {
	clonedSegment := proto.Clone(segment).(*datapb.SegmentInfo)
	binlogs := clonedSegment.Binlogs
	deltalogs := clonedSegment.Deltalogs
	statlogs := clonedSegment.Statslogs

	clonedSegment.Binlogs = nil
	clonedSegment.Deltalogs = nil
	clonedSegment.Statslogs = nil
	return clonedSegment, binlogs, deltalogs, statlogs
}

func marshalSegmentInfo(segment *datapb.SegmentInfo) (string, error) {
	segBytes, err := proto.Marshal(segment)
	if err != nil {
		return "", fmt.Errorf("failed to marshal segment: %d, err: %w", segment.ID, err)
	}

	return string(segBytes), nil
}

func buildSegmentKv(segment *datapb.SegmentInfo) (string, string, error) {
	segBytes, err := marshalSegmentInfo(segment)
	if err != nil {
		return "", "", err
	}
	key := buildSegmentPath(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
	return key, segBytes, nil
}

func buildCompactionTaskKV(task *datapb.CompactionTask) (string, string, error) {
	valueBytes, err := proto.Marshal(task)
	if err != nil {
		return "", "", fmt.Errorf("failed to marshal CompactionTask: %d/%d/%d, err: %w", task.TriggerID, task.PlanID, task.CollectionID, err)
	}
	key := buildCompactionTaskPath(task)
	return key, string(valueBytes), nil
}

func buildCompactionTaskPath(task *datapb.CompactionTask) string {
	return fmt.Sprintf("%s/%s/%d/%d", CompactionTaskPrefix, task.GetType(), task.TriggerID, task.PlanID)
}

func buildPartitionStatsInfoKv(info *datapb.PartitionStatsInfo) (string, string, error) {
	valueBytes, err := proto.Marshal(info)
	if err != nil {
		return "", "", fmt.Errorf("failed to marshal collection clustering compaction info: %d, err: %w", info.CollectionID, err)
	}
	key := buildPartitionStatsInfoPath(info)
	return key, string(valueBytes), nil
}

// buildPartitionStatsInfoPath
func buildPartitionStatsInfoPath(info *datapb.PartitionStatsInfo) string {
	return fmt.Sprintf("%s/%d/%d/%s/%d", PartitionStatsInfoPrefix, info.CollectionID, info.PartitionID, info.VChannel, info.Version)
}

func buildCurrentPartitionStatsVersionPath(collID, partID int64, channel string) string {
	return fmt.Sprintf("%s/%d/%d/%s", PartitionStatsCurrentVersionPrefix, collID, partID, channel)
}

// buildSegmentPath common logic mapping segment info to corresponding key in kv store
func buildSegmentPath(collectionID typeutil.UniqueID, partitionID typeutil.UniqueID, segmentID typeutil.UniqueID) string {
	return fmt.Sprintf("%s/%d/%d/%d", SegmentPrefix, collectionID, partitionID, segmentID)
}

func buildFieldBinlogPath(collectionID typeutil.UniqueID, partitionID typeutil.UniqueID, segmentID typeutil.UniqueID, fieldID typeutil.UniqueID) string {
	return fmt.Sprintf("%s/%d/%d/%d/%d", SegmentBinlogPathPrefix, collectionID, partitionID, segmentID, fieldID)
}

// TODO: There's no need to include fieldID in the delta log path key.
func buildFieldDeltalogPath(collectionID typeutil.UniqueID, partitionID typeutil.UniqueID, segmentID typeutil.UniqueID, fieldID typeutil.UniqueID) string {
	return fmt.Sprintf("%s/%d/%d/%d/%d", SegmentDeltalogPathPrefix, collectionID, partitionID, segmentID, fieldID)
}

// TODO: There's no need to include fieldID in the stats log path key.
func buildFieldStatslogPath(collectionID typeutil.UniqueID, partitionID typeutil.UniqueID, segmentID typeutil.UniqueID, fieldID typeutil.UniqueID) string {
	return fmt.Sprintf("%s/%d/%d/%d/%d", SegmentStatslogPathPrefix, collectionID, partitionID, segmentID, fieldID)
}

func buildFieldBinlogPathPrefix(collectionID typeutil.UniqueID, partitionID typeutil.UniqueID, segmentID typeutil.UniqueID) string {
	return fmt.Sprintf("%s/%d/%d/%d", SegmentBinlogPathPrefix, collectionID, partitionID, segmentID)
}

func buildFieldDeltalogPathPrefix(collectionID typeutil.UniqueID, partitionID typeutil.UniqueID, segmentID typeutil.UniqueID) string {
	return fmt.Sprintf("%s/%d/%d/%d", SegmentDeltalogPathPrefix, collectionID, partitionID, segmentID)
}

func buildFieldStatslogPathPrefix(collectionID typeutil.UniqueID, partitionID typeutil.UniqueID, segmentID typeutil.UniqueID) string {
	return fmt.Sprintf("%s/%d/%d/%d", SegmentStatslogPathPrefix, collectionID, partitionID, segmentID)
}

// buildChannelRemovePath builds vchannel remove flag path
func buildChannelRemovePath(channel string) string {
	return fmt.Sprintf("%s/%s", ChannelRemovePrefix, channel)
}

func buildChannelCPKey(vChannel string) string {
	return fmt.Sprintf("%s/%s", ChannelCheckpointPrefix, vChannel)
}

func BuildIndexKey(collectionID, indexID int64) string {
	return fmt.Sprintf("%s/%d/%d", util.FieldIndexPrefix, collectionID, indexID)
}

func BuildSegmentIndexKey(collectionID, partitionID, segmentID, buildID int64) string {
	return fmt.Sprintf("%s/%d/%d/%d/%d", util.SegmentIndexPrefix, collectionID, partitionID, segmentID, buildID)
}

func buildCollectionPrefix(collectionID typeutil.UniqueID) string {
	return fmt.Sprintf("%s/%d", SegmentPrefix, collectionID)
}

func buildPartitionPrefix(collectionID, partitionID typeutil.UniqueID) string {
	return fmt.Sprintf("%s/%d/%d", SegmentPrefix, collectionID, partitionID)
}

func buildImportJobKey(jobID int64) string {
	return fmt.Sprintf("%s/%d", ImportJobPrefix, jobID)
}

func buildImportTaskKey(taskID int64) string {
	return fmt.Sprintf("%s/%d", ImportTaskPrefix, taskID)
}

func buildPreImportTaskKey(taskID int64) string {
	return fmt.Sprintf("%s/%d", PreImportTaskPrefix, taskID)
}

func buildAnalyzeTaskKey(taskID int64) string {
	return fmt.Sprintf("%s/%d", AnalyzeTaskPrefix, taskID)
}

func buildStatsTaskKey(taskID int64) string {
	return fmt.Sprintf("%s/%d", StatsTaskPrefix, taskID)
}
