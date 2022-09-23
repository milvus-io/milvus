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
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/internal/util/metautil"

	"github.com/milvus-io/milvus/internal/util/etcd"

	"golang.org/x/exp/maps"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

type Catalog struct {
	Txn                  kv.TxnKV
	ChunkManagerRootPath string
}

func (kc *Catalog) ListSegments(ctx context.Context) ([]*datapb.SegmentInfo, error) {
	_, values, err := kc.Txn.LoadWithPrefix(SegmentPrefix)
	if err != nil {
		return nil, err
	}

	// get segment info
	var segments []*datapb.SegmentInfo
	for _, value := range values {
		segmentInfo := &datapb.SegmentInfo{}
		err = proto.Unmarshal([]byte(value), segmentInfo)
		if err != nil {
			log.Error("unmarshal segment info error", zap.Int64("segmentID", segmentInfo.ID), zap.Int64("collID", segmentInfo.CollectionID), zap.Error(err))
			return nil, err
		}
		segments = append(segments, segmentInfo)
	}

	for _, segmentInfo := range segments {
		if len(segmentInfo.Binlogs) == 0 {
			binlogs, err := kc.unmarshalBinlog(storage.InsertBinlog, segmentInfo.CollectionID, segmentInfo.PartitionID, segmentInfo.ID)
			if err != nil {
				return nil, err
			}
			segmentInfo.Binlogs = binlogs
		}

		if len(segmentInfo.Deltalogs) == 0 {
			deltalogs, err := kc.unmarshalBinlog(storage.DeleteBinlog, segmentInfo.CollectionID, segmentInfo.PartitionID, segmentInfo.ID)
			if err != nil {
				return nil, err
			}
			segmentInfo.Deltalogs = deltalogs
		}

		if len(segmentInfo.Statslogs) == 0 {
			statslogs, err := kc.unmarshalBinlog(storage.StatsBinlog, segmentInfo.CollectionID, segmentInfo.PartitionID, segmentInfo.ID)
			if err != nil {
				return nil, err
			}
			segmentInfo.Statslogs = statslogs
		}
	}

	return segments, nil
}

func (kc *Catalog) AddSegment(ctx context.Context, segment *datapb.SegmentInfo) error {
	kvs, err := buildSegmentAndBinlogsKvs(segment)
	if err != nil {
		return err
	}

	// save handoff req if segment is flushed
	if segment.State == commonpb.SegmentState_Flushed {
		flushSegKey := buildFlushedSegmentPath(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
		kvs[flushSegKey] = strconv.FormatInt(segment.GetID(), 10)
	}

	return kc.Txn.MultiSave(kvs)
}

func (kc *Catalog) AlterSegments(ctx context.Context, modSegments []*datapb.SegmentInfo) error {
	if len(modSegments) == 0 {
		return nil
	}

	kvs := make(map[string]string)
	for _, segment := range modSegments {
		segmentKvs, err := buildSegmentAndBinlogsKvs(segment)
		if err != nil {
			return err
		}

		maps.Copy(kvs, segmentKvs)
		// save handoff req if segment is flushed
		if segment.State == commonpb.SegmentState_Flushed {
			flushSegKey := buildFlushedSegmentPath(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
			kvs[flushSegKey] = strconv.FormatInt(segment.GetID(), 10)
		}
	}

	return kc.Txn.MultiSave(kvs)
}

func (kc *Catalog) hasBinlogPrefix(segment *datapb.SegmentInfo) (bool, error) {
	binlogsKey, _, err := kc.getBinlogsWithPrefix(storage.InsertBinlog, segment.CollectionID, segment.PartitionID, segment.ID)
	if err != nil {
		return false, err
	}

	deltalogsKey, _, err := kc.getBinlogsWithPrefix(storage.DeleteBinlog, segment.CollectionID, segment.PartitionID, segment.ID)
	if err != nil {
		return false, err
	}

	statslogsKey, _, err := kc.getBinlogsWithPrefix(storage.StatsBinlog, segment.CollectionID, segment.PartitionID, segment.ID)
	if err != nil {
		return false, err
	}

	if len(binlogsKey) == 0 && len(deltalogsKey) == 0 && len(statslogsKey) == 0 {
		return false, nil
	}

	return true, nil
}

func (kc *Catalog) AlterSegmentsAndAddNewSegment(ctx context.Context, segments []*datapb.SegmentInfo, newSegment *datapb.SegmentInfo) error {
	kvs := make(map[string]string)

	for _, s := range segments {
		noBinlogsSegment, binlogs, deltalogs, statslogs := CloneSegmentWithExcludeBinlogs(s)

		// for compacted segments
		if noBinlogsSegment.State == commonpb.SegmentState_Dropped {
			hasBinlogkeys, err := kc.hasBinlogPrefix(s)
			if err != nil {
				return err
			}

			// In order to guarantee back compatibility, the old format segments need
			// convert to new format that include segment key and three binlog keys,
			// or GC can not find data path on the storage.
			if !hasBinlogkeys {
				binlogsKvs, err := buildBinlogKvsWithLogID(noBinlogsSegment.CollectionID, noBinlogsSegment.PartitionID, noBinlogsSegment.ID, binlogs, deltalogs, statslogs)
				if err != nil {
					return err
				}
				maps.Copy(kvs, binlogsKvs)
			}
		}

		k, v, err := buildSegmentKv(noBinlogsSegment)
		if err != nil {
			return err
		}
		kvs[k] = v
	}

	if newSegment != nil {
		segmentKvs, err := buildSegmentAndBinlogsKvs(newSegment)
		if err != nil {
			return err
		}

		maps.Copy(kvs, segmentKvs)
	}

	return kc.Txn.MultiSave(kvs)
}

// RevertAlterSegmentsAndAddNewSegment reverts the metastore operation of AtlerSegmentsAndAddNewSegment
func (kc *Catalog) RevertAlterSegmentsAndAddNewSegment(ctx context.Context, oldSegments []*datapb.SegmentInfo, removeSegment *datapb.SegmentInfo) error {
	var (
		kvs      = make(map[string]string)
		removals []string
	)

	for _, s := range oldSegments {
		segmentKvs, err := buildSegmentAndBinlogsKvs(s)
		if err != nil {
			return err
		}

		maps.Copy(kvs, segmentKvs)
	}

	if removeSegment != nil {
		segKey := buildSegmentPath(removeSegment.GetCollectionID(), removeSegment.GetPartitionID(), removeSegment.GetID())
		removals = append(removals, segKey)
		binlogKeys := buildBinlogKeys(removeSegment)
		removals = append(removals, binlogKeys...)
	}

	err := kc.Txn.MultiSaveAndRemove(kvs, removals)
	if err != nil {
		log.Warn("batch save and remove segments failed", zap.Error(err))
		return err
	}

	return nil
}

func (kc *Catalog) SaveDroppedSegmentsInBatch(ctx context.Context, segments []*datapb.SegmentInfo) error {
	if len(segments) == 0 {
		return nil
	}

	kvs := make(map[string]string)
	for _, s := range segments {
		key := buildSegmentPath(s.GetCollectionID(), s.GetPartitionID(), s.GetID())
		noBinlogsSegment, _, _, _ := CloneSegmentWithExcludeBinlogs(s)
		segBytes, err := proto.Marshal(noBinlogsSegment)
		if err != nil {
			return fmt.Errorf("failed to marshal segment: %d, err: %w", s.GetID(), err)
		}
		kvs[key] = string(segBytes)
	}

	saveFn := func(partialKvs map[string]string) error {
		return kc.Txn.MultiSave(partialKvs)
	}
	if err := etcd.SaveByBatch(kvs, saveFn); err != nil {
		return err
	}

	return nil
}

func (kc *Catalog) DropSegment(ctx context.Context, segment *datapb.SegmentInfo) error {
	segKey := buildSegmentPath(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
	keys := []string{segKey}
	binlogKeys := buildBinlogKeys(segment)
	keys = append(keys, binlogKeys...)
	return kc.Txn.MultiRemove(keys)
}

func (kc *Catalog) MarkChannelDeleted(ctx context.Context, channel string) error {
	key := buildChannelRemovePath(channel)
	err := kc.Txn.Save(key, RemoveFlagTomestone)
	if err != nil {
		log.Error("Failed to mark channel dropped", zap.String("channel", channel), zap.Error(err))
		return err
	}

	return nil
}

func (kc *Catalog) IsChannelDropped(ctx context.Context, channel string) bool {
	key := buildChannelRemovePath(channel)
	v, err := kc.Txn.Load(key)
	if err != nil || v != RemoveFlagTomestone {
		return false
	}
	return true
}

// DropChannel removes channel remove flag after whole procedure is finished
func (kc *Catalog) DropChannel(ctx context.Context, channel string) error {
	key := buildChannelRemovePath(channel)
	return kc.Txn.Remove(key)
}

func (kc *Catalog) getBinlogsWithPrefix(binlogType storage.BinlogType, collectionID, partitionID,
	segmentID typeutil.UniqueID) ([]string, []string, error) {
	var binlogPrefix string
	switch binlogType {
	case storage.InsertBinlog:
		binlogPrefix = buildFieldBinlogPathPrefix(collectionID, partitionID, segmentID)
	case storage.DeleteBinlog:
		binlogPrefix = buildFieldDeltalogPathPrefix(collectionID, partitionID, segmentID)
	case storage.StatsBinlog:
		binlogPrefix = buildFieldStatslogPathPrefix(collectionID, partitionID, segmentID)
	default:
		return nil, nil, fmt.Errorf("invalid binlog type: %d", binlogType)
	}
	keys, values, err := kc.Txn.LoadWithPrefix(binlogPrefix)
	if err != nil {
		return nil, nil, err
	}

	return keys, values, nil
}

// unmarshal binlog/deltalog/statslog
func (kc *Catalog) unmarshalBinlog(binlogType storage.BinlogType, collectionID, partitionID, segmentID typeutil.UniqueID) ([]*datapb.FieldBinlog, error) {
	_, values, err := kc.getBinlogsWithPrefix(binlogType, collectionID, partitionID, segmentID)
	if err != nil {
		return nil, err
	}

	result := make([]*datapb.FieldBinlog, len(values))
	for i, value := range values {
		fieldBinlog := &datapb.FieldBinlog{}
		err = proto.Unmarshal([]byte(value), fieldBinlog)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal datapb.FieldBinlog: %d, err:%w", fieldBinlog.FieldID, err)
		}

		fillLogPathByLogID(kc.ChunkManagerRootPath, binlogType, collectionID, partitionID, segmentID, fieldBinlog)
		result[i] = fieldBinlog
	}
	return result, nil
}

func fillLogPathByLogID(chunkManagerRootPath string, binlogType storage.BinlogType, collectionID, partitionID,
	segmentID typeutil.UniqueID, fieldBinlog *datapb.FieldBinlog) error {
	for _, binlog := range fieldBinlog.Binlogs {
		path, err := buildLogPath(chunkManagerRootPath, binlogType, collectionID, partitionID,
			segmentID, fieldBinlog.GetFieldID(), binlog.GetLogID())
		if err != nil {
			return err
		}

		binlog.LogPath = path
	}

	return nil
}

func fillLogIDByLogPath(multiFieldBinlogs ...[]*datapb.FieldBinlog) error {
	for _, fieldBinlogs := range multiFieldBinlogs {
		for _, fieldBinlog := range fieldBinlogs {
			for _, binlog := range fieldBinlog.Binlogs {
				logPath := binlog.LogPath
				idx := strings.LastIndex(logPath, "/")
				if idx == -1 {
					return fmt.Errorf("invailed binlog path: %s", logPath)
				}
				logPathStr := logPath[(idx + 1):]
				logID, err := strconv.ParseInt(logPathStr, 10, 64)
				if err != nil {
					return err
				}

				// set log path to empty and only store log id
				binlog.LogPath = ""
				binlog.LogID = logID
			}
		}
	}
	return nil
}

// build a binlog path on the storage by metadata
func buildLogPath(chunkManagerRootPath string, binlogType storage.BinlogType, collectionID, partitionID, segmentID, filedID, logID typeutil.UniqueID) (string, error) {
	switch binlogType {
	case storage.InsertBinlog:
		path := metautil.BuildInsertLogPath(chunkManagerRootPath, collectionID, partitionID, segmentID, filedID, logID)
		return path, nil
	case storage.DeleteBinlog:
		path := metautil.BuildDeltaLogPath(chunkManagerRootPath, collectionID, partitionID, segmentID, logID)
		return path, nil
	case storage.StatsBinlog:
		path := metautil.BuildStatsLogPath(chunkManagerRootPath, collectionID, partitionID, segmentID, filedID, logID)
		return path, nil
	default:
		return "", fmt.Errorf("invalid binlog type: %d", binlogType)
	}
}

func buildBinlogKvsWithLogID(collectionID, partitionID, segmentID typeutil.UniqueID,
	binlogs, deltalogs, statslogs []*datapb.FieldBinlog) (map[string]string, error) {

	fillLogIDByLogPath(binlogs, deltalogs, statslogs)
	kvs, err := buildBinlogKvs(collectionID, partitionID, segmentID, binlogs, deltalogs, statslogs)
	if err != nil {
		return nil, err
	}

	return kvs, nil
}

func buildSegmentAndBinlogsKvs(segment *datapb.SegmentInfo) (map[string]string, error) {
	noBinlogsSegment, binlogs, deltalogs, statslogs := CloneSegmentWithExcludeBinlogs(segment)

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

func buildBinlogKvs(collectionID, partitionID, segmentID typeutil.UniqueID, binlogs, deltalogs, statslogs []*datapb.FieldBinlog) (map[string]string, error) {
	kv := make(map[string]string)

	// binlog kv
	for _, binlog := range binlogs {
		binlogBytes, err := proto.Marshal(binlog)
		if err != nil {
			return nil, fmt.Errorf("marshal binlogs failed, collectionID:%d, segmentID:%d, fieldID:%d, error:%w", collectionID, segmentID, binlog.FieldID, err)
		}
		key := buildFieldBinlogPath(collectionID, partitionID, segmentID, binlog.FieldID)
		kv[key] = string(binlogBytes)
	}

	// deltalog
	for _, deltalog := range deltalogs {
		binlogBytes, err := proto.Marshal(deltalog)
		if err != nil {
			return nil, fmt.Errorf("marshal deltalogs failed, collectionID:%d, segmentID:%d, fieldID:%d, error:%w", collectionID, segmentID, deltalog.FieldID, err)
		}
		key := buildFieldDeltalogPath(collectionID, partitionID, segmentID, deltalog.FieldID)
		kv[key] = string(binlogBytes)
	}

	// statslog
	for _, statslog := range statslogs {
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

func buildSegmentKv(segment *datapb.SegmentInfo) (string, string, error) {
	segBytes, err := proto.Marshal(segment)
	if err != nil {
		return "", "", fmt.Errorf("failed to marshal segment: %d, err: %w", segment.ID, err)
	}
	key := buildSegmentPath(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
	return key, string(segBytes), nil
}

// buildSegmentPath common logic mapping segment info to corresponding key in kv store
func buildSegmentPath(collectionID typeutil.UniqueID, partitionID typeutil.UniqueID, segmentID typeutil.UniqueID) string {
	return fmt.Sprintf("%s/%d/%d/%d", SegmentPrefix, collectionID, partitionID, segmentID)
}

func buildFieldBinlogPath(collectionID typeutil.UniqueID, partitionID typeutil.UniqueID, segmentID typeutil.UniqueID, fieldID typeutil.UniqueID) string {
	return fmt.Sprintf("%s/%d/%d/%d/%d", SegmentBinlogPathPrefix, collectionID, partitionID, segmentID, fieldID)
}

func buildFieldDeltalogPath(collectionID typeutil.UniqueID, partitionID typeutil.UniqueID, segmentID typeutil.UniqueID, fieldID typeutil.UniqueID) string {
	return fmt.Sprintf("%s/%d/%d/%d/%d", SegmentDeltalogPathPrefix, collectionID, partitionID, segmentID, fieldID)
}

func buildFieldStatslogPath(collectionID typeutil.UniqueID, partitionID typeutil.UniqueID, segmentID typeutil.UniqueID, fieldID typeutil.UniqueID) string {
	return fmt.Sprintf("%s/%d/%d/%d/%d", SegmentStatslogPathPrefix, collectionID, partitionID, segmentID, fieldID)
}

// buildFlushedSegmentPath common logic mapping segment info to corresponding key of IndexCoord in kv store
func buildFlushedSegmentPath(collectionID typeutil.UniqueID, partitionID typeutil.UniqueID, segmentID typeutil.UniqueID) string {
	return fmt.Sprintf("%s/%d/%d/%d", util.FlushedSegmentPrefix, collectionID, partitionID, segmentID)
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
