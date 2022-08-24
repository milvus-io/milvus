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

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

type Catalog struct {
	Txn kv.TxnKV
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

	// get binlogs from segment meta first for compatible
	var binlogs, deltalogs, statslogs []*datapb.FieldBinlog

	for _, segmentInfo := range segments {
		if len(segmentInfo.Binlogs) == 0 {
			binlogs, err = kc.unmarshalBinlog(storage.InsertBinlog, segmentInfo.CollectionID, segmentInfo.PartitionID, segmentInfo.ID)
			if err != nil {
				return nil, err
			}
		}

		if len(segmentInfo.Deltalogs) == 0 {
			deltalogs, err = kc.unmarshalBinlog(storage.DeleteBinlog, segmentInfo.CollectionID, segmentInfo.PartitionID, segmentInfo.CollectionID)
			if err != nil {
				return nil, err
			}
		}

		if len(segmentInfo.Statslogs) == 0 {
			statslogs, err = kc.unmarshalBinlog(storage.StatsBinlog, segmentInfo.CollectionID, segmentInfo.PartitionID, segmentInfo.PartitionID)
			if err != nil {
				return nil, err
			}
		}

		segmentInfo.Binlogs = binlogs
		segmentInfo.Deltalogs = deltalogs
		segmentInfo.Statslogs = statslogs
	}

	return segments, nil
}

func (kc *Catalog) AddSegment(ctx context.Context, segment *datapb.SegmentInfo) error {
	// save binlogs separately
	kvs, err := buildBinlogKvPair(segment)
	if err != nil {
		return err
	}

	// save segment info
	k, v, err := buildSegmentKeyValuePair(segment)
	if err != nil {
		return err
	}
	kvs[k] = v

	// save handoff req if segment is flushed
	if segment.State == commonpb.SegmentState_Flushed {
		flushedSegmentSegBytes, err := proto.Marshal(segment)
		if err != nil {
			return fmt.Errorf("failed to marshal segment: %d, error: %w", segment.GetID(), err)
		}
		flushSegKey := buildFlushedSegmentPath(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
		kvs[flushSegKey] = string(flushedSegmentSegBytes)
	}

	return kc.Txn.MultiSave(kvs)
}

func (kc *Catalog) AlterSegments(ctx context.Context, modSegments []*datapb.SegmentInfo) error {
	kv := make(map[string]string)
	for _, segment := range modSegments {
		// save binlogs separately
		binlogKvs, err := buildBinlogKvPair(segment)
		if err != nil {
			return err
		}
		kv = typeutil.MergeMap(binlogKvs, kv)

		// save segment info
		k, v, err := buildSegmentKeyValuePair(segment)
		if err != nil {
			return err
		}
		kv[k] = v

		// save handoff req if segment is flushed
		if segment.State == commonpb.SegmentState_Flushed {
			flushedSegmentSegBytes, err := proto.Marshal(segment)
			if err != nil {
				return fmt.Errorf("failed to marshal segment: %d, error: %w", segment.GetID(), err)
			}
			flushSegKey := buildFlushedSegmentPath(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
			kv[flushSegKey] = string(flushedSegmentSegBytes)
		}
	}

	return kc.Txn.MultiSave(kv)
}

func (kc *Catalog) AlterSegmentsAndAddNewSegment(ctx context.Context, segments []*datapb.SegmentInfo, newSegment *datapb.SegmentInfo) error {
	data := make(map[string]string)

	for _, s := range segments {
		k, v, err := buildSegmentKeyValuePair(s)
		if err != nil {
			return err
		}
		data[k] = v
	}

	if newSegment.NumOfRows > 0 {
		// save binlogs separately
		binlogKvs, err := buildBinlogKvPair(newSegment)
		if err != nil {
			return err
		}
		data = typeutil.MergeMap(binlogKvs, data)
		// save segment info
		k, v, err := buildSegmentKeyValuePair(newSegment)
		if err != nil {
			return err
		}
		data[k] = v
	}

	err := kc.Txn.MultiSave(data)
	if err != nil {
		log.Error("batch save segments failed", zap.Error(err))
		return err
	}

	return nil
}

func (kc *Catalog) SaveDroppedSegmentsInBatch(ctx context.Context, modSegments map[int64]*datapb.SegmentInfo) ([]int64, error) {
	kvs := make(map[string]string)
	batchIDs := make([]int64, 0, MaxOperationsPerTxn)

	size := 0
	for id, s := range modSegments {
		key := buildSegmentPath(s.GetCollectionID(), s.GetPartitionID(), s.GetID())
		segBytes, err := proto.Marshal(s)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal segment: %d, err: %w", s.GetID(), err)
		}
		kvs[key] = string(segBytes)
		batchIDs = append(batchIDs, s.ID)
		size += len(key) + len(segBytes)
		// remove record from map `modSegments`
		delete(modSegments, id)
		// batch stops when one of conditions matched:
		// 1. number of records is equals MaxOperationsPerTxn
		// 2. left number of modSegments is equals 1
		// 3. bytes size is greater than MaxBytesPerTxn
		if len(kvs) == MaxOperationsPerTxn || len(modSegments) == 1 || size >= MaxBytesPerTxn {
			break
		}
	}

	err := kc.Txn.MultiSave(kvs)
	if err != nil {
		log.Error("Failed to save segments in batch for DropChannel", zap.Any("segmentIDs", batchIDs), zap.Error(err))
		return nil, err
	}

	return batchIDs, nil
}

func (kc *Catalog) DropSegment(ctx context.Context, segment *datapb.SegmentInfo) error {
	segKey := buildSegmentPath(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
	keys := []string{segKey}
	binlogKvs, err := buildBinlogKvPair(segment)
	if err != nil {
		return err
	}
	binlogKeys := typeutil.GetMapKeys(binlogKvs)
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

func (kc *Catalog) unmarshalBinlog(binlogType storage.BinlogType, collectionID, partitionID, segmentID typeutil.UniqueID) ([]*datapb.FieldBinlog, error) {
	// unmarshal binlog/deltalog/statslog
	var binlogPrefix string
	switch binlogType {
	case storage.InsertBinlog:
		binlogPrefix = buildFieldBinlogPathPrefix(collectionID, partitionID, segmentID)
	case storage.DeleteBinlog:
		binlogPrefix = buildFieldDeltalogPathPrefix(collectionID, partitionID, segmentID)
	case storage.StatsBinlog:
		binlogPrefix = buildFieldStatslogPathPrefix(collectionID, partitionID, segmentID)
	default:
		return nil, fmt.Errorf("invalid binlog type: %d", binlogType)
	}
	_, values, err := kc.Txn.LoadWithPrefix(binlogPrefix)
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
		result[i] = fieldBinlog
	}
	return result, nil
}

func buildBinlogKvPair(segment *datapb.SegmentInfo) (map[string]string, error) {
	kv := make(map[string]string)

	// binlog kv
	for _, binlog := range segment.Binlogs {
		binlogBytes, err := proto.Marshal(binlog)
		if err != nil {
			return nil, fmt.Errorf("marshal binlogs failed, collectionID:%d, segmentID:%d, fieldID:%d, error:%w", segment.CollectionID, segment.GetID(), binlog.FieldID, err)
		}
		key := buildFieldBinlogPath(segment.CollectionID, segment.PartitionID, segment.ID, binlog.FieldID)
		kv[key] = string(binlogBytes)
	}

	// deltalog etcd kv
	for _, deltalog := range segment.Deltalogs {
		binlogBytes, err := proto.Marshal(deltalog)
		if err != nil {
			return nil, fmt.Errorf("marshal deltalogs failed, collectionID:%d, segmentID:%d, fieldID:%d, error:%w", segment.CollectionID, segment.GetID(), deltalog.FieldID, err)
		}
		key := buildFieldDeltalogPath(segment.CollectionID, segment.PartitionID, segment.ID, deltalog.FieldID)
		kv[key] = string(binlogBytes)
	}

	// statslog etcd kv
	for _, statslog := range segment.Statslogs {
		binlogBytes, err := proto.Marshal(statslog)
		if err != nil {
			return nil, fmt.Errorf("marshal statslogs failed, collectionID:%d, segmentID:%d, fieldID:%d, error:%w", segment.CollectionID, segment.GetID(), statslog.FieldID, err)
		}
		key := buildFieldStatslogPath(segment.CollectionID, segment.PartitionID, segment.ID, statslog.FieldID)
		kv[key] = string(binlogBytes)
	}

	return kv, nil
}

func buildSegmentKeyValuePair(segment *datapb.SegmentInfo) (string, string, error) {
	clonedSegment := proto.Clone(segment).(*datapb.SegmentInfo)
	clonedSegment.Binlogs = nil
	clonedSegment.Deltalogs = nil
	clonedSegment.Statslogs = nil
	segBytes, err := proto.Marshal(clonedSegment)
	if err != nil {
		return "", "", fmt.Errorf("failed to marshal segment: %d, err: %w", segment.ID, err)
	}
	key := buildSegmentPath(clonedSegment.GetCollectionID(), clonedSegment.GetPartitionID(), clonedSegment.GetID())
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
