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
	"path"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/binlog"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/segmentutil"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/kv"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type Catalog struct {
	MetaKv kv.MetaKv

	paginationSize       int
	ChunkManagerRootPath string
	metaRootpath         string
}

func NewCatalog(MetaKv kv.MetaKv, chunkManagerRootPath string, metaRootpath string) *Catalog {
	return &Catalog{
		MetaKv:               MetaKv,
		paginationSize:       paramtable.Get().MetaStoreCfg.PaginationSize.GetAsInt(),
		ChunkManagerRootPath: chunkManagerRootPath,
		metaRootpath:         metaRootpath,
	}
}

func (kc *Catalog) ListSegments(ctx context.Context) ([]*datapb.SegmentInfo, error) {
	group, _ := errgroup.WithContext(ctx)
	segments := make([]*datapb.SegmentInfo, 0)
	insertLogs := make(map[typeutil.UniqueID][]*datapb.FieldBinlog, 1)
	deltaLogs := make(map[typeutil.UniqueID][]*datapb.FieldBinlog, 1)
	statsLogs := make(map[typeutil.UniqueID][]*datapb.FieldBinlog, 1)
	bm25Logs := make(map[typeutil.UniqueID][]*datapb.FieldBinlog, 1)

	executeFn := func(binlogType storage.BinlogType, result map[typeutil.UniqueID][]*datapb.FieldBinlog) {
		group.Go(func() error {
			ret, err := kc.listBinlogs(ctx, binlogType)
			if err != nil {
				return err
			}

			maps.Copy(result, ret)
			return nil
		})
	}

	// execute list segment meta
	executeFn(storage.InsertBinlog, insertLogs)
	executeFn(storage.DeleteBinlog, deltaLogs)
	executeFn(storage.StatsBinlog, statsLogs)
	executeFn(storage.BM25Binlog, bm25Logs)
	group.Go(func() error {
		ret, err := kc.listSegments(ctx)
		if err != nil {
			return err
		}
		segments = append(segments, ret...)
		return nil
	})

	err := group.Wait()
	if err != nil {
		return nil, err
	}

	err = kc.applyBinlogInfo(segments, insertLogs, deltaLogs, statsLogs, bm25Logs)
	if err != nil {
		return nil, err
	}
	return segments, nil
}

func (kc *Catalog) listSegments(ctx context.Context) ([]*datapb.SegmentInfo, error) {
	segments := make([]*datapb.SegmentInfo, 0)

	applyFn := func(key []byte, value []byte) error {
		// due to SegmentStatslogPathPrefix has the same prefix with SegmentPrefix, so skip it.
		if strings.Contains(string(key), SegmentStatslogPathPrefix) {
			return nil
		}

		// due to StatsTaskPrefix has the same prefix with SegmentPrefix, so skip it.
		// when the WalkWithPrefix is refactored, this patch can be removed.
		if strings.Contains(string(key), StatsTaskPrefix) {
			return nil
		}

		segmentInfo := &datapb.SegmentInfo{}
		err := proto.Unmarshal(value, segmentInfo)
		if err != nil {
			return err
		}

		segments = append(segments, segmentInfo)
		return nil
	}

	err := kc.MetaKv.WalkWithPrefix(ctx, SegmentPrefix+"/", kc.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}

	return segments, nil
}

func (kc *Catalog) parseBinlogKey(key string, prefixIdx int) (int64, int64, int64, error) {
	remainedKey := key[prefixIdx:]
	keyWordGroup := strings.Split(remainedKey, "/")
	if len(keyWordGroup) < 3 {
		return 0, 0, 0, fmt.Errorf("parse key: %s failed, trimmed key:%s", key, remainedKey)
	}

	collectionID, err := strconv.ParseInt(keyWordGroup[0], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("parse key: %s failed, trimmed key:%s, %w", key, remainedKey, err)
	}

	partitionID, err := strconv.ParseInt(keyWordGroup[1], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("parse key: %s failed, trimmed key:%s, %w", key, remainedKey, err)
	}

	segmentID, err := strconv.ParseInt(keyWordGroup[2], 10, 64)
	if err != nil {
		return 0, 0, 0, fmt.Errorf("parse key: %s failed, trimmed key:%s, %w", key, remainedKey, err)
	}

	return collectionID, partitionID, segmentID, nil
}

func (kc *Catalog) listBinlogs(ctx context.Context, binlogType storage.BinlogType) (map[typeutil.UniqueID][]*datapb.FieldBinlog, error) {
	ret := make(map[typeutil.UniqueID][]*datapb.FieldBinlog)

	var err error
	var logPathPrefix string
	switch binlogType {
	case storage.InsertBinlog:
		logPathPrefix = SegmentBinlogPathPrefix
	case storage.DeleteBinlog:
		logPathPrefix = SegmentDeltalogPathPrefix
	case storage.StatsBinlog:
		logPathPrefix = SegmentStatslogPathPrefix
	case storage.BM25Binlog:
		logPathPrefix = SegmentBM25logPathPrefix
	default:
		err = fmt.Errorf("invalid binlog type: %d", binlogType)
	}
	if err != nil {
		return nil, err
	}

	var prefixIdx int
	if len(kc.metaRootpath) == 0 {
		prefixIdx = len(logPathPrefix) + 1
	} else {
		prefixIdx = len(kc.metaRootpath) + 1 + len(logPathPrefix) + 1
	}

	applyFn := func(key []byte, value []byte) error {
		fieldBinlog := &datapb.FieldBinlog{}
		err := proto.Unmarshal(value, fieldBinlog)
		if err != nil {
			return fmt.Errorf("failed to unmarshal datapb.FieldBinlog: %d, err:%w", fieldBinlog.FieldID, err)
		}

		_, _, segmentID, err := kc.parseBinlogKey(string(key), prefixIdx)
		if err != nil {
			return fmt.Errorf("prefix:%s, %w", path.Join(kc.metaRootpath, logPathPrefix), err)
		}

		// set log size to memory size if memory size is zero for old segment before v2.4.3
		for i, b := range fieldBinlog.GetBinlogs() {
			if b.GetMemorySize() == 0 {
				fieldBinlog.Binlogs[i].MemorySize = b.GetLogSize()
			}
		}

		// no need to set log path and only store log id
		ret[segmentID] = append(ret[segmentID], fieldBinlog)
		return nil
	}

	err = kc.MetaKv.WalkWithPrefix(ctx, logPathPrefix, kc.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (kc *Catalog) applyBinlogInfo(segments []*datapb.SegmentInfo, insertLogs, deltaLogs,
	statsLogs, bm25Logs map[typeutil.UniqueID][]*datapb.FieldBinlog,
) error {
	var err error
	for _, segmentInfo := range segments {
		if len(segmentInfo.Binlogs) == 0 {
			segmentInfo.Binlogs = insertLogs[segmentInfo.ID]
		}
		if err = binlog.CompressFieldBinlogs(segmentInfo.Binlogs); err != nil {
			return err
		}

		if len(segmentInfo.Deltalogs) == 0 {
			segmentInfo.Deltalogs = deltaLogs[segmentInfo.ID]
		}
		if err = binlog.CompressFieldBinlogs(segmentInfo.Deltalogs); err != nil {
			return err
		}

		if len(segmentInfo.Statslogs) == 0 {
			segmentInfo.Statslogs = statsLogs[segmentInfo.ID]
		}
		if err = binlog.CompressFieldBinlogs(segmentInfo.Statslogs); err != nil {
			return err
		}

		if len(segmentInfo.Bm25Statslogs) == 0 {
			segmentInfo.Bm25Statslogs = bm25Logs[segmentInfo.ID]
		}
		if err = binlog.CompressFieldBinlogs(segmentInfo.Bm25Statslogs); err != nil {
			return err
		}
	}
	return nil
}

func (kc *Catalog) AddSegment(ctx context.Context, segment *datapb.SegmentInfo) error {
	kvs, err := buildSegmentAndBinlogsKvs(segment)
	if err != nil {
		return err
	}
	return kc.MetaKv.MultiSave(ctx, kvs)
}

// LoadFromSegmentPath loads segment info from persistent storage by given segment path.
// # TESTING ONLY #
func (kc *Catalog) LoadFromSegmentPath(ctx context.Context, colID, partID, segID typeutil.UniqueID) (*datapb.SegmentInfo, error) {
	v, err := kc.MetaKv.Load(ctx, buildSegmentPath(colID, partID, segID))
	if err != nil {
		log.Ctx(context.TODO()).Error("(testing only) failed to load segment info by segment path")
		return nil, err
	}

	segInfo := &datapb.SegmentInfo{}
	err = proto.Unmarshal([]byte(v), segInfo)
	if err != nil {
		log.Ctx(context.TODO()).Error("(testing only) failed to unmarshall segment info")
		return nil, err
	}

	return segInfo, nil
}

func (kc *Catalog) AlterSegments(ctx context.Context, segments []*datapb.SegmentInfo, binlogs ...metastore.BinlogsIncrement) error {
	if len(segments) == 0 {
		return nil
	}
	kvs := make(map[string]string)
	for _, segment := range segments {
		// we don't persist binlog fields, but instead store binlogs as independent kvs
		cloned := proto.Clone(segment).(*datapb.SegmentInfo)
		resetBinlogFields(cloned)

		rowCount := segmentutil.CalcRowCountFromBinLog(segment)
		if cloned.GetNumOfRows() != rowCount {
			cloned.NumOfRows = rowCount
		}

		if segment.GetState() == commonpb.SegmentState_Dropped {
			binlogs, err := kc.handleDroppedSegment(ctx, segment)
			if err != nil {
				return err
			}
			maps.Copy(kvs, binlogs)
		}

		k, v, err := buildSegmentKv(cloned)
		if err != nil {
			return err
		}
		kvs[k] = v
	}

	for _, b := range binlogs {
		segment := b.Segment

		binlogKvs, err := buildBinlogKvsWithLogID(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID(),
			cloneLogs(segment.GetBinlogs()), cloneLogs(segment.GetDeltalogs()), cloneLogs(segment.GetStatslogs()), cloneLogs(segment.GetBm25Statslogs()))
		if err != nil {
			return err
		}

		maps.Copy(kvs, binlogKvs)
	}

	return kc.SaveByBatch(ctx, kvs)
}

func (kc *Catalog) handleDroppedSegment(ctx context.Context, segment *datapb.SegmentInfo) (kvs map[string]string, err error) {
	var has bool
	has, err = kc.hasBinlogPrefix(ctx, segment)
	if err != nil {
		return
	}
	// To be compatible with previous implementation, we have to write binlogs on etcd for correct gc.
	if !has {
		kvs, err = buildBinlogKvsWithLogID(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID(), cloneLogs(segment.GetBinlogs()), cloneLogs(segment.GetDeltalogs()), cloneLogs(segment.GetStatslogs()), cloneLogs(segment.GetBm25Statslogs()))
		if err != nil {
			return
		}
	}
	return
}

func (kc *Catalog) SaveByBatch(ctx context.Context, kvs map[string]string) error {
	saveFn := func(partialKvs map[string]string) error {
		return kc.MetaKv.MultiSave(ctx, partialKvs)
	}
	err := etcd.SaveByBatchWithLimit(kvs, util.MaxEtcdTxnNum, saveFn)
	if err != nil {
		log.Ctx(ctx).Error("failed to save by batch", zap.Error(err))
		return err
	}
	return nil
}

func (kc *Catalog) hasBinlogPrefix(ctx context.Context, segment *datapb.SegmentInfo) (bool, error) {
	collectionID, partitionID, segmentID := segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID()
	prefix := buildFieldBinlogPathPrefix(collectionID, partitionID, segmentID)
	hasBinlogPrefix, err := kc.MetaKv.HasPrefix(ctx, prefix)
	if err != nil {
		return false, err
	}

	prefix = buildFieldDeltalogPathPrefix(collectionID, partitionID, segmentID)
	hasDeltaPrefix, err := kc.MetaKv.HasPrefix(ctx, prefix)
	if err != nil {
		return false, err
	}

	prefix = buildFieldStatslogPathPrefix(collectionID, partitionID, segmentID)
	hasStatsPrefix, err := kc.MetaKv.HasPrefix(ctx, prefix)
	if err != nil {
		return false, err
	}

	return hasBinlogPrefix || hasDeltaPrefix || hasStatsPrefix, nil
}

func (kc *Catalog) SaveDroppedSegmentsInBatch(ctx context.Context, segments []*datapb.SegmentInfo) error {
	if len(segments) == 0 {
		return nil
	}

	kvs := make(map[string]string)
	for _, s := range segments {
		key := buildSegmentPath(s.GetCollectionID(), s.GetPartitionID(), s.GetID())
		noBinlogsSegment, _, _, _, _ := CloneSegmentWithExcludeBinlogs(s)
		// `s` is not mutated above. Also, `noBinlogsSegment` is a cloned version of `s`.
		segmentutil.ReCalcRowCount(s, noBinlogsSegment)
		segBytes, err := proto.Marshal(noBinlogsSegment)
		if err != nil {
			return fmt.Errorf("failed to marshal segment: %d, err: %w", s.GetID(), err)
		}
		kvs[key] = string(segBytes)
	}

	saveFn := func(partialKvs map[string]string) error {
		return kc.MetaKv.MultiSave(ctx, partialKvs)
	}
	if err := etcd.SaveByBatchWithLimit(kvs, util.MaxEtcdTxnNum, saveFn); err != nil {
		return err
	}

	return nil
}

func (kc *Catalog) DropSegment(ctx context.Context, segment *datapb.SegmentInfo) error {
	segKey := buildSegmentPath(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
	binlogPreix := fmt.Sprintf("%s/%d/%d/%d", SegmentBinlogPathPrefix, segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
	deltalogPreix := fmt.Sprintf("%s/%d/%d/%d", SegmentDeltalogPathPrefix, segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
	statelogPreix := fmt.Sprintf("%s/%d/%d/%d", SegmentStatslogPathPrefix, segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
	bm25logPrefix := fmt.Sprintf("%s/%d/%d/%d", SegmentBM25logPathPrefix, segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())

	keys := []string{segKey, binlogPreix, deltalogPreix, statelogPreix, bm25logPrefix}
	if err := kc.MetaKv.MultiSaveAndRemoveWithPrefix(ctx, nil, keys); err != nil {
		return err
	}

	return nil
}

func (kc *Catalog) MarkChannelAdded(ctx context.Context, channel string) error {
	key := buildChannelRemovePath(channel)
	err := kc.MetaKv.Save(ctx, key, NonRemoveFlagTomestone)
	if err != nil {
		log.Ctx(ctx).Error("failed to mark channel added", zap.String("channel", channel), zap.Error(err))
		return err
	}
	log.Ctx(ctx).Info("NON remove flag tombstone added", zap.String("channel", channel))
	return nil
}

func (kc *Catalog) MarkChannelDeleted(ctx context.Context, channel string) error {
	key := buildChannelRemovePath(channel)
	err := kc.MetaKv.Save(ctx, key, RemoveFlagTomestone)
	if err != nil {
		log.Ctx(ctx).Error("Failed to mark channel dropped", zap.String("channel", channel), zap.Error(err))
		return err
	}
	log.Ctx(ctx).Info("remove flag tombstone added", zap.String("channel", channel))
	return nil
}

func (kc *Catalog) ShouldDropChannel(ctx context.Context, channel string) bool {
	key := buildChannelRemovePath(channel)
	v, err := kc.MetaKv.Load(ctx, key)
	if err != nil || v != RemoveFlagTomestone {
		return false
	}
	return true
}

func (kc *Catalog) ChannelExists(ctx context.Context, channel string) bool {
	key := buildChannelRemovePath(channel)
	v, err := kc.MetaKv.Load(ctx, key)
	return err == nil && v == NonRemoveFlagTomestone
}

// DropChannel removes channel remove flag after whole procedure is finished
func (kc *Catalog) DropChannel(ctx context.Context, channel string) error {
	key := buildChannelRemovePath(channel)
	log.Ctx(ctx).Info("removing channel remove path", zap.String("channel", channel))
	return kc.MetaKv.Remove(ctx, key)
}

func (kc *Catalog) ListChannelCheckpoint(ctx context.Context) (map[string]*msgpb.MsgPosition, error) {
	channelCPs := make(map[string]*msgpb.MsgPosition)
	applyFn := func(key []byte, value []byte) error {
		channelCP := &msgpb.MsgPosition{}
		err := proto.Unmarshal(value, channelCP)
		if err != nil {
			log.Ctx(ctx).Error("unmarshal channelCP failed when ListChannelCheckpoint", zap.Error(err))
			return err
		}
		ss := strings.Split(string(key), "/")
		vChannel := ss[len(ss)-1]
		channelCPs[vChannel] = channelCP
		return nil
	}

	err := kc.MetaKv.WalkWithPrefix(ctx, ChannelCheckpointPrefix, kc.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}

	return channelCPs, nil
}

func (kc *Catalog) SaveChannelCheckpoint(ctx context.Context, vChannel string, pos *msgpb.MsgPosition) error {
	k := buildChannelCPKey(vChannel)
	v, err := proto.Marshal(pos)
	if err != nil {
		return err
	}
	return kc.MetaKv.Save(ctx, k, string(v))
}

func (kc *Catalog) SaveChannelCheckpoints(ctx context.Context, positions []*msgpb.MsgPosition) error {
	kvs := make(map[string]string)
	for _, position := range positions {
		k := buildChannelCPKey(position.GetChannelName())
		v, err := proto.Marshal(position)
		if err != nil {
			return err
		}
		kvs[k] = string(v)
	}
	return kc.SaveByBatch(ctx, kvs)
}

func (kc *Catalog) DropChannelCheckpoint(ctx context.Context, vChannel string) error {
	k := buildChannelCPKey(vChannel)
	return kc.MetaKv.Remove(ctx, k)
}

func (kc *Catalog) getBinlogsWithPrefix(ctx context.Context, binlogType storage.BinlogType, collectionID, partitionID,
	segmentID typeutil.UniqueID,
) ([]string, []string, error) {
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
	keys, values, err := kc.MetaKv.LoadWithPrefix(ctx, binlogPrefix)
	if err != nil {
		return nil, nil, err
	}
	return keys, values, nil
}

func (kc *Catalog) CreateIndex(ctx context.Context, index *model.Index) error {
	key := BuildIndexKey(index.CollectionID, index.IndexID)

	value, err := proto.Marshal(model.MarshalIndexModel(index))
	if err != nil {
		return err
	}

	err = kc.MetaKv.Save(ctx, key, string(value))
	if err != nil {
		return err
	}
	return nil
}

func (kc *Catalog) ListIndexes(ctx context.Context) ([]*model.Index, error) {
	indexes := make([]*model.Index, 0)
	applyFn := func(key []byte, value []byte) error {
		meta := &indexpb.FieldIndex{}
		err := proto.Unmarshal(value, meta)
		if err != nil {
			log.Ctx(ctx).Warn("unmarshal index info failed", zap.Error(err))
			return err
		}

		indexes = append(indexes, model.UnmarshalIndexModel(meta))
		return nil
	}

	err := kc.MetaKv.WalkWithPrefix(ctx, util.FieldIndexPrefix, kc.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}
	return indexes, nil
}

func (kc *Catalog) AlterIndexes(ctx context.Context, indexes []*model.Index) error {
	kvs := make(map[string]string)
	for _, index := range indexes {
		key := BuildIndexKey(index.CollectionID, index.IndexID)

		value, err := proto.Marshal(model.MarshalIndexModel(index))
		if err != nil {
			return err
		}

		kvs[key] = string(value)
		// TODO when we have better txn kv we should make this as a transaction
		if len(kvs) >= 64 {
			err = kc.MetaKv.MultiSave(ctx, kvs)
			if err != nil {
				return err
			}
			kvs = make(map[string]string)
		}
	}
	if len(kvs) != 0 {
		return kc.MetaKv.MultiSave(ctx, kvs)
	}
	return nil
}

func (kc *Catalog) DropIndex(ctx context.Context, collID typeutil.UniqueID, dropIdxID typeutil.UniqueID) error {
	key := BuildIndexKey(collID, dropIdxID)

	err := kc.MetaKv.Remove(ctx, key)
	if err != nil {
		log.Ctx(ctx).Error("drop collection index meta fail", zap.Int64("collectionID", collID),
			zap.Int64("indexID", dropIdxID), zap.Error(err))
		return err
	}

	return nil
}

func (kc *Catalog) CreateSegmentIndex(ctx context.Context, segIdx *model.SegmentIndex) error {
	key := BuildSegmentIndexKey(segIdx.CollectionID, segIdx.PartitionID, segIdx.SegmentID, segIdx.BuildID)

	value, err := proto.Marshal(model.MarshalSegmentIndexModel(segIdx))
	if err != nil {
		return err
	}
	err = kc.MetaKv.Save(ctx, key, string(value))
	if err != nil {
		log.Ctx(ctx).Error("failed to save segment index meta in etcd", zap.Int64("buildID", segIdx.BuildID),
			zap.Int64("segmentID", segIdx.SegmentID), zap.Error(err))
		return err
	}
	return nil
}

func (kc *Catalog) ListSegmentIndexes(ctx context.Context) ([]*model.SegmentIndex, error) {
	segIndexes := make([]*model.SegmentIndex, 0)
	applyFn := func(key []byte, value []byte) error {
		segmentIndexInfo := &indexpb.SegmentIndex{}
		err := proto.Unmarshal(value, segmentIndexInfo)
		if err != nil {
			log.Ctx(ctx).Warn("unmarshal segment index info failed", zap.Error(err))
			return err
		}

		segIndexes = append(segIndexes, model.UnmarshalSegmentIndexModel(segmentIndexInfo))
		return nil
	}

	err := kc.MetaKv.WalkWithPrefix(ctx, util.SegmentIndexPrefix, kc.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}

	return segIndexes, nil
}

func (kc *Catalog) AlterSegmentIndexes(ctx context.Context, segIdxes []*model.SegmentIndex) error {
	kvs := make(map[string]string)
	for _, segIdx := range segIdxes {
		key := BuildSegmentIndexKey(segIdx.CollectionID, segIdx.PartitionID, segIdx.SegmentID, segIdx.BuildID)
		value, err := proto.Marshal(model.MarshalSegmentIndexModel(segIdx))
		if err != nil {
			return err
		}
		kvs[key] = string(value)
	}
	return kc.MetaKv.MultiSave(ctx, kvs)
}

func (kc *Catalog) DropSegmentIndex(ctx context.Context, collID, partID, segID, buildID typeutil.UniqueID) error {
	key := BuildSegmentIndexKey(collID, partID, segID, buildID)

	err := kc.MetaKv.Remove(ctx, key)
	if err != nil {
		log.Ctx(ctx).Error("drop segment index meta fail", zap.Int64("buildID", buildID), zap.Error(err))
		return err
	}

	return nil
}

func (kc *Catalog) SaveImportJob(ctx context.Context, job *datapb.ImportJob) error {
	key := buildImportJobKey(job.GetJobID())
	value, err := proto.Marshal(job)
	if err != nil {
		return err
	}
	return kc.MetaKv.Save(ctx, key, string(value))
}

func (kc *Catalog) ListImportJobs(ctx context.Context) ([]*datapb.ImportJob, error) {
	jobs := make([]*datapb.ImportJob, 0)
	applyFn := func(key []byte, value []byte) error {
		job := &datapb.ImportJob{}
		err := proto.Unmarshal(value, job)
		if err != nil {
			return err
		}
		jobs = append(jobs, job)
		return nil
	}

	err := kc.MetaKv.WalkWithPrefix(ctx, ImportJobPrefix, kc.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

func (kc *Catalog) DropImportJob(ctx context.Context, jobID int64) error {
	key := buildImportJobKey(jobID)
	return kc.MetaKv.Remove(ctx, key)
}

func (kc *Catalog) SavePreImportTask(ctx context.Context, task *datapb.PreImportTask) error {
	key := buildPreImportTaskKey(task.GetTaskID())
	value, err := proto.Marshal(task)
	if err != nil {
		return err
	}
	return kc.MetaKv.Save(ctx, key, string(value))
}

func (kc *Catalog) ListPreImportTasks(ctx context.Context) ([]*datapb.PreImportTask, error) {
	tasks := make([]*datapb.PreImportTask, 0)

	applyFn := func(key []byte, value []byte) error {
		task := &datapb.PreImportTask{}
		err := proto.Unmarshal(value, task)
		if err != nil {
			return err
		}
		tasks = append(tasks, task)
		return nil
	}

	err := kc.MetaKv.WalkWithPrefix(ctx, PreImportTaskPrefix, kc.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

func (kc *Catalog) DropPreImportTask(ctx context.Context, taskID int64) error {
	key := buildPreImportTaskKey(taskID)
	return kc.MetaKv.Remove(ctx, key)
}

func (kc *Catalog) SaveImportTask(ctx context.Context, task *datapb.ImportTaskV2) error {
	key := buildImportTaskKey(task.GetTaskID())
	value, err := proto.Marshal(task)
	if err != nil {
		return err
	}
	return kc.MetaKv.Save(ctx, key, string(value))
}

func (kc *Catalog) ListImportTasks(ctx context.Context) ([]*datapb.ImportTaskV2, error) {
	tasks := make([]*datapb.ImportTaskV2, 0)

	applyFn := func(key []byte, value []byte) error {
		task := &datapb.ImportTaskV2{}
		err := proto.Unmarshal(value, task)
		if err != nil {
			return err
		}
		tasks = append(tasks, task)
		return nil
	}

	err := kc.MetaKv.WalkWithPrefix(ctx, ImportTaskPrefix, kc.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

func (kc *Catalog) DropImportTask(ctx context.Context, taskID int64) error {
	key := buildImportTaskKey(taskID)
	return kc.MetaKv.Remove(ctx, key)
}

// GcConfirm returns true if related collection/partition is not found.
// DataCoord will remove all the meta eventually after GC is finished.
func (kc *Catalog) GcConfirm(ctx context.Context, collectionID, partitionID typeutil.UniqueID) bool {
	prefix := buildCollectionPrefix(collectionID)
	if partitionID != common.AllPartitionsID {
		prefix = buildPartitionPrefix(collectionID, partitionID)
	}
	keys, values, err := kc.MetaKv.LoadWithPrefix(ctx, prefix)
	if err != nil {
		// error case can be regarded as not finished.
		return false
	}
	return len(keys) == 0 && len(values) == 0
}

func (kc *Catalog) ListCompactionTask(ctx context.Context) ([]*datapb.CompactionTask, error) {
	tasks := make([]*datapb.CompactionTask, 0)

	applyFn := func(key []byte, value []byte) error {
		info := &datapb.CompactionTask{}
		err := proto.Unmarshal(value, info)
		if err != nil {
			return err
		}
		tasks = append(tasks, info)
		return nil
	}

	err := kc.MetaKv.WalkWithPrefix(ctx, CompactionTaskPrefix, kc.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

func (kc *Catalog) SaveCompactionTask(ctx context.Context, coll *datapb.CompactionTask) error {
	if coll == nil {
		return nil
	}
	cloned := proto.Clone(coll).(*datapb.CompactionTask)
	k, v, err := buildCompactionTaskKV(cloned)
	if err != nil {
		return err
	}
	kvs := make(map[string]string)
	kvs[k] = v
	return kc.SaveByBatch(ctx, kvs)
}

func (kc *Catalog) DropCompactionTask(ctx context.Context, task *datapb.CompactionTask) error {
	key := buildCompactionTaskPath(task)
	return kc.MetaKv.Remove(ctx, key)
}

func (kc *Catalog) ListAnalyzeTasks(ctx context.Context) ([]*indexpb.AnalyzeTask, error) {
	tasks := make([]*indexpb.AnalyzeTask, 0)

	applyFn := func(key []byte, value []byte) error {
		task := &indexpb.AnalyzeTask{}
		err := proto.Unmarshal(value, task)
		if err != nil {
			return err
		}
		tasks = append(tasks, task)
		return nil
	}

	err := kc.MetaKv.WalkWithPrefix(ctx, AnalyzeTaskPrefix, kc.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

func (kc *Catalog) SaveAnalyzeTask(ctx context.Context, task *indexpb.AnalyzeTask) error {
	key := buildAnalyzeTaskKey(task.TaskID)

	value, err := proto.Marshal(task)
	if err != nil {
		return err
	}

	err = kc.MetaKv.Save(ctx, key, string(value))
	if err != nil {
		return err
	}
	return nil
}

func (kc *Catalog) DropAnalyzeTask(ctx context.Context, taskID typeutil.UniqueID) error {
	key := buildAnalyzeTaskKey(taskID)
	return kc.MetaKv.Remove(ctx, key)
}

func (kc *Catalog) ListPartitionStatsInfos(ctx context.Context) ([]*datapb.PartitionStatsInfo, error) {
	infos := make([]*datapb.PartitionStatsInfo, 0)

	applyFn := func(key []byte, value []byte) error {
		info := &datapb.PartitionStatsInfo{}
		err := proto.Unmarshal(value, info)
		if err != nil {
			return err
		}
		infos = append(infos, info)
		return nil
	}

	err := kc.MetaKv.WalkWithPrefix(ctx, PartitionStatsInfoPrefix, kc.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}
	return infos, nil
}

func (kc *Catalog) SavePartitionStatsInfo(ctx context.Context, coll *datapb.PartitionStatsInfo) error {
	if coll == nil {
		return nil
	}
	cloned := proto.Clone(coll).(*datapb.PartitionStatsInfo)
	k, v, err := buildPartitionStatsInfoKv(cloned)
	if err != nil {
		return err
	}
	kvs := make(map[string]string)
	kvs[k] = v
	return kc.SaveByBatch(ctx, kvs)
}

func (kc *Catalog) DropPartitionStatsInfo(ctx context.Context, info *datapb.PartitionStatsInfo) error {
	key := buildPartitionStatsInfoPath(info)
	return kc.MetaKv.Remove(ctx, key)
}

func (kc *Catalog) SaveCurrentPartitionStatsVersion(ctx context.Context, collID, partID int64, vChannel string, currentVersion int64) error {
	key := buildCurrentPartitionStatsVersionPath(collID, partID, vChannel)
	value := strconv.FormatInt(currentVersion, 10)
	return kc.MetaKv.Save(ctx, key, value)
}

func (kc *Catalog) GetCurrentPartitionStatsVersion(ctx context.Context, collID, partID int64, vChannel string) (int64, error) {
	key := buildCurrentPartitionStatsVersionPath(collID, partID, vChannel)
	valueStr, err := kc.MetaKv.Load(ctx, key)
	if err != nil {
		if errors.Is(err, merr.ErrIoKeyNotFound) {
			return 0, nil
		}
		return 0, err
	}

	return strconv.ParseInt(valueStr, 10, 64)
}

func (kc *Catalog) DropCurrentPartitionStatsVersion(ctx context.Context, collID, partID int64, vChannel string) error {
	key := buildCurrentPartitionStatsVersionPath(collID, partID, vChannel)
	return kc.MetaKv.Remove(ctx, key)
}

func (kc *Catalog) ListStatsTasks(ctx context.Context) ([]*indexpb.StatsTask, error) {
	tasks := make([]*indexpb.StatsTask, 0)

	applyFn := func(key []byte, value []byte) error {
		task := &indexpb.StatsTask{}
		err := proto.Unmarshal(value, task)
		if err != nil {
			return err
		}
		tasks = append(tasks, task)
		return nil
	}

	err := kc.MetaKv.WalkWithPrefix(ctx, StatsTaskPrefix, kc.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

func (kc *Catalog) SaveStatsTask(ctx context.Context, task *indexpb.StatsTask) error {
	key := buildStatsTaskKey(task.TaskID)
	value, err := proto.Marshal(task)
	if err != nil {
		return err
	}

	err = kc.MetaKv.Save(ctx, key, string(value))
	if err != nil {
		return err
	}
	return nil
}

func (kc *Catalog) DropStatsTask(ctx context.Context, taskID typeutil.UniqueID) error {
	key := buildStatsTaskKey(taskID)
	return kc.MetaKv.Remove(ctx, key)
}
