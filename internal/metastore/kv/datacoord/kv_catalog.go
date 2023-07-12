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

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/segmentutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var maxEtcdTxnNum = 128

var paginationSize = 2000

type Catalog struct {
	MetaKv               kv.MetaKv
	ChunkManagerRootPath string
	metaRootpath         string
}

func NewCatalog(MetaKv kv.MetaKv, chunkManagerRootPath string, metaRootpath string) *Catalog {
	return &Catalog{MetaKv: MetaKv, ChunkManagerRootPath: chunkManagerRootPath, metaRootpath: metaRootpath}
}

func (kc *Catalog) ListSegments(ctx context.Context) ([]*datapb.SegmentInfo, error) {
	group, _ := errgroup.WithContext(ctx)
	segments := make([]*datapb.SegmentInfo, 0)
	insertLogs := make(map[typeutil.UniqueID][]*datapb.FieldBinlog, 1)
	deltaLogs := make(map[typeutil.UniqueID][]*datapb.FieldBinlog, 1)
	statsLogs := make(map[typeutil.UniqueID][]*datapb.FieldBinlog, 1)

	executeFn := func(binlogType storage.BinlogType, result map[typeutil.UniqueID][]*datapb.FieldBinlog) {
		group.Go(func() error {
			ret, err := kc.listBinlogs(binlogType)
			if err != nil {
				return err
			}

			maps.Copy(result, ret)
			return nil
		})
	}

	//execute list segment meta
	executeFn(storage.InsertBinlog, insertLogs)
	executeFn(storage.DeleteBinlog, deltaLogs)
	executeFn(storage.StatsBinlog, statsLogs)
	group.Go(func() error {
		ret, err := kc.listSegments()
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

	kc.applyBinlogInfo(segments, insertLogs, deltaLogs, statsLogs)
	return segments, nil
}

func (kc *Catalog) listSegments() ([]*datapb.SegmentInfo, error) {
	segments := make([]*datapb.SegmentInfo, 0)

	applyFn := func(key []byte, value []byte) error {
		// due to SegmentStatslogPathPrefix has the same prefix with SegmentPrefix, so skip it.
		if strings.Contains(string(key), SegmentStatslogPathPrefix) {
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

	err := kc.MetaKv.WalkWithPrefix(SegmentPrefix+"/", paginationSize, applyFn)
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

func (kc *Catalog) listBinlogs(binlogType storage.BinlogType) (map[typeutil.UniqueID][]*datapb.FieldBinlog, error) {
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

		collectionID, partitionID, segmentID, err := kc.parseBinlogKey(string(key), prefixIdx)
		if err != nil {
			return fmt.Errorf("prefix:%s, %w", path.Join(kc.metaRootpath, logPathPrefix), err)
		}

		switch binlogType {
		case storage.InsertBinlog:
			fillLogPathByLogID(kc.ChunkManagerRootPath, storage.InsertBinlog, collectionID, partitionID, segmentID, fieldBinlog)
		case storage.DeleteBinlog:
			fillLogPathByLogID(kc.ChunkManagerRootPath, storage.DeleteBinlog, collectionID, partitionID, segmentID, fieldBinlog)
		case storage.StatsBinlog:
			fillLogPathByLogID(kc.ChunkManagerRootPath, storage.StatsBinlog, collectionID, partitionID, segmentID, fieldBinlog)
		}

		ret[segmentID] = append(ret[segmentID], fieldBinlog)
		return nil
	}

	err = kc.MetaKv.WalkWithPrefix(logPathPrefix, paginationSize, applyFn)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (kc *Catalog) applyBinlogInfo(segments []*datapb.SegmentInfo, insertLogs, deltaLogs,
	statsLogs map[typeutil.UniqueID][]*datapb.FieldBinlog) {
	for _, segmentInfo := range segments {
		if len(segmentInfo.Binlogs) == 0 {
			segmentInfo.Binlogs = insertLogs[segmentInfo.ID]
		}

		if len(segmentInfo.Deltalogs) == 0 {
			segmentInfo.Deltalogs = deltaLogs[segmentInfo.ID]
		}

		if len(segmentInfo.Statslogs) == 0 {
			segmentInfo.Statslogs = statsLogs[segmentInfo.ID]
		}
	}
}

func (kc *Catalog) AddSegment(ctx context.Context, segment *datapb.SegmentInfo) error {
	kvs, err := buildSegmentAndBinlogsKvs(segment)
	if err != nil {
		return err
	}
	return kc.MetaKv.MultiSave(kvs)
}

// LoadFromSegmentPath loads segment info from persistent storage by given segment path.
// # TESTING ONLY #
func (kc *Catalog) LoadFromSegmentPath(colID, partID, segID typeutil.UniqueID) (*datapb.SegmentInfo, error) {
	v, err := kc.MetaKv.Load(buildSegmentPath(colID, partID, segID))
	if err != nil {
		log.Error("(testing only) failed to load segment info by segment path")
		return nil, err
	}

	segInfo := &datapb.SegmentInfo{}
	err = proto.Unmarshal([]byte(v), segInfo)
	if err != nil {
		log.Error("(testing only) failed to unmarshall segment info")
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
		kc.collectMetrics(segment)

		// we don't persist binlog fields, but instead store binlogs as independent kvs
		cloned := proto.Clone(segment).(*datapb.SegmentInfo)
		resetBinlogFields(cloned)

		rowCount := segmentutil.CalcRowCountFromBinLog(segment)
		if cloned.GetNumOfRows() != rowCount {
			cloned.NumOfRows = rowCount
		}

		if segment.GetState() == commonpb.SegmentState_Dropped {
			binlogs, err := kc.handleDroppedSegment(segment)
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
		binlogKvs, err := buildBinlogKvsWithLogID(segment.GetCollectionID(), segment.GetPartitionID(),
			segment.GetID(), cloneLogs(b.Insertlogs), cloneLogs(b.Deltalogs), cloneLogs(b.Statslogs), len(segment.GetCompactionFrom()) > 0)
		if err != nil {
			return err
		}
		maps.Copy(kvs, binlogKvs)
	}

	return kc.SaveByBatch(kvs)
}

func (kc *Catalog) handleDroppedSegment(segment *datapb.SegmentInfo) (kvs map[string]string, err error) {
	var has bool
	has, err = kc.hasBinlogPrefix(segment)
	if err != nil {
		return
	}
	// To be compatible with previous implementation, we have to write binlogs on etcd for correct gc.
	if !has {
		kvs, err = buildBinlogKvsWithLogID(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID(), cloneLogs(segment.GetBinlogs()), cloneLogs(segment.GetDeltalogs()), cloneLogs(segment.GetStatslogs()), true)
		if err != nil {
			return
		}
	}
	return
}

func (kc *Catalog) SaveByBatch(kvs map[string]string) error {
	saveFn := func(partialKvs map[string]string) error {
		return kc.MetaKv.MultiSave(partialKvs)
	}
	if len(kvs) <= maxEtcdTxnNum {
		if err := etcd.SaveByBatch(kvs, saveFn); err != nil {
			log.Error("failed to save by batch", zap.Error(err))
			return err
		}
	} else {
		// Split kvs into multiple operations to avoid over-sized operations.
		// Also make sure kvs of the same segment are not split into different operations.
		batch := make(map[string]string)
		for k, v := range kvs {
			if len(batch) == maxEtcdTxnNum {
				if err := etcd.SaveByBatch(batch, saveFn); err != nil {
					log.Error("failed to save by batch", zap.Error(err))
					return err
				}
				maps.Clear(batch)
			}
			batch[k] = v
		}

		if len(batch) > 0 {
			if err := etcd.SaveByBatch(batch, saveFn); err != nil {
				log.Error("failed to save by batch", zap.Error(err))
				return err
			}
		}
	}
	return nil
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

func (kc *Catalog) collectMetrics(s *datapb.SegmentInfo) {
	statsFieldFn := func(fieldBinlogs []*datapb.FieldBinlog) int {
		cnt := 0
		for _, fbs := range fieldBinlogs {
			cnt += len(fbs.Binlogs)
		}
		return cnt
	}

	cnt := 0
	cnt += statsFieldFn(s.GetBinlogs())
	cnt += statsFieldFn(s.GetStatslogs())
	cnt += statsFieldFn(s.GetDeltalogs())

	metrics.DataCoordSegmentBinLogFileCount.
		WithLabelValues(fmt.Sprint(s.CollectionID), fmt.Sprint(s.GetID())).
		Set(float64(cnt))
}

func (kc *Catalog) hasBinlogPrefix(segment *datapb.SegmentInfo) (bool, error) {
	collectionID, partitionID, segmentID := segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID()
	prefix := buildFieldBinlogPathPrefix(collectionID, partitionID, segmentID)
	hasBinlogPrefix, err := kc.MetaKv.HasPrefix(prefix)
	if err != nil {
		return false, err
	}

	prefix = buildFieldDeltalogPathPrefix(collectionID, partitionID, segmentID)
	hasDeltaPrefix, err := kc.MetaKv.HasPrefix(prefix)
	if err != nil {
		return false, err
	}

	prefix = buildFieldStatslogPathPrefix(collectionID, partitionID, segmentID)
	hasStatsPrefix, err := kc.MetaKv.HasPrefix(prefix)
	if err != nil {
		return false, err
	}

	return hasBinlogPrefix || hasDeltaPrefix || hasStatsPrefix, nil
}

func (kc *Catalog) AlterSegmentsAndAddNewSegment(ctx context.Context, segments []*datapb.SegmentInfo, newSegment *datapb.SegmentInfo) error {
	kvs := make(map[string]string)

	for _, s := range segments {
		noBinlogsSegment, binlogs, deltalogs, statslogs := CloneSegmentWithExcludeBinlogs(s)
		// `s` is not mutated above. Also, `noBinlogsSegment` is a cloned version of `s`.
		segmentutil.ReCalcRowCount(s, noBinlogsSegment)
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
				binlogsKvs, err := buildBinlogKvsWithLogID(noBinlogsSegment.CollectionID, noBinlogsSegment.PartitionID, noBinlogsSegment.ID, binlogs, deltalogs, statslogs, true)
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
		if newSegment.NumOfRows > 0 {
			kc.collectMetrics(newSegment)
		}
	}
	return kc.MetaKv.MultiSave(kvs)
}

// RevertAlterSegmentsAndAddNewSegment reverts the metastore operation of AlterSegmentsAndAddNewSegment
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

	err := kc.MetaKv.MultiSaveAndRemove(kvs, removals)
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
		// `s` is not mutated above. Also, `noBinlogsSegment` is a cloned version of `s`.
		segmentutil.ReCalcRowCount(s, noBinlogsSegment)
		segBytes, err := proto.Marshal(noBinlogsSegment)
		if err != nil {
			return fmt.Errorf("failed to marshal segment: %d, err: %w", s.GetID(), err)
		}
		kvs[key] = string(segBytes)
	}

	saveFn := func(partialKvs map[string]string) error {
		return kc.MetaKv.MultiSave(partialKvs)
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
	if err := kc.MetaKv.MultiRemove(keys); err != nil {
		return err
	}

	metrics.CleanupDataCoordSegmentMetrics(segment.CollectionID, segment.ID)
	return nil
}

func (kc *Catalog) MarkChannelAdded(ctx context.Context, channel string) error {
	key := buildChannelRemovePath(channel)
	err := kc.MetaKv.Save(key, NonRemoveFlagTomestone)
	if err != nil {
		log.Error("failed to mark channel added", zap.String("channel", channel), zap.Error(err))
		return err
	}
	log.Info("NON remove flag tombstone added", zap.String("channel", channel))
	return nil
}

func (kc *Catalog) MarkChannelDeleted(ctx context.Context, channel string) error {
	key := buildChannelRemovePath(channel)
	err := kc.MetaKv.Save(key, RemoveFlagTomestone)
	if err != nil {
		log.Error("Failed to mark channel dropped", zap.String("channel", channel), zap.Error(err))
		return err
	}
	log.Info("remove flag tombstone added", zap.String("channel", channel))
	return nil
}

func (kc *Catalog) ShouldDropChannel(ctx context.Context, channel string) bool {
	key := buildChannelRemovePath(channel)
	v, err := kc.MetaKv.Load(key)
	if err != nil || v != RemoveFlagTomestone {
		return false
	}
	return true
}

func (kc *Catalog) ChannelExists(ctx context.Context, channel string) bool {
	key := buildChannelRemovePath(channel)
	v, err := kc.MetaKv.Load(key)
	return err == nil && v == NonRemoveFlagTomestone
}

// DropChannel removes channel remove flag after whole procedure is finished
func (kc *Catalog) DropChannel(ctx context.Context, channel string) error {
	key := buildChannelRemovePath(channel)
	log.Info("removing channel remove path", zap.String("channel", channel))
	return kc.MetaKv.Remove(key)
}

func (kc *Catalog) ListChannelCheckpoint(ctx context.Context) (map[string]*msgpb.MsgPosition, error) {
	keys, values, err := kc.MetaKv.LoadWithPrefix(ChannelCheckpointPrefix)
	if err != nil {
		return nil, err
	}

	channelCPs := make(map[string]*msgpb.MsgPosition)
	for i, key := range keys {
		value := values[i]
		channelCP := &msgpb.MsgPosition{}
		err = proto.Unmarshal([]byte(value), channelCP)
		if err != nil {
			log.Error("unmarshal channelCP failed when ListChannelCheckpoint", zap.Error(err))
			return nil, err
		}
		ss := strings.Split(key, "/")
		vChannel := ss[len(ss)-1]
		channelCPs[vChannel] = channelCP
	}

	return channelCPs, nil
}

func (kc *Catalog) SaveChannelCheckpoint(ctx context.Context, vChannel string, pos *msgpb.MsgPosition) error {
	k := buildChannelCPKey(vChannel)
	v, err := proto.Marshal(pos)
	if err != nil {
		return err
	}
	return kc.MetaKv.Save(k, string(v))
}

func (kc *Catalog) DropChannelCheckpoint(ctx context.Context, vChannel string) error {
	k := buildChannelCPKey(vChannel)
	return kc.MetaKv.Remove(k)
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
	keys, values, err := kc.MetaKv.LoadWithPrefix(binlogPrefix)
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

func (kc *Catalog) CreateIndex(ctx context.Context, index *model.Index) error {
	key := BuildIndexKey(index.CollectionID, index.IndexID)

	value, err := proto.Marshal(model.MarshalIndexModel(index))
	if err != nil {
		return err
	}

	err = kc.MetaKv.Save(key, string(value))
	if err != nil {
		return err
	}
	return nil
}

func (kc *Catalog) ListIndexes(ctx context.Context) ([]*model.Index, error) {
	_, values, err := kc.MetaKv.LoadWithPrefix(util.FieldIndexPrefix)
	if err != nil {
		log.Error("list index meta fail", zap.String("prefix", util.FieldIndexPrefix), zap.Error(err))
		return nil, err
	}

	indexes := make([]*model.Index, 0)
	for _, value := range values {
		meta := &indexpb.FieldIndex{}
		err = proto.Unmarshal([]byte(value), meta)
		if err != nil {
			log.Warn("unmarshal index info failed", zap.Error(err))
			return nil, err
		}

		indexes = append(indexes, model.UnmarshalIndexModel(meta))
	}

	return indexes, nil
}

func (kc *Catalog) AlterIndex(ctx context.Context, index *model.Index) error {
	return kc.CreateIndex(ctx, index)
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
	}
	return kc.MetaKv.MultiSave(kvs)
}

func (kc *Catalog) DropIndex(ctx context.Context, collID typeutil.UniqueID, dropIdxID typeutil.UniqueID) error {
	key := BuildIndexKey(collID, dropIdxID)

	err := kc.MetaKv.Remove(key)
	if err != nil {
		log.Error("drop collection index meta fail", zap.Int64("collectionID", collID),
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
	err = kc.MetaKv.Save(key, string(value))
	if err != nil {
		log.Error("failed to save segment index meta in etcd", zap.Int64("buildID", segIdx.BuildID),
			zap.Int64("segmentID", segIdx.SegmentID), zap.Error(err))
		return err
	}
	return nil
}

func (kc *Catalog) ListSegmentIndexes(ctx context.Context) ([]*model.SegmentIndex, error) {
	_, values, err := kc.MetaKv.LoadWithPrefix(util.SegmentIndexPrefix)
	if err != nil {
		log.Error("list segment index meta fail", zap.String("prefix", util.SegmentIndexPrefix), zap.Error(err))
		return nil, err
	}

	segIndexes := make([]*model.SegmentIndex, 0)
	for _, value := range values {
		segmentIndexInfo := &indexpb.SegmentIndex{}
		err = proto.Unmarshal([]byte(value), segmentIndexInfo)
		if err != nil {
			log.Warn("unmarshal segment index info failed", zap.Error(err))
			return segIndexes, err
		}

		segIndexes = append(segIndexes, model.UnmarshalSegmentIndexModel(segmentIndexInfo))
	}

	return segIndexes, nil
}

func (kc *Catalog) AlterSegmentIndex(ctx context.Context, segIdx *model.SegmentIndex) error {
	return kc.CreateSegmentIndex(ctx, segIdx)
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
	return kc.MetaKv.MultiSave(kvs)
}

func (kc *Catalog) DropSegmentIndex(ctx context.Context, collID, partID, segID, buildID typeutil.UniqueID) error {
	key := BuildSegmentIndexKey(collID, partID, segID, buildID)

	err := kc.MetaKv.Remove(key)
	if err != nil {
		log.Error("drop segment index meta fail", zap.Int64("buildID", buildID), zap.Error(err))
		return err
	}

	return nil
}

const allPartitionID = -1

// GcConfirm returns true if related collection/partition is not found.
// DataCoord will remove all the meta eventually after GC is finished.
func (kc *Catalog) GcConfirm(ctx context.Context, collectionID, partitionID typeutil.UniqueID) bool {
	prefix := buildCollectionPrefix(collectionID)
	if partitionID != allPartitionID {
		prefix = buildPartitionPrefix(collectionID, partitionID)
	}
	keys, values, err := kc.MetaKv.LoadWithPrefix(prefix)
	if err != nil {
		// error case can be regarded as not finished.
		return false
	}
	return len(keys) == 0 && len(values) == 0
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

func checkBinlogs(binlogType storage.BinlogType, segmentID typeutil.UniqueID, logs []*datapb.FieldBinlog) {
	check := func(getSegmentID func(logPath string) typeutil.UniqueID) {
		for _, fieldBinlog := range logs {
			for _, binlog := range fieldBinlog.Binlogs {
				if segmentID != getSegmentID(binlog.LogPath) {
					log.Panic("the segment path doesn't match the segment id", zap.Int64("segment_id", segmentID), zap.String("path", binlog.LogPath))
				}
			}
		}
	}
	switch binlogType {
	case storage.InsertBinlog:
		check(metautil.GetSegmentIDFromInsertLogPath)
	case storage.DeleteBinlog:
		check(metautil.GetSegmentIDFromDeltaLogPath)
	case storage.StatsBinlog:
		check(metautil.GetSegmentIDFromStatsLogPath)
	default:
		log.Panic("invalid binlog type")
	}
}

func hasSepcialStatslog(logs *datapb.FieldBinlog) bool {
	for _, statslog := range logs.Binlogs {
		_, logidx := path.Split(statslog.LogPath)
		if logidx == storage.CompoundStatsType.LogIdx() {
			return true
		}
	}
	return false
}

func buildBinlogKvsWithLogID(collectionID, partitionID, segmentID typeutil.UniqueID,
	binlogs, deltalogs, statslogs []*datapb.FieldBinlog, ignoreNumberCheck bool) (map[string]string, error) {

	checkBinlogs(storage.InsertBinlog, segmentID, binlogs)
	checkBinlogs(storage.DeleteBinlog, segmentID, deltalogs)
	checkBinlogs(storage.StatsBinlog, segmentID, statslogs)
	// check stats log and bin log size match
	// num of stats log may one more than num of binlogs if segment flushed and merged stats log
	if !ignoreNumberCheck && len(binlogs) != 0 && len(statslogs) != 0 && !hasSepcialStatslog(statslogs[0]) {
		if len(binlogs[0].GetBinlogs()) != len(statslogs[0].GetBinlogs()) {
			log.Warn("find invalid segment while bin log size didn't match stat log size",
				zap.Int64("collection", collectionID),
				zap.Int64("partition", partitionID),
				zap.Int64("segment", segmentID),
				zap.Any("binlogs", binlogs),
				zap.Any("stats", statslogs),
				zap.Any("delta", deltalogs),
			)
			return nil, fmt.Errorf("segment can not be saved because of binlog "+
				"file not match stat log number: collection %v, segment %v", collectionID, segmentID)
		}
	}

	fillLogIDByLogPath(binlogs, deltalogs, statslogs)
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

	// compacted segment has only one statslog
	ignore := (len(segment.GetCompactionFrom()) > 0)
	// save binlogs separately
	kvs, err := buildBinlogKvsWithLogID(noBinlogsSegment.CollectionID, noBinlogsSegment.PartitionID, noBinlogsSegment.ID, binlogs, deltalogs, statslogs, ignore)
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

// buildFlushedSegmentPath common logic mapping segment info to corresponding key of IndexCoord in kv store
// TODO @cai.zhang: remove this
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
