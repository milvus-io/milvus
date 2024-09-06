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
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/util"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

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

	// execute list segment meta
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

	err = kc.applyBinlogInfo(segments, insertLogs, deltaLogs, statsLogs)
	if err != nil {
		return nil, err
	}
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

	err = kc.MetaKv.WalkWithPrefix(logPathPrefix, paginationSize, applyFn)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (kc *Catalog) applyBinlogInfo(segments []*datapb.SegmentInfo, insertLogs, deltaLogs,
	statsLogs map[typeutil.UniqueID][]*datapb.FieldBinlog,
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
	}
	return nil
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

		binlogKvs, err := buildBinlogKvsWithLogID(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID(),
			cloneLogs(segment.GetBinlogs()), cloneLogs(segment.GetDeltalogs()), cloneLogs(segment.GetStatslogs()))
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
		kvs, err = buildBinlogKvsWithLogID(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID(), cloneLogs(segment.GetBinlogs()), cloneLogs(segment.GetDeltalogs()), cloneLogs(segment.GetStatslogs()))
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
	err := etcd.SaveByBatchWithLimit(kvs, util.MaxEtcdTxnNum, saveFn)
	if err != nil {
		log.Error("failed to save by batch", zap.Error(err))
		return err
	}
	return nil
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

	keys := []string{segKey, binlogPreix, deltalogPreix, statelogPreix}
	if err := kc.MetaKv.MultiSaveAndRemoveWithPrefix(nil, keys); err != nil {
		return err
	}

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
	return kc.SaveByBatch(kvs)
}

func (kc *Catalog) DropChannelCheckpoint(ctx context.Context, vChannel string) error {
	k := buildChannelCPKey(vChannel)
	return kc.MetaKv.Remove(k)
}

func (kc *Catalog) getBinlogsWithPrefix(binlogType storage.BinlogType, collectionID, partitionID,
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
	keys, values, err := kc.MetaKv.LoadWithPrefix(binlogPrefix)
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
			err = kc.MetaKv.MultiSave(kvs)
			if err != nil {
				return err
			}
			kvs = make(map[string]string)
		}
	}
	if len(kvs) != 0 {
		return kc.MetaKv.MultiSave(kvs)
	}
	return nil
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

func (kc *Catalog) SaveImportJob(job *datapb.ImportJob) error {
	key := buildImportJobKey(job.GetJobID())
	value, err := proto.Marshal(job)
	if err != nil {
		return err
	}
	return kc.MetaKv.Save(key, string(value))
}

func (kc *Catalog) ListImportJobs() ([]*datapb.ImportJob, error) {
	jobs := make([]*datapb.ImportJob, 0)
	_, values, err := kc.MetaKv.LoadWithPrefix(ImportJobPrefix)
	if err != nil {
		return nil, err
	}
	for _, value := range values {
		job := &datapb.ImportJob{}
		err = proto.Unmarshal([]byte(value), job)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func (kc *Catalog) DropImportJob(jobID int64) error {
	key := buildImportJobKey(jobID)
	return kc.MetaKv.Remove(key)
}

func (kc *Catalog) SavePreImportTask(task *datapb.PreImportTask) error {
	key := buildPreImportTaskKey(task.GetTaskID())
	value, err := proto.Marshal(task)
	if err != nil {
		return err
	}
	return kc.MetaKv.Save(key, string(value))
}

func (kc *Catalog) ListPreImportTasks() ([]*datapb.PreImportTask, error) {
	tasks := make([]*datapb.PreImportTask, 0)

	_, values, err := kc.MetaKv.LoadWithPrefix(PreImportTaskPrefix)
	if err != nil {
		return nil, err
	}
	for _, value := range values {
		task := &datapb.PreImportTask{}
		err = proto.Unmarshal([]byte(value), task)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}

	return tasks, nil
}

func (kc *Catalog) DropPreImportTask(taskID int64) error {
	key := buildPreImportTaskKey(taskID)
	return kc.MetaKv.Remove(key)
}

func (kc *Catalog) SaveImportTask(task *datapb.ImportTaskV2) error {
	key := buildImportTaskKey(task.GetTaskID())
	value, err := proto.Marshal(task)
	if err != nil {
		return err
	}
	return kc.MetaKv.Save(key, string(value))
}

func (kc *Catalog) ListImportTasks() ([]*datapb.ImportTaskV2, error) {
	tasks := make([]*datapb.ImportTaskV2, 0)

	_, values, err := kc.MetaKv.LoadWithPrefix(ImportTaskPrefix)
	if err != nil {
		return nil, err
	}
	for _, value := range values {
		task := &datapb.ImportTaskV2{}
		err = proto.Unmarshal([]byte(value), task)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func (kc *Catalog) DropImportTask(taskID int64) error {
	key := buildImportTaskKey(taskID)
	return kc.MetaKv.Remove(key)
}

// GcConfirm returns true if related collection/partition is not found.
// DataCoord will remove all the meta eventually after GC is finished.
func (kc *Catalog) GcConfirm(ctx context.Context, collectionID, partitionID typeutil.UniqueID) bool {
	prefix := buildCollectionPrefix(collectionID)
	if partitionID != common.AllPartitionsID {
		prefix = buildPartitionPrefix(collectionID, partitionID)
	}
	keys, values, err := kc.MetaKv.LoadWithPrefix(prefix)
	if err != nil {
		// error case can be regarded as not finished.
		return false
	}
	return len(keys) == 0 && len(values) == 0
}

func (kc *Catalog) ListCompactionTask(ctx context.Context) ([]*datapb.CompactionTask, error) {
	tasks := make([]*datapb.CompactionTask, 0)

	_, values, err := kc.MetaKv.LoadWithPrefix(CompactionTaskPrefix)
	if err != nil {
		return nil, err
	}
	for _, value := range values {
		info := &datapb.CompactionTask{}
		err = proto.Unmarshal([]byte(value), info)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, info)
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
	return kc.SaveByBatch(kvs)
}

func (kc *Catalog) DropCompactionTask(ctx context.Context, task *datapb.CompactionTask) error {
	key := buildCompactionTaskPath(task)
	return kc.MetaKv.Remove(key)
}

func (kc *Catalog) ListAnalyzeTasks(ctx context.Context) ([]*indexpb.AnalyzeTask, error) {
	tasks := make([]*indexpb.AnalyzeTask, 0)

	_, values, err := kc.MetaKv.LoadWithPrefix(AnalyzeTaskPrefix)
	if err != nil {
		return nil, err
	}
	for _, value := range values {
		task := &indexpb.AnalyzeTask{}
		err = proto.Unmarshal([]byte(value), task)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func (kc *Catalog) SaveAnalyzeTask(ctx context.Context, task *indexpb.AnalyzeTask) error {
	key := buildAnalyzeTaskKey(task.TaskID)

	value, err := proto.Marshal(task)
	if err != nil {
		return err
	}

	err = kc.MetaKv.Save(key, string(value))
	if err != nil {
		return err
	}
	return nil
}

func (kc *Catalog) DropAnalyzeTask(ctx context.Context, taskID typeutil.UniqueID) error {
	key := buildAnalyzeTaskKey(taskID)
	return kc.MetaKv.Remove(key)
}

func (kc *Catalog) ListPartitionStatsInfos(ctx context.Context) ([]*datapb.PartitionStatsInfo, error) {
	infos := make([]*datapb.PartitionStatsInfo, 0)

	_, values, err := kc.MetaKv.LoadWithPrefix(PartitionStatsInfoPrefix)
	if err != nil {
		return nil, err
	}
	for _, value := range values {
		info := &datapb.PartitionStatsInfo{}
		err = proto.Unmarshal([]byte(value), info)
		if err != nil {
			return nil, err
		}
		infos = append(infos, info)
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
	return kc.SaveByBatch(kvs)
}

func (kc *Catalog) DropPartitionStatsInfo(ctx context.Context, info *datapb.PartitionStatsInfo) error {
	key := buildPartitionStatsInfoPath(info)
	return kc.MetaKv.Remove(key)
}

func (kc *Catalog) SaveCurrentPartitionStatsVersion(ctx context.Context, collID, partID int64, vChannel string, currentVersion int64) error {
	key := buildCurrentPartitionStatsVersionPath(collID, partID, vChannel)
	value := strconv.FormatInt(currentVersion, 10)
	return kc.MetaKv.Save(key, value)
}

func (kc *Catalog) GetCurrentPartitionStatsVersion(ctx context.Context, collID, partID int64, vChannel string) (int64, error) {
	key := buildCurrentPartitionStatsVersionPath(collID, partID, vChannel)
	valueStr, err := kc.MetaKv.Load(key)
	if err != nil {
		return 0, err
	}

	return strconv.ParseInt(valueStr, 10, 64)
}

func (kc *Catalog) DropCurrentPartitionStatsVersion(ctx context.Context, collID, partID int64, vChannel string) error {
	key := buildCurrentPartitionStatsVersionPath(collID, partID, vChannel)
	return kc.MetaKv.Remove(key)
}

func (kc *Catalog) ListStatsTasks(ctx context.Context) ([]*indexpb.StatsTask, error) {
	tasks := make([]*indexpb.StatsTask, 0)
	_, values, err := kc.MetaKv.LoadWithPrefix(StatsTaskPrefix)
	if err != nil {
		return nil, err
	}

	for _, value := range values {
		task := &indexpb.StatsTask{}
		err = proto.Unmarshal([]byte(value), task)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func (kc *Catalog) SaveStatsTask(ctx context.Context, task *indexpb.StatsTask) error {
	key := buildStatsTaskKey(task.TaskID)
	value, err := proto.Marshal(task)
	if err != nil {
		return err
	}

	err = kc.MetaKv.Save(key, string(value))
	if err != nil {
		return err
	}
	return nil
}

func (kc *Catalog) DropStatsTask(ctx context.Context, taskID typeutil.UniqueID) error {
	key := buildStatsTaskKey(taskID)
	return kc.MetaKv.Remove(key)
}
