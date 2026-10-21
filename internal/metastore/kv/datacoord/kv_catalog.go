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

	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/etcd"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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

func (kc *Catalog) segmentCodec() SegmentCodec {
	return NewSegmentCodec(kc.ChunkManagerRootPath, kc.MetaKv.HasPrefix)
}

func (kc *Catalog) ListSegments(ctx context.Context, collectionID int64) ([]*datapb.SegmentInfo, error) {
	group, _ := errgroup.WithContext(ctx)
	segments := make([]*datapb.SegmentInfo, 0)
	binlogs := make([]map[typeutil.UniqueID][]*datapb.FieldBinlog, len(segmentBinlogFamilies))
	for i, family := range segmentBinlogFamilies {
		group.Go(func() error {
			ret, err := kc.listBinlogs(ctx, fmt.Sprintf("%s/%d", family.prefix, collectionID))
			binlogs[i] = ret
			return err
		})
	}

	// execute list segment meta
	group.Go(func() error {
		ret, err := kc.listSegments(ctx, collectionID)
		if err != nil {
			return err
		}
		segments = append(segments, ret...)
		return nil
	})

	if err := group.Wait(); err != nil {
		return nil, err
	}
	if err := FillSegmentBinlogs(segments, binlogs); err != nil {
		return nil, err
	}
	return segments, nil
}

func (kc *Catalog) listSegments(ctx context.Context, collectionID int64) ([]*datapb.SegmentInfo, error) {
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

		// Restore full paths for text index logs (compatible with old version).
		// JsonKeyStats.Files intentionally remain relative in SegmentInfo; callers
		// that need full paths should rebuild them with the segment's V2/V3 basePath.
		kc.segmentCodec().DecodeRecord(segmentInfo)

		segments = append(segments, segmentInfo)
		return nil
	}

	err := kc.MetaKv.WalkWithPrefix(ctx, buildCollectionPrefix(collectionID), kc.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}

	return segments, nil
}

func (kc *Catalog) listBinlogs(ctx context.Context, logPathPrefix string) (map[typeutil.UniqueID][]*datapb.FieldBinlog, error) {
	ret := make(map[typeutil.UniqueID][]*datapb.FieldBinlog)
	applyFn := func(key []byte, value []byte) error {
		segmentID, fieldBinlog, err := DecodeFieldBinlog(string(key), value)
		if err != nil {
			return merr.Wrapf(err, "prefix:%s", kc.metaRootpath+"/"+logPathPrefix)
		}
		ret[segmentID] = append(ret[segmentID], fieldBinlog)
		return nil
	}
	if err := kc.MetaKv.WalkWithPrefix(ctx, logPathPrefix, kc.paginationSize, applyFn); err != nil {
		return nil, err
	}
	return ret, nil
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
		mlog.Error(ctx, "(testing only) failed to load segment info by segment path")
		return nil, err
	}

	segInfo := &datapb.SegmentInfo{}
	err = proto.Unmarshal([]byte(v), segInfo)
	if err != nil {
		mlog.Error(ctx, "(testing only) failed to unmarshall segment info")
		return nil, err
	}

	return segInfo, nil
}

func (kc *Catalog) AlterSegments(ctx context.Context, segments []*datapb.SegmentInfo, binlogs ...metastore.BinlogsIncrement) error {
	if len(segments) == 0 {
		return nil
	}
	kvs, removals, err := kc.segmentCodec().alterSegmentsKvs(ctx, segments, binlogs)
	if err != nil {
		return err
	}

	if err := kc.SaveByBatch(ctx, kvs); err != nil {
		return err
	}
	// Explicit removal is required: AlterSegments persists binlogs as
	// independent per-FieldID KVs and listBinlogs rebuilds them via a prefix
	// scan on restart. An operator that structurally drops a FieldBinlog
	// from segment.Binlogs (e.g. when all ChildFields of a column group are
	// claimed by a backfill commit) must also delete the orphan KV, otherwise
	// the stripped group resurrects on the next datacoord start.
	if len(removals) > 0 {
		if err := kc.MetaKv.MultiSaveAndRemove(ctx, nil, removals); err != nil {
			return err
		}
	}
	return nil
}

func (kc *Catalog) SaveByBatch(ctx context.Context, kvs map[string]string) error {
	saveFn := func(partialKvs map[string]string) error {
		return kc.MetaKv.MultiSave(ctx, partialKvs)
	}
	maxTxnNum := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()
	err := etcd.SaveByBatchWithLimit(kvs, maxTxnNum, saveFn)
	if err != nil {
		mlog.Error(ctx, "failed to save by batch", mlog.Err(err))
		return err
	}
	return nil
}

// buildDroppedSegmentKvs computes the record-only key/value writes for a
// batch of segments. Factored out so both Catalog.SaveDroppedSegmentsInBatch
// (batched MultiSave) and Catalog.Update's ActionUpdate segment path
// (accumulated into a single txn.Builder) apply the exact same kv encoding:
// segment-only KVs (no binlog KVs, no handleDroppedSegment compat writes) -
// deliberately distinct from buildAlterSegmentsKvs, which additionally
// persists binlog KVs for the ActionAdd path.
func buildDroppedSegmentKvs(segments []*datapb.SegmentInfo) (map[string]string, error) {
	kvs := make(map[string]string)
	for _, s := range segments {
		key, value, err := buildSegmentKv(persistedRecord(s))
		if err != nil {
			return nil, err
		}
		kvs[key] = value
	}
	return kvs, nil
}

func (kc *Catalog) SaveDroppedSegmentsInBatch(ctx context.Context, segments []*datapb.SegmentInfo) error {
	if len(segments) == 0 {
		return nil
	}

	kvs, err := buildDroppedSegmentKvs(segments)
	if err != nil {
		return err
	}

	saveFn := func(partialKvs map[string]string) error {
		return kc.MetaKv.MultiSave(ctx, partialKvs)
	}
	maxTxnNum := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum.GetAsInt()
	if err := etcd.SaveByBatchWithLimit(kvs, maxTxnNum, saveFn); err != nil {
		return err
	}

	return nil
}

func (kc *Catalog) DropSegment(ctx context.Context, segment *datapb.SegmentInfo) error {
	segKey := buildSegmentPath(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())
	keys := append([]string{segKey}, SegmentBinlogPrefixesOf(segment.GetCollectionID(), segment.GetPartitionID(), segment.GetID())...)
	if err := kc.MetaKv.MultiSaveAndRemoveWithPrefix(ctx, nil, keys); err != nil {
		return err
	}

	return nil
}

func (kc *Catalog) SaveDataView(ctx context.Context, dataView *viewpb.DataViewOfCollection) error {
	key := buildDataViewVersionKey(
		dataView.GetCollectionId(),
		dataView.GetDataVersion().GetStreamingVersion(),
		dataView.GetDataVersion().GetCompactVersion(),
	)
	value, err := proto.Marshal(dataView)
	if err != nil {
		return err
	}
	return kc.MetaKv.Save(ctx, key, string(value))
}

func (kc *Catalog) ListDataViews(ctx context.Context, collectionID int64) ([]*viewpb.DataViewOfCollection, error) {
	return kc.listDataViewsWithPrefix(ctx, buildDataViewVersionPrefix(collectionID))
}

func (kc *Catalog) ListAllDataViews(ctx context.Context) ([]*viewpb.DataViewOfCollection, error) {
	return kc.listDataViewsWithPrefix(ctx, DataViewPrefix+"/")
}

func (kc *Catalog) listDataViewsWithPrefix(ctx context.Context, prefix string) ([]*viewpb.DataViewOfCollection, error) {
	dataViews := make([]*viewpb.DataViewOfCollection, 0)
	applyFn := func(key []byte, value []byte) error {
		dataView := &viewpb.DataViewOfCollection{}
		if err := proto.Unmarshal(value, dataView); err != nil {
			// Skip one corrupt value instead of aborting the whole walk: a
			// single unmarshalable key would otherwise fail ListAllDataViews
			// on every attempt and block Coordinator startup (the manager's
			// per-view validation can only run on values that unmarshal).
			mlog.Warn(context.TODO(), "skip unmarshalable DataView key during ListAllDataViews",
				mlog.String("key", string(key)),
				mlog.Err(err))
			return nil
		}
		dataViews = append(dataViews, dataView)
		return nil
	}

	if err := kc.MetaKv.WalkWithPrefix(ctx, prefix, kc.paginationSize, applyFn); err != nil {
		return nil, err
	}
	return dataViews, nil
}

func (kc *Catalog) DropDataView(ctx context.Context, collectionID int64, dataVersion *viewpb.DataVersion) error {
	return kc.MetaKv.Remove(ctx, buildDataViewVersionKey(
		collectionID,
		dataVersion.GetStreamingVersion(),
		dataVersion.GetCompactVersion(),
	))
}

func (kc *Catalog) DropDataViews(ctx context.Context, collectionID int64) error {
	return kc.MetaKv.RemoveWithPrefix(ctx, buildDataViewVersionPrefix(collectionID))
}

func (kc *Catalog) MarkChannelAdded(ctx context.Context, channel string) error {
	key := buildChannelRemovePath(channel)
	err := kc.MetaKv.Save(ctx, key, NonRemoveFlagTomestone)
	if err != nil {
		mlog.Error(ctx, "failed to mark channel added", mlog.String("channel", channel), mlog.Err(err))
		return err
	}
	mlog.Info(ctx, "NON remove flag tombstone added", mlog.String("channel", channel))
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

// ChannelExists reports whether the channel marker is active. Missing keys are
// treated as absent; other lookup failures are returned to the caller.
func (kc *Catalog) ChannelExists(ctx context.Context, channel string) (bool, error) {
	key := buildChannelRemovePath(channel)
	v, err := kc.MetaKv.Load(ctx, key)
	if errors.Is(err, merr.ErrIoKeyNotFound) {
		return false, nil
	}
	if err != nil {
		return false, merr.Wrapf(err, "failed to load channel marker %s", channel)
	}
	return v == NonRemoveFlagTomestone, nil
}

// DropChannel removes channel remove flag after whole procedure is finished
func (kc *Catalog) DropChannel(ctx context.Context, channel string) error {
	key := buildChannelRemovePath(channel)
	mlog.Info(ctx, "removing channel remove path", mlog.String("channel", channel))
	return kc.MetaKv.Remove(ctx, key)
}

func (kc *Catalog) ListChannelCheckpoint(ctx context.Context) (map[string]*msgpb.MsgPosition, error) {
	channelCPs := make(map[string]*msgpb.MsgPosition)
	applyFn := func(key []byte, value []byte) error {
		channelCP := &msgpb.MsgPosition{}
		err := proto.Unmarshal(value, channelCP)
		if err != nil {
			mlog.Error(ctx, "unmarshal channelCP failed when ListChannelCheckpoint", mlog.Err(err))
			return err
		}
		ss := strings.Split(string(key), "/")
		vChannel := ss[len(ss)-1]
		channelCPs[vChannel] = channelCP
		return nil
	}

	err := kc.MetaKv.WalkWithPrefix(ctx, ChannelCheckpointPrefix+"/", kc.paginationSize, applyFn)
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
		return nil, nil, merr.WrapErrServiceInternalMsg("invalid binlog type: %d", binlogType)
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
			mlog.Warn(ctx, "unmarshal index info failed", mlog.Err(err))
			return err
		}

		indexes = append(indexes, model.UnmarshalIndexModel(meta))
		return nil
	}

	err := kc.MetaKv.WalkWithPrefix(ctx, util.FieldIndexPrefix+"/", kc.paginationSize, applyFn)
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
		mlog.Error(ctx, "drop collection index meta fail", mlog.FieldCollectionID(collID),
			mlog.FieldIndexID(dropIdxID), mlog.Err(err))
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
		mlog.Error(ctx, "failed to save segment index meta in etcd", mlog.FieldBuildID(segIdx.BuildID),
			mlog.FieldSegmentID(segIdx.SegmentID), mlog.Err(err))
		return err
	}
	return nil
}

func (kc *Catalog) ListSegmentIndexes(ctx context.Context, collectionID int64) ([]*model.SegmentIndex, error) {
	segIndexes := make([]*model.SegmentIndex, 0)
	applyFn := func(key []byte, value []byte) error {
		segmentIndexInfo := &indexpb.SegmentIndex{}
		err := proto.Unmarshal(value, segmentIndexInfo)
		if err != nil {
			mlog.Warn(ctx, "unmarshal segment index info failed", mlog.Err(err))
			return err
		}

		segIndexes = append(segIndexes, model.UnmarshalSegmentIndexModel(segmentIndexInfo))
		return nil
	}

	prefix := buildSegmentIndexCollectionPrefix(collectionID)
	err := kc.MetaKv.WalkWithPrefix(ctx, prefix, kc.paginationSize, applyFn)
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
		mlog.Error(ctx, "drop segment index meta fail", mlog.FieldBuildID(buildID), mlog.Err(err))
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

	err := kc.MetaKv.WalkWithPrefix(ctx, ImportJobPrefix+"/", kc.paginationSize, applyFn)
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

	err := kc.MetaKv.WalkWithPrefix(ctx, PreImportTaskPrefix+"/", kc.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

func (kc *Catalog) DropPreImportTask(ctx context.Context, taskID int64) error {
	key := buildPreImportTaskKey(taskID)
	return kc.MetaKv.Remove(ctx, key)
}

func (kc *Catalog) SavePreImportTaskV3(ctx context.Context, task *datapb.PreImportTaskV3) error {
	key := buildPreImportTaskV3Key(task.GetTaskId())
	value, err := proto.Marshal(task)
	if err != nil {
		return err
	}
	return kc.MetaKv.Save(ctx, key, string(value))
}

func (kc *Catalog) ListPreImportTasksV3(ctx context.Context) ([]*datapb.PreImportTaskV3, error) {
	tasks := make([]*datapb.PreImportTaskV3, 0)

	applyFn := func(key []byte, value []byte) error {
		task := &datapb.PreImportTaskV3{}
		err := proto.Unmarshal(value, task)
		if err != nil {
			return err
		}
		tasks = append(tasks, task)
		return nil
	}

	err := kc.MetaKv.WalkWithPrefix(ctx, PreImportTaskV3Prefix+"/", kc.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

func (kc *Catalog) DropPreImportTaskV3(ctx context.Context, taskID int64) error {
	key := buildPreImportTaskV3Key(taskID)
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

	err := kc.MetaKv.WalkWithPrefix(ctx, ImportTaskPrefix+"/", kc.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

func (kc *Catalog) DropImportTask(ctx context.Context, taskID int64) error {
	key := buildImportTaskKey(taskID)
	return kc.MetaKv.Remove(ctx, key)
}

func (kc *Catalog) SaveReshardTask(ctx context.Context, task *datapb.ReshardTask) error {
	key := buildReshardTaskKey(task.GetTaskId())
	value, err := proto.Marshal(task)
	if err != nil {
		return err
	}
	return kc.MetaKv.Save(ctx, key, string(value))
}

func (kc *Catalog) ListReshardTasks(ctx context.Context) ([]*datapb.ReshardTask, error) {
	tasks := make([]*datapb.ReshardTask, 0)
	err := kc.MetaKv.WalkWithPrefix(ctx, ReshardTaskPrefix+"/", kc.paginationSize, func(_ []byte, value []byte) error {
		task := &datapb.ReshardTask{}
		if err := proto.Unmarshal(value, task); err != nil {
			return err
		}
		tasks = append(tasks, task)
		return nil
	})
	return tasks, err
}

func (kc *Catalog) DropReshardTask(ctx context.Context, taskID int64) error {
	return kc.MetaKv.Remove(ctx, buildReshardTaskKey(taskID))
}

func (kc *Catalog) SaveImportTaskV3(ctx context.Context, task *datapb.ImportTaskV3) error {
	key := buildImportTaskV3Key(task.GetTaskId())
	value, err := proto.Marshal(task)
	if err != nil {
		return err
	}
	return kc.MetaKv.Save(ctx, key, string(value))
}

func (kc *Catalog) ListImportTasksV3(ctx context.Context) ([]*datapb.ImportTaskV3, error) {
	tasks := make([]*datapb.ImportTaskV3, 0)
	err := kc.MetaKv.WalkWithPrefix(ctx, ImportTaskV3Prefix+"/", kc.paginationSize, func(_ []byte, value []byte) error {
		task := &datapb.ImportTaskV3{}
		if err := proto.Unmarshal(value, task); err != nil {
			return err
		}
		tasks = append(tasks, task)
		return nil
	})
	return tasks, err
}

func (kc *Catalog) DropImportTaskV3(ctx context.Context, taskID int64) error {
	return kc.MetaKv.Remove(ctx, buildImportTaskV3Key(taskID))
}

func (kc *Catalog) SaveCopySegmentJob(ctx context.Context, job *datapb.CopySegmentJob) error {
	key := buildCopySegmentJobKey(job.GetJobId())
	value, err := proto.Marshal(job)
	if err != nil {
		return err
	}
	return kc.MetaKv.Save(ctx, key, string(value))
}

func (kc *Catalog) ListCopySegmentJobs(ctx context.Context) ([]*datapb.CopySegmentJob, error) {
	jobs := make([]*datapb.CopySegmentJob, 0)
	applyFn := func(key []byte, value []byte) error {
		job := &datapb.CopySegmentJob{}
		err := proto.Unmarshal(value, job)
		if err != nil {
			return err
		}
		jobs = append(jobs, job)
		return nil
	}

	err := kc.MetaKv.WalkWithPrefix(ctx, CopySegmentJobPrefix+"/", kc.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

func (kc *Catalog) DropCopySegmentJob(ctx context.Context, jobID int64) error {
	key := buildCopySegmentJobKey(jobID)
	return kc.MetaKv.Remove(ctx, key)
}

func (kc *Catalog) SaveCopySegmentTask(ctx context.Context, task *datapb.CopySegmentTask) error {
	key := buildCopySegmentTaskKey(task.GetTaskId())
	value, err := proto.Marshal(task)
	if err != nil {
		return err
	}
	return kc.MetaKv.Save(ctx, key, string(value))
}

func (kc *Catalog) SaveCopySegmentTasksBatch(ctx context.Context, tasks []*datapb.CopySegmentTask) error {
	if len(tasks) == 0 {
		return nil
	}

	kvs := make(map[string]string, len(tasks))
	for _, task := range tasks {
		key := buildCopySegmentTaskKey(task.GetTaskId())
		value, err := proto.Marshal(task)
		if err != nil {
			return err
		}
		kvs[key] = string(value)
	}

	return kc.MetaKv.MultiSave(ctx, kvs)
}

func (kc *Catalog) ListCopySegmentTasks(ctx context.Context) ([]*datapb.CopySegmentTask, error) {
	tasks := make([]*datapb.CopySegmentTask, 0)

	applyFn := func(key []byte, value []byte) error {
		task := &datapb.CopySegmentTask{}
		err := proto.Unmarshal(value, task)
		if err != nil {
			return err
		}
		tasks = append(tasks, task)
		return nil
	}

	err := kc.MetaKv.WalkWithPrefix(ctx, CopySegmentTaskPrefix+"/", kc.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

func (kc *Catalog) DropCopySegmentTask(ctx context.Context, taskID int64) error {
	key := buildCopySegmentTaskKey(taskID)
	return kc.MetaKv.Remove(ctx, key)
}

// GcConfirm returns true if related collection/partition is not found.
// DataCoord will remove all the meta eventually after GC is finished.
func (kc *Catalog) GcConfirm(ctx context.Context, collectionID, partitionID typeutil.UniqueID) bool {
	prefix := buildCollectionPrefix(collectionID)
	if partitionID != common.AllPartitionsID {
		prefix = buildPartitionPrefix(collectionID, partitionID)
	}
	exists, err := kc.MetaKv.HasPrefix(ctx, prefix)
	if err != nil {
		// error case can be regarded as not finished.
		return false
	}
	return !exists
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

	err := kc.MetaKv.WalkWithPrefix(ctx, CompactionTaskPrefix+"/", kc.paginationSize, applyFn)
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

func (kc *Catalog) ListCompactionTargets(ctx context.Context) ([]*datapb.CompactionTarget, error) {
	records := make([]*datapb.CompactionTarget, 0)

	applyFn := func(key []byte, value []byte) error {
		record := &datapb.CompactionTarget{}
		if err := proto.Unmarshal(value, record); err != nil {
			return err
		}
		records = append(records, record)
		return nil
	}

	err := kc.MetaKv.WalkWithPrefix(ctx, CompactionTargetPrefix+"/", kc.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}
	return records, nil
}

func (kc *Catalog) SaveCompactionTarget(ctx context.Context, record *datapb.CompactionTarget) error {
	if record == nil {
		return nil
	}
	cloned := proto.Clone(record).(*datapb.CompactionTarget)
	key, value, err := buildCompactionTargetKV(cloned)
	if err != nil {
		return err
	}
	return kc.SaveByBatch(ctx, map[string]string{key: value})
}

func (kc *Catalog) UpdateCompactionTargetState(ctx context.Context, targetID int64, state datapb.TargetState, inactivatedAtTS uint64) error {
	key := buildCompactionTargetPath(targetID)
	value, err := kc.MetaKv.Load(ctx, key)
	if err != nil {
		return err
	}

	record := &datapb.CompactionTarget{}
	if err := proto.Unmarshal([]byte(value), record); err != nil {
		return err
	}
	record.State = state
	if state == datapb.TargetState_TARGET_STATE_INACTIVE {
		record.InactivatedAtTS = inactivatedAtTS
	} else {
		record.InactivatedAtTS = 0
	}
	return kc.SaveCompactionTarget(ctx, record)
}

func (kc *Catalog) DropCompactionTarget(ctx context.Context, record *datapb.CompactionTarget) error {
	if record == nil {
		return nil
	}
	key := buildCompactionTargetPath(record.GetTargetID())
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

	err := kc.MetaKv.WalkWithPrefix(ctx, AnalyzeTaskPrefix+"/", kc.paginationSize, applyFn)
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

	err := kc.MetaKv.WalkWithPrefix(ctx, PartitionStatsInfoPrefix+"/", kc.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}
	return infos, nil
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

	err := kc.MetaKv.WalkWithPrefix(ctx, StatsTaskPrefix+"/", kc.paginationSize, applyFn)
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

// ListExternalCollectionRefreshJobs lists all external collection refresh jobs from etcd
func (kc *Catalog) ListExternalCollectionRefreshJobs(ctx context.Context) ([]*datapb.ExternalCollectionRefreshJob, error) {
	jobs := make([]*datapb.ExternalCollectionRefreshJob, 0)
	applyFn := func(key []byte, value []byte) error {
		job := &datapb.ExternalCollectionRefreshJob{}
		if err := proto.Unmarshal(value, job); err != nil {
			mlog.Warn(ctx, "failed to unmarshal external collection refresh job", mlog.Err(err))
			return err
		}
		jobs = append(jobs, job)
		return nil
	}
	err := kc.MetaKv.WalkWithPrefix(ctx, ExternalCollectionRefreshJobPrefix+"/", kc.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}
	return jobs, nil
}

// SaveExternalCollectionRefreshJob saves an external collection refresh job to etcd
func (kc *Catalog) SaveExternalCollectionRefreshJob(ctx context.Context, job *datapb.ExternalCollectionRefreshJob) error {
	key := buildExternalCollectionRefreshJobKey(job.JobId)
	value, err := proto.Marshal(job)
	if err != nil {
		return err
	}
	return kc.MetaKv.Save(ctx, key, string(value))
}

// ListExternalCollectionRefreshTasks lists all external collection refresh tasks from etcd
func (kc *Catalog) ListExternalCollectionRefreshTasks(ctx context.Context) ([]*datapb.ExternalCollectionRefreshTask, error) {
	tasks := make([]*datapb.ExternalCollectionRefreshTask, 0)
	applyFn := func(key []byte, value []byte) error {
		task := &datapb.ExternalCollectionRefreshTask{}
		if err := proto.Unmarshal(value, task); err != nil {
			mlog.Warn(ctx, "failed to unmarshal external collection refresh task", mlog.Err(err))
			return err
		}
		tasks = append(tasks, task)
		return nil
	}
	err := kc.MetaKv.WalkWithPrefix(ctx, ExternalCollectionRefreshTaskPrefix+"/", kc.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

// SaveExternalCollectionRefreshTask saves an external collection refresh task to etcd
func (kc *Catalog) SaveExternalCollectionRefreshTask(ctx context.Context, task *datapb.ExternalCollectionRefreshTask) error {
	key := buildExternalCollectionRefreshTaskKey(task.TaskId)
	value, err := proto.Marshal(task)
	if err != nil {
		return err
	}
	return kc.MetaKv.Save(ctx, key, string(value))
}

func (kc *Catalog) SaveSnapshot(ctx context.Context, snapshot *datapb.SnapshotInfo) error {
	key := buildSnapshotKey(snapshot.GetCollectionId(), snapshot.GetId())
	value, err := proto.Marshal(snapshot)
	if err != nil {
		return err
	}
	return kc.MetaKv.Save(ctx, key, string(value))
}

func (kc *Catalog) DropSnapshot(ctx context.Context, collectionID int64, snapshotID int64) error {
	key := buildSnapshotKey(collectionID, snapshotID)
	return kc.MetaKv.Remove(ctx, key)
}

func (kc *Catalog) ListSnapshots(ctx context.Context) ([]*datapb.SnapshotInfo, error) {
	snapshots := make([]*datapb.SnapshotInfo, 0)

	applyFn := func(key []byte, value []byte) error {
		snapshot := &datapb.SnapshotInfo{}
		err := proto.Unmarshal(value, snapshot)
		if err != nil {
			return err
		}
		snapshots = append(snapshots, snapshot)
		return nil
	}

	err := kc.MetaKv.WalkWithPrefix(ctx, SnapshotPrefix+"/", kc.paginationSize, applyFn)
	if err != nil {
		return nil, err
	}
	return snapshots, nil
}

func (kc *Catalog) SaveExportSnapshotJob(ctx context.Context, job *datapb.ExportSnapshotJob) error {
	value, err := proto.Marshal(job)
	if err != nil {
		return err
	}
	return kc.MetaKv.Save(ctx, buildExportSnapshotJobKey(job.GetJobId()), string(value))
}

func (kc *Catalog) ListExportSnapshotJobs(ctx context.Context) ([]*datapb.ExportSnapshotJob, error) {
	jobs := make([]*datapb.ExportSnapshotJob, 0)
	applyFn := func(key []byte, value []byte) error {
		job := &datapb.ExportSnapshotJob{}
		if err := proto.Unmarshal(value, job); err != nil {
			return err
		}
		jobs = append(jobs, job)
		return nil
	}
	if err := kc.MetaKv.WalkWithPrefix(ctx, ExportSnapshotJobPrefix+"/", kc.paginationSize, applyFn); err != nil {
		return nil, err
	}
	return jobs, nil
}

func (kc *Catalog) DropExportSnapshotJob(ctx context.Context, jobID int64) error {
	return kc.MetaKv.Remove(ctx, buildExportSnapshotJobKey(jobID))
}

// ListSegmentChangeGroups lists all segment change groups from etcd.
//
// A malformed value is an ERROR, not a skip: unlike DataView snapshots (which
// are reconstructible from the SegmentMeta projection), a group record is the
// sole owner of its staged members' visibility — SegmentInfo has no
// change_group_id field yet (design F4) — so silently skipping a bad key
// orphans its members (IsInvisible=true with no owner, never published or
// reclaimed). This matches the other 13 List methods in this file, which all
// propagate decode errors; recovery can then fail closed on corruption.
func (kc *Catalog) ListSegmentChangeGroups(ctx context.Context) ([]*model.SegmentChangeGroup, error) {
	groups := make([]*model.SegmentChangeGroup, 0)
	applyFn := func(key []byte, value []byte) error {
		group, err := model.UnmarshalSegmentChangeGroup(value)
		if err != nil {
			// C36: identify the offending etcd key so an operator facing a
			// fail-closed startup can locate and remove it; Validate errors on
			// a corrupt/zero record report GroupID 0, which is not searchable.
			return merr.Wrap(err, "failed to decode a persisted segment change group at key "+string(key))
		}
		groups = append(groups, group)
		return nil
	}
	if err := kc.MetaKv.WalkWithPrefix(ctx, SegmentChangeGroupPrefix+"/", kc.paginationSize, applyFn); err != nil {
		return nil, err
	}
	return groups, nil
}

// DropSegmentChangeGroups removes every segment change group of one collection.
// Used by collection drop; the per-collection prefix delete runs under the
// caller's collection lifecycle lock.
func (kc *Catalog) DropSegmentChangeGroups(ctx context.Context, collectionID int64) error {
	return kc.MetaKv.RemoveWithPrefix(ctx, buildSegmentChangeGroupCollectionPrefix(collectionID))
}
