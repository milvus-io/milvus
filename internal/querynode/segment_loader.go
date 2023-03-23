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

package querynode

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"path"
	"runtime"
	"runtime/debug"
	"strconv"
	"time"

	"go.uber.org/multierr"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/concurrency"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/hardware"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/panjf2000/ants/v2"
)

const (
	UsedDiskMemoryRatio = 4
)

var (
	ErrReadDeltaMsgFailed = errors.New("ReadDeltaMsgFailed")
)

// segmentLoader is only responsible for loading the field data from binlog
type segmentLoader struct {
	metaReplica ReplicaInterface

	dataCoord types.DataCoord

	cm     storage.ChunkManager // minio cm
	etcdKV *etcdkv.EtcdKV

	ioPool  *concurrency.Pool
	cpuPool *concurrency.Pool

	factory msgstream.Factory
}

func (loader *segmentLoader) getFieldType(segment *Segment, fieldID FieldID) (schemapb.DataType, error) {
	coll, err := loader.metaReplica.getCollectionByID(segment.collectionID)
	if err != nil {
		return schemapb.DataType_None, err
	}

	return coll.getFieldType(fieldID)
}

func (loader *segmentLoader) LoadSegment(ctx context.Context, req *querypb.LoadSegmentsRequest, segmentType segmentType) ([]UniqueID, error) {
	if req.Base == nil {
		return nil, fmt.Errorf("nil base message when load segment, collectionID = %d", req.CollectionID)
	}

	log := log.With(zap.Int64("collectionID", req.CollectionID), zap.String("segmentType", segmentType.String()))
	// no segment needs to load, return
	segmentNum := len(req.Infos)

	if segmentNum == 0 {
		log.Warn("find no valid segment target, skip load segment", zap.Any("request", req))
		return nil, nil
	}

	log.Info("segmentLoader start loading...", zap.Any("segmentNum", segmentNum))

	// check memory limit
	min := func(first int, values ...int) int {
		minValue := first
		for _, v := range values {
			if v < minValue {
				minValue = v
			}
		}
		return minValue
	}
	concurrencyLevel := min(runtime.GOMAXPROCS(0), len(req.Infos))
	for ; concurrencyLevel > 1; concurrencyLevel /= 2 {
		err := loader.checkSegmentSize(req.CollectionID, req.Infos, concurrencyLevel)
		if err == nil {
			break
		}
	}

	err := loader.checkSegmentSize(req.CollectionID, req.Infos, concurrencyLevel)
	if err != nil {
		log.Error("load failed, OOM if loaded",
			zap.Int64("loadSegmentRequest msgID", req.Base.MsgID),
			zap.Error(err))
		return nil, err
	}

	newSegments := make(map[UniqueID]*Segment, segmentNum)
	loadDoneSegmentIDSet := typeutil.NewConcurrentSet[int64]()
	segmentGC := func(force bool) {
		for id, s := range newSegments {
			if force || !loadDoneSegmentIDSet.Contain(id) {
				deleteSegment(s)
			}
		}
		debug.FreeOSMemory()
	}

	for _, info := range req.Infos {
		segmentID := info.SegmentID
		partitionID := info.PartitionID
		collectionID := info.CollectionID
		vChannelID := info.InsertChannel

		collection, err := loader.metaReplica.getCollectionByID(collectionID)
		if err != nil {
			segmentGC(true)
			return nil, err
		}

		segment, err := newSegment(collection, segmentID, partitionID, collectionID, vChannelID, segmentType, req.GetVersion(), info.StartPosition)
		if err != nil {
			log.Error("load segment failed when create new segment",
				zap.Int64("partitionID", partitionID),
				zap.Int64("segmentID", segmentID),
				zap.Error(err))
			segmentGC(true)
			return nil, err
		}

		newSegments[segmentID] = segment
	}

	loadFileFunc := func(idx int) error {
		loadInfo := req.Infos[idx]
		partitionID := loadInfo.PartitionID
		segmentID := loadInfo.SegmentID
		segment := newSegments[segmentID]

		tr := timerecord.NewTimeRecorder("loadDurationPerSegment")
		err := loader.loadFiles(ctx, segment, loadInfo)
		if err != nil {
			log.Error("load segment failed when load data into memory",
				zap.Int64("partitionID", partitionID),
				zap.Int64("segmentID", segmentID),
				zap.Error(err))
			return err
		}

		loadDoneSegmentIDSet.Insert(segmentID)
		metrics.QueryNodeLoadSegmentLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Observe(float64(tr.ElapseSpan().Milliseconds()))

		return nil
	}

	// start to load
	// Make sure we can always benefit from concurrency, and not spawn too many idle goroutines
	log.Info("start to load segments in parallel",
		zap.Int("segmentNum", segmentNum),
		zap.Int("concurrencyLevel", concurrencyLevel))
	loadErr := funcutil.ProcessFuncParallel(segmentNum,
		concurrencyLevel, loadFileFunc, "loadSegmentFunc")

	// set segment which has been loaded done to meta replica
	failedSetMetaSegmentIDs := make([]UniqueID, 0)
	for _, id := range loadDoneSegmentIDSet.Collect() {
		segment := newSegments[id]
		err = loader.metaReplica.setSegment(segment)
		if err != nil {
			log.Error("load segment failed, set segment to meta failed",
				zap.Int64("collectionID", segment.collectionID),
				zap.Int64("partitionID", segment.partitionID),
				zap.Int64("segmentID", segment.segmentID),
				zap.Int64("loadSegmentRequest msgID", req.Base.MsgID),
				zap.Error(err))
			failedSetMetaSegmentIDs = append(failedSetMetaSegmentIDs, id)
			loadDoneSegmentIDSet.Remove(id)
		}
	}
	if len(failedSetMetaSegmentIDs) > 0 {
		err = fmt.Errorf("load segment failed, set segment to meta failed, segmentIDs: %v", failedSetMetaSegmentIDs)
	}

	err = multierr.Combine(loadErr, err)
	if err != nil {
		segmentGC(false)
		return loadDoneSegmentIDSet.Collect(), err
	}

	return loadDoneSegmentIDSet.Collect(), nil
}

func (loader *segmentLoader) loadFiles(ctx context.Context, segment *Segment,
	loadInfo *querypb.SegmentLoadInfo) error {
	collectionID := loadInfo.CollectionID
	partitionID := loadInfo.PartitionID
	segmentID := loadInfo.SegmentID
	log.Info("start loading segment data into memory",
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID),
		zap.Int64("segmentID", segmentID),
		zap.String("segmentType", segment.getType().String()))

	pkFieldID, err := loader.metaReplica.getPKFieldIDByCollectionID(collectionID)
	if err != nil {
		return err
	}

	// TODO(xige-16): Optimize the data loading process and reduce data copying
	// for now, there will be multiple copies in the process of data loading into segCore
	defer debug.FreeOSMemory()

	if segment.getType() == segmentTypeSealed {
		fieldID2IndexInfo := make(map[int64]*querypb.FieldIndexInfo)
		for _, indexInfo := range loadInfo.IndexInfos {
			if len(indexInfo.IndexFilePaths) > 0 {
				fieldID := indexInfo.FieldID
				fieldID2IndexInfo[fieldID] = indexInfo
			}
		}

		indexedFieldInfos := make(map[int64]*IndexedFieldInfo)
		fieldBinlogs := make([]*datapb.FieldBinlog, 0, len(loadInfo.BinlogPaths))

		for _, fieldBinlog := range loadInfo.BinlogPaths {
			fieldID := fieldBinlog.FieldID
			// check num rows of data meta and index meta are consistent
			if indexInfo, ok := fieldID2IndexInfo[fieldID]; ok {
				fieldInfo := &IndexedFieldInfo{
					fieldBinlog: fieldBinlog,
					indexInfo:   indexInfo,
				}
				indexedFieldInfos[fieldID] = fieldInfo
			} else {
				fieldBinlogs = append(fieldBinlogs, fieldBinlog)
			}
		}

		if err := loader.loadIndexedFieldData(ctx, segment, indexedFieldInfos); err != nil {
			return err
		}
		if err := loader.loadSealedSegmentFields(ctx, segment, fieldBinlogs, loadInfo.GetNumOfRows()); err != nil {
			return err
		}
	} else {
		if err := segment.LoadMultiFieldData(loadInfo.GetNumOfRows(), loadInfo.BinlogPaths); err != nil {
			return err
		}
	}

	if pkFieldID == common.InvalidFieldID {
		log.Warn("segment primary key field doesn't exist when load segment")
	} else {
		log.Info("loading bloom filter...", zap.Int64("segmentID", segmentID))
		pkStatsBinlogs := loader.filterPKStatsBinlogs(loadInfo.Statslogs, pkFieldID)
		err = loader.loadSegmentBloomFilter(ctx, segment, pkStatsBinlogs)
		if err != nil {
			return err
		}
	}

	log.Info("loading delta...", zap.Int64("segmentID", segmentID))
	err = loader.loadDeltaLogs(ctx, segment, loadInfo.Deltalogs)
	return err
}

func (loader *segmentLoader) filterPKStatsBinlogs(fieldBinlogs []*datapb.FieldBinlog, pkFieldID int64) []string {
	result := make([]string, 0)
	for _, fieldBinlog := range fieldBinlogs {
		if fieldBinlog.FieldID == pkFieldID {
			for _, binlog := range fieldBinlog.GetBinlogs() {
				result = append(result, binlog.GetLogPath())
			}
		}
	}
	return result
}

func (loader *segmentLoader) loadSealedSegmentFields(ctx context.Context, segment *Segment, fields []*datapb.FieldBinlog, rowCount int64) error {
	runningGroup, _ := errgroup.WithContext(ctx)
	for _, field := range fields {
		fieldBinLog := field
		fieldID := field.FieldID
		runningGroup.Go(func() error {
			return segment.LoadFieldData(fieldID, rowCount, fieldBinLog)
		})
	}
	err := runningGroup.Wait()
	if err != nil {
		return err
	}

	log.Info("load field binlogs done for sealed segment",
		zap.Int64("collection", segment.collectionID),
		zap.Int64("segment", segment.segmentID),
		zap.Any("len(field)", len(fields)),
		zap.String("segmentType", segment.getType().String()))

	return nil
}

// Load binlogs concurrently into memory from KV storage asyncly
func (loader *segmentLoader) loadFieldBinlogsAsync(ctx context.Context, field *datapb.FieldBinlog) []*concurrency.Future {
	futures := make([]*concurrency.Future, 0, len(field.Binlogs))
	for i := range field.Binlogs {
		path := field.Binlogs[i].GetLogPath()
		future := loader.ioPool.Submit(func() (interface{}, error) {
			binLog, err := loader.cm.Read(ctx, path)
			if err != nil {
				log.Warn("failed to load binlog", zap.String("filePath", path), zap.Error(err))
				return nil, err
			}
			blob := &storage.Blob{
				Key:   path,
				Value: binLog,
			}

			return blob, nil
		})

		futures = append(futures, future)
	}
	return futures
}

func (loader *segmentLoader) loadIndexedFieldData(ctx context.Context, segment *Segment, vecFieldInfos map[int64]*IndexedFieldInfo) error {
	for fieldID, fieldInfo := range vecFieldInfos {
		indexInfo := fieldInfo.indexInfo
		err := loader.loadFieldIndexData(ctx, segment, indexInfo)
		if err != nil {
			return err
		}

		log.Info("load field binlogs done for sealed segment with index",
			zap.Int64("collection", segment.collectionID),
			zap.Int64("segment", segment.segmentID),
			zap.Int64("fieldID", fieldID))

		segment.setIndexedFieldInfo(fieldID, fieldInfo)
	}

	return nil
}

func (loader *segmentLoader) loadFieldIndexData(ctx context.Context, segment *Segment, indexInfo *querypb.FieldIndexInfo) error {
	filteredPaths := make([]string, 0, len(indexInfo.IndexFilePaths))

	for _, indexPath := range indexInfo.IndexFilePaths {
		if path.Base(indexPath) != storage.IndexParamsKey {
			filteredPaths = append(filteredPaths, indexPath)
		}
	}

	// 2. use index path to update segment
	indexInfo.IndexFilePaths = filteredPaths
	fieldType, err := loader.getFieldType(segment, indexInfo.FieldID)
	if err != nil {
		return err
	}

	return segment.segmentLoadIndexData(indexInfo, fieldType)
}

func (loader *segmentLoader) loadSegmentBloomFilter(ctx context.Context, segment *Segment, binlogPaths []string) error {
	if len(binlogPaths) == 0 {
		log.Info("there are no stats logs saved with segment", zap.Any("segmentID", segment.segmentID))
		return nil
	}

	startTs := time.Now()
	values, err := loader.cm.MultiRead(ctx, binlogPaths)
	if err != nil {
		return err
	}
	blobs := make([]*storage.Blob, 0)
	for i := 0; i < len(values); i++ {
		blobs = append(blobs, &storage.Blob{Value: values[i]})
	}

	stats, err := storage.DeserializeStats(blobs)
	if err != nil {
		log.Warn("failed to deserialize stats", zap.Error(err))
		return err
	}
	var size uint
	for _, stat := range stats {
		pkStat := &storage.PkStatistics{
			PkFilter: stat.BF,
			MinPK:    stat.MinPk,
			MaxPK:    stat.MaxPk,
		}
		size += stat.BF.Cap()
		segment.historyStats = append(segment.historyStats, pkStat)
	}
	log.Info("Successfully load pk stats", zap.Any("time", time.Since(startTs)), zap.Int64("segment", segment.segmentID), zap.Uint("size", size))
	return nil
}

func (loader *segmentLoader) loadDeltaLogs(ctx context.Context, segment *Segment, deltaLogs []*datapb.FieldBinlog) error {
	dCodec := storage.DeleteCodec{}
	var blobs []*storage.Blob
	for _, deltaLog := range deltaLogs {
		for _, bLog := range deltaLog.GetBinlogs() {
			value, err := loader.cm.Read(ctx, bLog.GetLogPath())
			if err != nil {
				return err
			}
			blob := &storage.Blob{
				Key:   bLog.GetLogPath(),
				Value: value,
			}
			blobs = append(blobs, blob)
		}
	}
	if len(blobs) == 0 {
		log.Info("there are no delta logs saved with segment, skip loading delete record", zap.Any("segmentID", segment.segmentID))
		return nil
	}
	_, _, deltaData, err := dCodec.Deserialize(blobs)
	if err != nil {
		return err
	}

	err = segment.segmentLoadDeletedRecord(deltaData.Pks, deltaData.Tss, deltaData.RowCount)
	if err != nil {
		return err
	}
	return nil
}

func (loader *segmentLoader) FromDmlCPLoadDelete(ctx context.Context, collectionID int64, position *internalpb.MsgPosition,
	segmentIDs []int64) error {
	startTs := time.Now()
	stream, err := loader.factory.NewTtMsgStream(ctx)
	if err != nil {
		return err
	}

	defer func() {
		metrics.QueryNodeNumConsumers.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Dec()
		stream.Close()
	}()

	vchannelName := position.ChannelName
	pChannelName := funcutil.ToPhysicalChannel(position.ChannelName)
	position.ChannelName = pChannelName

	ts, _ := tsoutil.ParseTS(position.Timestamp)

	// Random the subname in case we trying to load same delta at the same time
	subName := fmt.Sprintf("querynode-delta-loader-%d-%d-%d", Params.QueryNodeCfg.GetNodeID(), collectionID, rand.Int())
	log.Info("from dml check point load delete", zap.Any("position", position), zap.String("subName", subName), zap.Time("positionTs", ts))
	stream.AsConsumer([]string{pChannelName}, subName, mqwrapper.SubscriptionPositionUnknown)
	// make sure seek position is earlier than
	lastMsgID, err := stream.GetLatestMsgID(pChannelName)
	if err != nil {
		return err
	}

	reachLatest, err := lastMsgID.Equal(position.MsgID)
	if err != nil {
		return err
	}

	if reachLatest || lastMsgID.AtEarliestPosition() {
		log.Info("there is no more delta msg", zap.Int64("collectionID", collectionID), zap.String("channel", pChannelName))
		return nil
	}

	metrics.QueryNodeNumConsumers.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Inc()
	err = stream.Seek([]*internalpb.MsgPosition{position})
	if err != nil {
		return err
	}
	stream.Start()

	delData := &deleteData{
		deleteIDs:        make(map[UniqueID][]primaryKey),
		deleteTimestamps: make(map[UniqueID][]Timestamp),
		deleteOffset:     make(map[UniqueID]int64),
	}

	log.Info("start read delta msg from seek position to last position",
		zap.Int64("collectionID", collectionID),
		zap.String("channel", pChannelName),
		zap.String("seekPos", position.String()),
		zap.Any("lastMsg", lastMsgID), // use any in case of nil
	)

	hasMore := true
	for hasMore {
		select {
		case <-ctx.Done():
			log.Info("read delta msg from seek position done", zap.Error(ctx.Err()))
			return ctx.Err()
		case msgPack, ok := <-stream.Chan():
			if !ok {
				err = fmt.Errorf("%w: pChannelName=%v, msgID=%v",
					ErrReadDeltaMsgFailed,
					pChannelName,
					position.GetMsgID())
				log.Warn("fail to read delta msg",
					zap.String("pChannelName", pChannelName),
					zap.Binary("msgID", position.GetMsgID()),
					zap.Error(err),
				)
				return err
			}

			if msgPack == nil {
				continue
			}

			for _, tsMsg := range msgPack.Msgs {
				if tsMsg.Type() == commonpb.MsgType_Delete {
					dmsg := tsMsg.(*msgstream.DeleteMsg)
					if dmsg.CollectionID != collectionID {
						continue
					}
					err = processDeleteMessages(loader.metaReplica, segmentTypeSealed, dmsg, delData, vchannelName)
					if err != nil {
						// TODO: panic?
						// error occurs when missing meta info or unexpected pk type, should not happen
						err = fmt.Errorf("processDeleteMessages failed, collectionID = %d, err = %s", dmsg.CollectionID, err)
						log.Error("FromDmlCPLoadDelete failed to process delete message", zap.Error(err))
						return err
					}
				}
			}
			ret, err := lastMsgID.LessOrEqualThan(msgPack.EndPositions[0].MsgID)
			if err != nil {
				log.Warn("check whether current MsgID less than last MsgID failed",
					zap.Int64("collectionID", collectionID),
					zap.String("channel", pChannelName),
					zap.Error(err),
				)
				return err
			}
			if ret {
				hasMore = false
				break
			}
		}
	}

	log.Info("All data has been read, there is no more data",
		zap.Int64("collectionID", collectionID),
		zap.String("channel", pChannelName),
		zap.Binary("msgID", position.GetMsgID()),
	)

	segmentIDSet := typeutil.NewUniqueSet(segmentIDs...)

	for segmentID, pks := range delData.deleteIDs {
		// ignore non target segment
		if !segmentIDSet.Contain(segmentID) {
			continue
		}
		segment, err := loader.metaReplica.getSegmentByID(segmentID, segmentTypeSealed)
		if err != nil {
			log.Warn("failed to get segment", zap.Int64("segment", segmentID), zap.Error(err))
			return err
		}
		offset := segment.segmentPreDelete(len(pks))
		delData.deleteOffset[segmentID] = offset
		timestamps := delData.deleteTimestamps[segmentID]
		err = segment.segmentDelete(offset, pks, timestamps)
		if err != nil {
			log.Warn("QueryNode: segment delete failed", zap.Int64("segment", segmentID), zap.Error(err))
			return err
		}
	}

	log.Info("from dml check point load done", zap.String("subName", subName), zap.Any("timeTake", time.Since(startTs)))
	return nil
}

// JoinIDPath joins ids to path format.
func JoinIDPath(ids ...UniqueID) string {
	idStr := make([]string, 0, len(ids))
	for _, id := range ids {
		idStr = append(idStr, strconv.FormatInt(id, 10))
	}
	return path.Join(idStr...)
}

func GetStorageSizeByIndexInfo(indexInfo *querypb.FieldIndexInfo) (uint64, uint64, error) {
	indexType, err := funcutil.GetAttrByKeyFromRepeatedKV("index_type", indexInfo.IndexParams)
	if err != nil {
		return 0, 0, fmt.Errorf("index type not exist in index params")
	}
	if indexType == indexparamcheck.IndexDISKANN {
		neededMemSize := indexInfo.IndexSize / UsedDiskMemoryRatio
		return uint64(neededMemSize), uint64(indexInfo.IndexSize), nil
	}

	return uint64(indexInfo.IndexSize), 0, nil
}

func (loader *segmentLoader) checkSegmentSize(collectionID UniqueID, segmentLoadInfos []*querypb.SegmentLoadInfo, concurrency int) error {
	usedMem := hardware.GetUsedMemoryCount()
	totalMem := hardware.GetMemoryCount()
	if len(segmentLoadInfos) < concurrency {
		concurrency = len(segmentLoadInfos)
	}

	if usedMem == 0 || totalMem == 0 {
		return fmt.Errorf("get memory failed when checkSegmentSize, collectionID = %d", collectionID)
	}

	usedMemAfterLoad := usedMem
	maxSegmentSize := uint64(0)

	localUsedSize, err := GetLocalUsedSize()
	if err != nil {
		return fmt.Errorf("get local used size failed, collectionID = %d", collectionID)
	}
	usedLocalSizeAfterLoad := uint64(localUsedSize)

	for _, loadInfo := range segmentLoadInfos {
		oldUsedMem := usedMemAfterLoad
		vecFieldID2IndexInfo := make(map[int64]*querypb.FieldIndexInfo)
		for _, fieldIndexInfo := range loadInfo.IndexInfos {
			if fieldIndexInfo.EnableIndex {
				fieldID := fieldIndexInfo.FieldID
				vecFieldID2IndexInfo[fieldID] = fieldIndexInfo
			}
		}

		for _, fieldBinlog := range loadInfo.BinlogPaths {
			fieldID := fieldBinlog.FieldID
			if fieldIndexInfo, ok := vecFieldID2IndexInfo[fieldID]; ok {
				neededMemSize, neededDiskSize, err := GetStorageSizeByIndexInfo(fieldIndexInfo)
				if err != nil {
					log.Error(err.Error(), zap.Int64("collectionID", loadInfo.CollectionID),
						zap.Int64("segmentID", loadInfo.SegmentID),
						zap.Int64("indexBuildID", fieldIndexInfo.BuildID))
					return err
				}
				usedMemAfterLoad += neededMemSize
				usedLocalSizeAfterLoad += neededDiskSize
			} else {
				usedMemAfterLoad += uint64(funcutil.GetFieldSizeFromFieldBinlog(fieldBinlog))
			}
		}

		// get size of state data
		for _, fieldBinlog := range loadInfo.Statslogs {
			usedMemAfterLoad += uint64(funcutil.GetFieldSizeFromFieldBinlog(fieldBinlog))
		}

		// get size of delete data
		for _, fieldBinlog := range loadInfo.Deltalogs {
			usedMemAfterLoad += uint64(funcutil.GetFieldSizeFromFieldBinlog(fieldBinlog))
		}

		if usedMemAfterLoad-oldUsedMem > maxSegmentSize {
			maxSegmentSize = usedMemAfterLoad - oldUsedMem
		}
	}

	toMB := func(mem uint64) uint64 {
		return mem / 1024 / 1024
	}

	// when load segment, data will be copied from go memory to c++ memory
	memLoadingUsage := usedMemAfterLoad + uint64(
		float64(maxSegmentSize)*float64(concurrency)*Params.QueryNodeCfg.LoadMemoryUsageFactor)
	log.Info("predict memory and disk usage while loading (in MiB)",
		zap.Int64("collectionID", collectionID),
		zap.Int("concurrency", concurrency),
		zap.Uint64("memUsage", toMB(memLoadingUsage)),
		zap.Uint64("memUsageAfterLoad", toMB(usedMemAfterLoad)),
		zap.Uint64("diskUsageAfterLoad", toMB(usedLocalSizeAfterLoad)))

	if memLoadingUsage > uint64(float64(totalMem)*Params.QueryNodeCfg.OverloadedMemoryThresholdPercentage) {
		return fmt.Errorf("%w, load segment failed, OOM if load, collectionID = %d, maxSegmentSize = %v MB, concurrency = %d, usedMemAfterLoad = %v MB, totalMem = %v MB, thresholdFactor = %f",
			ErrInsufficientMemory,
			collectionID,
			toMB(maxSegmentSize),
			concurrency,
			toMB(usedMemAfterLoad),
			toMB(totalMem),
			Params.QueryNodeCfg.OverloadedMemoryThresholdPercentage)
	}

	if usedLocalSizeAfterLoad > uint64(float64(Params.QueryNodeCfg.DiskCapacityLimit)*Params.QueryNodeCfg.MaxDiskUsagePercentage) {
		return fmt.Errorf("load segment failed, disk space is not enough, collectionID = %d, usedDiskAfterLoad = %v MB, totalDisk = %v MB, thresholdFactor = %f",
			collectionID,
			toMB(usedLocalSizeAfterLoad),
			toMB(uint64(Params.QueryNodeCfg.DiskCapacityLimit)),
			Params.QueryNodeCfg.MaxDiskUsagePercentage)
	}

	return nil
}

func newSegmentLoader(
	metaReplica ReplicaInterface,
	etcdKV *etcdkv.EtcdKV,
	cm storage.ChunkManager,
	factory msgstream.Factory) *segmentLoader {

	cpuNum := runtime.GOMAXPROCS(0)
	ioPoolSize := cpuNum * 8
	// make sure small machines could load faster
	if ioPoolSize < 32 {
		ioPoolSize = 32
	}
	// limit the number of concurrency
	if ioPoolSize > 256 {
		ioPoolSize = 256
	}
	ioPool, err := concurrency.NewPool(ioPoolSize, ants.WithPreAlloc(true))
	if err != nil {
		log.Error("failed to create goroutine pool for segment loader",
			zap.Error(err))
		panic(err)
	}
	cpuPool, err := concurrency.NewPool(cpuNum, ants.WithPreAlloc(true))
	if err != nil {
		log.Error("failed to create cpu goroutine pool for segment loader", zap.Error(err))
		panic(err)
	}

	log.Info("SegmentLoader created",
		zap.Int("ioPoolSize", ioPoolSize),
		zap.Int("cpuPoolSize", cpuNum),
	)

	loader := &segmentLoader{
		metaReplica: metaReplica,

		cm:     cm,
		etcdKV: etcdKV,

		// init them later
		ioPool:  ioPool,
		cpuPool: cpuPool,

		factory: factory,
	}

	return loader
}
