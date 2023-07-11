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

package segments

import (
	"context"
	"fmt"
	"path"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/metrics"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/conc"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/indexparamcheck"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/timerecord"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	. "github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	UsedDiskMemoryRatio = 4
)

var (
	ErrReadDeltaMsgFailed = errors.New("ReadDeltaMsgFailed")
)

type Loader interface {
	// Load loads binlogs, and spawn segments,
	// NOTE: make sure the ref count of the corresponding collection will never go down to 0 during this
	Load(ctx context.Context, collectionID int64, segmentType SegmentType, version int64, segments ...*querypb.SegmentLoadInfo) ([]Segment, error)

	LoadDeltaLogs(ctx context.Context, segment *LocalSegment, deltaLogs []*datapb.FieldBinlog) error

	// LoadRemote loads needed binlogs/statslog for RemoteSegment.
	LoadBloomFilterSet(ctx context.Context, collectionID int64, version int64, infos ...*querypb.SegmentLoadInfo) ([]*pkoracle.BloomFilterSet, error)
}

func NewLoader(
	manager *Manager,
	cm storage.ChunkManager,
) *segmentLoader {
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

	if configPoolSize := paramtable.Get().QueryNodeCfg.IoPoolSize.GetAsInt(); configPoolSize > 0 {
		ioPoolSize = configPoolSize
	}

	ioPool := conc.NewPool[*storage.Blob](ioPoolSize, conc.WithPreAlloc(true))

	log.Info("SegmentLoader created", zap.Int("ioPoolSize", ioPoolSize))

	loader := &segmentLoader{
		manager:         manager,
		cm:              cm,
		ioPool:          ioPool,
		loadingSegments: typeutil.NewConcurrentMap[int64, chan struct{}](),
	}

	return loader
}

// segmentLoader is only responsible for loading the field data from binlog
type segmentLoader struct {
	manager *Manager
	cm      storage.ChunkManager
	ioPool  *conc.Pool[*storage.Blob]

	mut sync.Mutex
	// The channel will be closed as the segment loaded
	loadingSegments   *typeutil.ConcurrentMap[int64, chan struct{}]
	committedMemSize  uint64
	committedDiskSize uint64
}

var _ Loader = (*segmentLoader)(nil)

func (loader *segmentLoader) Load(ctx context.Context,
	collectionID int64,
	segmentType SegmentType,
	version int64,
	segments ...*querypb.SegmentLoadInfo,
) ([]Segment, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", collectionID),
		zap.String("segmentType", segmentType.String()),
	)

	// Filter out loaded & loading segments
	infos := loader.prepare(segmentType, segments...)
	defer loader.unregister(infos...)

	segmentNum := len(infos)
	if segmentNum == 0 {
		log.Info("no segment to load")
		return nil, nil
	}

	log.Info("start loading...", zap.Int("segmentNum", segmentNum))

	// Check memory & storage limit
	memUsage, diskUsage, concurrencyLevel, err := loader.requestResource(infos...)
	if err != nil {
		return nil, err
	}
	defer func() {
		loader.mut.Lock()
		defer loader.mut.Unlock()

		loader.committedMemSize -= memUsage
		loader.committedDiskSize -= diskUsage
	}()

	newSegments := make(map[int64]*LocalSegment, len(infos))
	clearAll := func() {
		for _, s := range newSegments {
			DeleteSegment(s)
		}
		debug.FreeOSMemory()
	}

	for _, info := range infos {
		segmentID := info.SegmentID
		partitionID := info.PartitionID
		collectionID := info.CollectionID
		shard := info.InsertChannel

		collection := loader.manager.Collection.Get(collectionID)
		if collection == nil {
			err := merr.WrapErrCollectionNotFound(collectionID)
			log.Warn("failed to get collection", zap.Error(err))
			clearAll()
			return nil, err
		}
		segment, err := NewSegment(collection, segmentID, partitionID, collectionID, shard, segmentType, version, info.GetStartPosition(), info.GetDeltaPosition())
		if err != nil {
			log.Error("load segment failed when create new segment",
				zap.Int64("partitionID", partitionID),
				zap.Int64("segmentID", segmentID),
				zap.Error(err),
			)
			clearAll()
			return nil, err
		}

		newSegments[segmentID] = segment
	}

	loadSegmentFunc := func(idx int) error {
		loadInfo := infos[idx]
		partitionID := loadInfo.PartitionID
		segmentID := loadInfo.SegmentID
		segment := newSegments[segmentID]

		tr := timerecord.NewTimeRecorder("loadDurationPerSegment")
		err := loader.loadSegment(ctx, segment, loadInfo)
		if err != nil {
			log.Error("load segment failed when load data into memory",
				zap.Int64("partitionID", partitionID),
				zap.Int64("segmentID", segmentID),
				zap.Error(err),
			)
			return err
		}
		log.Info("load segment done", zap.Int64("segmentID", segmentID))

		metrics.QueryNodeLoadSegmentLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Observe(float64(tr.ElapseSpan().Milliseconds()))

		waitCh, ok := loader.loadingSegments.Get(segmentID)
		if !ok {
			return errors.New("segment was removed from the loading map early")
		}
		close(waitCh)

		return nil
	}

	// Start to load,
	// Make sure we can always benefit from concurrency, and not spawn too many idle goroutines
	log.Info("start to load segments in parallel",
		zap.Int("segmentNum", segmentNum),
		zap.Int("concurrencyLevel", concurrencyLevel))
	err = funcutil.ProcessFuncParallel(segmentNum,
		concurrencyLevel, loadSegmentFunc, "loadSegmentFunc")
	if err != nil {
		clearAll()
		log.Warn("failed to load some segments", zap.Error(err))
		return nil, err
	}

	// Wait for all segments loaded
	for _, segment := range segments {
		if loader.manager.Segment.GetWithType(segment.GetSegmentID(), segmentType) != nil {
			continue
		}

		waitCh, ok := loader.loadingSegments.Get(segment.GetSegmentID())
		if !ok {
			log.Warn("segment was removed from the loading map early", zap.Int64("segmentID", segment.GetSegmentID()))
			return nil, errors.New("segment was removed from the loading map early")
		}

		log.Info("wait segment loaded...", zap.Int64("segmentID", segment.GetSegmentID()))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-waitCh:
		}
		log.Info("segment loaded...", zap.Int64("segmentID", segment.GetSegmentID()))
	}

	loaded := make([]Segment, 0, len(newSegments))
	for _, segment := range newSegments {
		loaded = append(loaded, segment)
	}
	loader.manager.Segment.Put(segmentType, loaded...)
	log.Info("all segment load done")
	return loaded, nil
}

func (loader *segmentLoader) prepare(segmentType SegmentType, segments ...*querypb.SegmentLoadInfo) []*querypb.SegmentLoadInfo {
	loader.mut.Lock()
	defer loader.mut.Unlock()

	// filter out loaded & loading segments
	infos := make([]*querypb.SegmentLoadInfo, 0, len(segments))
	for _, segment := range segments {
		// Not loaded & loading
		if len(loader.manager.Segment.GetBy(WithType(segmentType), WithID(segment.GetSegmentID()))) == 0 &&
			!loader.loadingSegments.Contain(segment.GetSegmentID()) {
			infos = append(infos, segment)
			loader.loadingSegments.Insert(segment.GetSegmentID(), make(chan struct{}))
		} else {
			log.Info("skip loaded/loading segment", zap.Int64("segmentID", segment.GetSegmentID()),
				zap.Bool("isLoaded", len(loader.manager.Segment.GetBy(WithType(segmentType), WithID(segment.GetSegmentID()))) > 0),
				zap.Bool("isLoading", loader.loadingSegments.Contain(segment.GetSegmentID())),
			)
		}
	}

	return infos
}

func (loader *segmentLoader) unregister(segments ...*querypb.SegmentLoadInfo) {
	loader.mut.Lock()
	defer loader.mut.Unlock()
	for i := range segments {
		loader.loadingSegments.GetAndRemove(segments[i].GetSegmentID())
	}
}

// requestResource requests memory & storage to load segments,
// returns the memory usage, disk usage and concurrency with the gained memory.
func (loader *segmentLoader) requestResource(infos ...*querypb.SegmentLoadInfo) (uint64, uint64, int, error) {
	loader.mut.Lock()
	defer loader.mut.Unlock()

	concurrencyLevel := funcutil.Min(runtime.GOMAXPROCS(0), len(infos))

	logNum := 0
	for _, field := range infos[0].GetBinlogPaths() {
		logNum += len(field.GetBinlogs())
	}
	if logNum > 0 {
		// IO pool will be run out even with the new smaller level
		concurrencyLevel = funcutil.Min(concurrencyLevel, funcutil.Max(loader.ioPool.Free()/logNum, 1))
	}

	for ; concurrencyLevel > 1; concurrencyLevel /= 2 {
		_, _, err := loader.checkSegmentSize(infos, concurrencyLevel)
		if err == nil {
			break
		}
	}

	memUsage, diskUsage, err := loader.checkSegmentSize(infos, concurrencyLevel)
	if err != nil {
		log.Warn("no sufficient resource to load segments", zap.Error(err))
		return 0, 0, 0, err
	}

	loader.committedMemSize += memUsage
	loader.committedDiskSize += diskUsage

	return memUsage, diskUsage, concurrencyLevel, nil
}

func (loader *segmentLoader) LoadBloomFilterSet(ctx context.Context, collectionID int64, version int64, infos ...*querypb.SegmentLoadInfo) ([]*pkoracle.BloomFilterSet, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", collectionID),
		zap.Int64s("segmentIDs", lo.Map(infos, func(info *querypb.SegmentLoadInfo, _ int) int64 {
			return info.SegmentID
		})),
	)

	segmentNum := len(infos)
	if segmentNum == 0 {
		log.Info("no segment to load")
		return nil, nil
	}

	collection := loader.manager.Collection.Get(collectionID)
	if collection == nil {
		err := merr.WrapErrCollectionNotFound(collectionID)
		log.Warn("failed to get collection while loading segment", zap.Error(err))
		return nil, err
	}
	pkField := GetPkField(collection.Schema())

	log.Info("start loading remote...", zap.Int("segmentNum", segmentNum))

	loadedBfs := NewConcurrentSet[*pkoracle.BloomFilterSet]()
	// TODO check memory for bf size
	loadRemoteFunc := func(idx int) error {
		loadInfo := infos[idx]
		partitionID := loadInfo.PartitionID
		segmentID := loadInfo.SegmentID
		bfs := pkoracle.NewBloomFilterSet(segmentID, partitionID, commonpb.SegmentState_Sealed)

		log.Info("loading bloom filter for remote...")
		pkStatsBinlogs, logType := loader.filterPKStatsBinlogs(loadInfo.Statslogs, pkField.GetFieldID())
		err := loader.loadBloomFilter(ctx, segmentID, bfs, pkStatsBinlogs, logType)
		if err != nil {
			log.Warn("load remote segment bloom filter failed",
				zap.Int64("partitionID", partitionID),
				zap.Int64("segmentID", segmentID),
				zap.Error(err),
			)
			return err
		}
		loadedBfs.Insert(bfs)

		return nil
	}

	err := funcutil.ProcessFuncParallel(segmentNum, segmentNum, loadRemoteFunc, "loadRemoteFunc")
	if err != nil {
		// no partial success here
		log.Warn("failed to load remote segment", zap.Error(err))
		return nil, err
	}

	return loadedBfs.Collect(), nil
}

func (loader *segmentLoader) loadSegment(ctx context.Context,
	segment *LocalSegment,
	loadInfo *querypb.SegmentLoadInfo,
) error {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", segment.Collection()),
		zap.Int64("partitionID", segment.Partition()),
		zap.String("shard", segment.Shard()),
		zap.Int64("segmentID", segment.ID()),
	)
	log.Info("start loading segment files",
		zap.Int64("rowNum", loadInfo.GetNumOfRows()),
		zap.String("segmentType", segment.Type().String()))

	collection := loader.manager.Collection.Get(segment.Collection())
	if collection == nil {
		err := merr.WrapErrCollectionNotFound(segment.Collection())
		log.Warn("failed to get collection while loading segment", zap.Error(err))
		return err
	}
	pkField := GetPkField(collection.Schema())

	// TODO(xige-16): Optimize the data loading process and reduce data copying
	// for now, there will be multiple copies in the process of data loading into segCore
	defer debug.FreeOSMemory()

	if segment.Type() == SegmentTypeSealed {
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
					FieldBinlog: fieldBinlog,
					IndexInfo:   indexInfo,
				}
				indexedFieldInfos[fieldID] = fieldInfo
			} else {
				fieldBinlogs = append(fieldBinlogs, fieldBinlog)
			}
		}

		log.Info("load fields...",
			zap.Int64s("indexedFields", lo.Keys(indexedFieldInfos)),
		)
		if err := loader.loadFieldsIndex(ctx, segment, indexedFieldInfos); err != nil {
			return err
		}
		if err := loader.loadSealedSegmentFields(ctx, segment, fieldBinlogs, loadInfo.GetNumOfRows()); err != nil {
			return err
		}
		// https://github.com/milvus-io/milvus/23654
		// legacy entry num = 0
		if err := loader.patchEntryNumber(ctx, segment, loadInfo); err != nil {
			return err
		}
	} else {
		if err := segment.LoadMultiFieldData(loadInfo.GetNumOfRows(), loadInfo.BinlogPaths); err != nil {
			return err
		}
	}

	// load statslog if it's growing segment
	if segment.typ == SegmentTypeGrowing {
		log.Info("loading statslog...")
		pkStatsBinlogs, logType := loader.filterPKStatsBinlogs(loadInfo.Statslogs, pkField.GetFieldID())
		err := loader.loadBloomFilter(ctx, segment.segmentID, segment.bloomFilterSet, pkStatsBinlogs, logType)
		if err != nil {
			return err
		}
	}

	log.Info("loading delta...")
	return loader.LoadDeltaLogs(ctx, segment, loadInfo.Deltalogs)
}

func (loader *segmentLoader) filterPKStatsBinlogs(fieldBinlogs []*datapb.FieldBinlog, pkFieldID int64) ([]string, storage.StatsLogType) {
	result := make([]string, 0)
	for _, fieldBinlog := range fieldBinlogs {
		if fieldBinlog.FieldID == pkFieldID {
			for _, binlog := range fieldBinlog.GetBinlogs() {
				_, logidx := path.Split(binlog.GetLogPath())
				// if special status log exist
				// only load one file
				switch logidx {
				case storage.CompoundStatsType.LogIdx():
					return []string{binlog.GetLogPath()}, storage.CompoundStatsType
				default:
					result = append(result, binlog.GetLogPath())
				}
			}
		}
	}
	return result, storage.DefaultStatsType
}

func (loader *segmentLoader) loadSealedSegmentFields(ctx context.Context, segment *LocalSegment, fields []*datapb.FieldBinlog, rowCount int64) error {
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
		zap.Int("len(field)", len(fields)),
		zap.String("segmentType", segment.Type().String()))

	return nil
}

// Load binlogs concurrently into memory from KV storage asyncly
func (loader *segmentLoader) loadFieldBinlogsAsync(ctx context.Context, field *datapb.FieldBinlog) []*conc.Future[*storage.Blob] {
	futures := make([]*conc.Future[*storage.Blob], 0, len(field.Binlogs))
	for i := range field.Binlogs {
		path := field.Binlogs[i].GetLogPath()
		future := loader.ioPool.Submit(func() (*storage.Blob, error) {
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

func (loader *segmentLoader) loadFieldsIndex(ctx context.Context, segment *LocalSegment, vecFieldInfos map[int64]*IndexedFieldInfo) error {
	for fieldID, fieldInfo := range vecFieldInfos {
		indexInfo := fieldInfo.IndexInfo
		err := loader.loadFieldIndex(ctx, segment, indexInfo)
		if err != nil {
			return err
		}

		log.Info("load field binlogs done for sealed segment with index",
			zap.Int64("collection", segment.collectionID),
			zap.Int64("segment", segment.segmentID),
			zap.Int64("fieldID", fieldID),
			zap.Any("binlog", fieldInfo.FieldBinlog.Binlogs),
		)

		segment.AddIndex(fieldID, fieldInfo)
	}

	return nil
}

func (loader *segmentLoader) loadFieldIndex(ctx context.Context, segment *LocalSegment, indexInfo *querypb.FieldIndexInfo) error {
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

	return segment.LoadIndexData(indexInfo, fieldType)
}

func (loader *segmentLoader) insertIntoSegment(segment *LocalSegment,
	rowIDs []UniqueID,
	timestamps []Timestamp,
	insertData *storage.InsertData) error {
	rowNum := len(rowIDs)
	if rowNum != len(timestamps) || insertData == nil {
		return errors.New(fmt.Sprintln("illegal insert data when load segment, collectionID = ", segment.collectionID))
	}

	log := log.With(
		zap.Int64("collectionID", segment.Collection()),
		zap.Int64("segmentID", segment.ID()),
	)

	log.Info("start load growing segments...", zap.Int("rowNum", len(rowIDs)))

	// 1. update bloom filter
	insertRecord, err := storage.TransferInsertDataToInsertRecord(insertData)
	if err != nil {
		return err
	}
	insertMsg := &msgstream.InsertMsg{
		InsertRequest: msgpb.InsertRequest{
			CollectionID: segment.collectionID,
			Timestamps:   timestamps,
			RowIDs:       rowIDs,
			NumRows:      uint64(rowNum),
			FieldsData:   insertRecord.FieldsData,
			Version:      msgpb.InsertDataVersion_ColumnBased,
		},
	}
	collection := loader.manager.Collection.Get(segment.Collection())
	if collection == nil {
		err := merr.WrapErrCollectionNotFound(segment.Collection())
		log.Warn("failed to get collection while inserting data into segment", zap.Error(err))
		return err
	}
	pks, err := GetPrimaryKeys(insertMsg, collection.Schema())
	if err != nil {
		return err
	}
	segment.bloomFilterSet.UpdateBloomFilter(pks)

	// 2. do insert
	err = segment.Insert(rowIDs, timestamps, insertRecord)
	if err != nil {
		return err
	}
	log.Info("Do insert done for growing segment", zap.Int("rowNum", rowNum))
	return nil
}

func (loader *segmentLoader) loadBloomFilter(ctx context.Context, segmentID int64, bfs *pkoracle.BloomFilterSet,
	binlogPaths []string, logType storage.StatsLogType) error {

	log := log.Ctx(ctx).With(
		zap.Int64("segmentID", segmentID),
	)
	if len(binlogPaths) == 0 {
		log.Info("there are no stats logs saved with segment")
		return nil
	}

	startTs := time.Now()
	values, err := loader.cm.MultiRead(ctx, binlogPaths)
	if err != nil {
		return err
	}
	blobs := []*storage.Blob{}
	for i := 0; i < len(values); i++ {
		blobs = append(blobs, &storage.Blob{Value: values[i]})
	}

	var stats []*storage.PrimaryKeyStats
	if logType == storage.CompoundStatsType {
		stats, err = storage.DeserializeStatsList(blobs[0])
		if err != nil {
			log.Warn("failed to deserialize stats list", zap.Error(err))
			return err
		}
	} else {
		stats, err = storage.DeserializeStats(blobs)
		if err != nil {
			log.Warn("failed to deserialize stats", zap.Error(err))
			return err
		}
	}

	var size uint
	for _, stat := range stats {
		pkStat := &storage.PkStatistics{
			PkFilter: stat.BF,
			MinPK:    stat.MinPk,
			MaxPK:    stat.MaxPk,
		}
		size += stat.BF.Cap()
		bfs.AddHistoricalStats(pkStat)
	}
	log.Info("Successfully load pk stats", zap.Duration("time", time.Since(startTs)), zap.Uint("size", size))
	return nil
}

func (loader *segmentLoader) LoadDeltaLogs(ctx context.Context, segment *LocalSegment, deltaLogs []*datapb.FieldBinlog) error {
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

	err = segment.LoadDeltaData(deltaData)
	if err != nil {
		return err
	}
	return nil
}

func (loader *segmentLoader) patchEntryNumber(ctx context.Context, segment *LocalSegment, loadInfo *querypb.SegmentLoadInfo) error {
	var needReset bool

	segment.fieldIndexes.Range(func(fieldID int64, info *IndexedFieldInfo) bool {
		for _, info := range info.FieldBinlog.GetBinlogs() {
			if info.GetEntriesNum() == 0 {
				needReset = true
				return false
			}
		}
		return true
	})
	if !needReset {
		return nil
	}

	log.Warn("legacy segment binlog found, start to patch entry num", zap.Int64("segmentID", segment.segmentID))
	rowIDField := lo.FindOrElse(loadInfo.BinlogPaths, nil, func(binlog *datapb.FieldBinlog) bool {
		return binlog.GetFieldID() == common.RowIDField
	})

	if rowIDField == nil {
		return errors.New("rowID field binlog not found")
	}

	counts := make([]int64, 0, len(rowIDField.GetBinlogs()))
	for _, binlog := range rowIDField.GetBinlogs() {
		bs, err := loader.cm.Read(ctx, binlog.LogPath)
		if err != nil {
			return err
		}

		// get binlog entry num from rowID field
		// since header does not store entry numb, we have to read all data here

		reader, err := storage.NewBinlogReader(bs)
		if err != nil {
			return err
		}
		er, err := reader.NextEventReader()
		if err != nil {
			return err
		}

		rowIDs, err := er.GetInt64FromPayload()
		if err != nil {
			return err
		}
		counts = append(counts, int64(len(rowIDs)))
	}

	var err error
	segment.fieldIndexes.Range(func(fieldID int64, info *IndexedFieldInfo) bool {
		if len(info.FieldBinlog.GetBinlogs()) != len(counts) {
			err = errors.New("rowID & index binlog number not matched")
			return false
		}
		for i, binlog := range info.FieldBinlog.GetBinlogs() {
			binlog.EntriesNum = counts[i]
		}
		return true
	})
	return err
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
	indexType, err := funcutil.GetAttrByKeyFromRepeatedKV(common.IndexTypeKey, indexInfo.IndexParams)
	if err != nil {
		return 0, 0, fmt.Errorf("index type not exist in index params")
	}
	if indexType == indexparamcheck.IndexDISKANN {
		neededMemSize := indexInfo.IndexSize / UsedDiskMemoryRatio
		neededDiskSize := indexInfo.IndexSize - neededMemSize
		return uint64(neededMemSize), uint64(neededDiskSize), nil
	}

	return uint64(indexInfo.IndexSize), 0, nil
}

// checkSegmentSize checks whether the memory & disk is sufficient to load the segments with given concurrency,
// returns the memory & disk usage while loading if possible to load,
// otherwise, returns error
func (loader *segmentLoader) checkSegmentSize(segmentLoadInfos []*querypb.SegmentLoadInfo, concurrency int) (uint64, uint64, error) {
	if len(segmentLoadInfos) == 0 || concurrency == 0 {
		return 0, 0, nil
	}

	log := log.With(
		zap.Int64("collectionID", segmentLoadInfos[0].GetCollectionID()),
	)

	usedMem := hardware.GetUsedMemoryCount() + loader.committedMemSize
	totalMem := hardware.GetMemoryCount()

	if usedMem == 0 || totalMem == 0 {
		return 0, 0, errors.New("get memory failed when checkSegmentSize")
	}

	usedMemAfterLoad := usedMem
	maxSegmentSize := uint64(0)

	localUsedSize, err := GetLocalUsedSize(paramtable.Get().LocalStorageCfg.Path.GetValue())
	if err != nil {
		return 0, 0, errors.Wrap(err, "get local used size failed")
	}
	diskUsed := uint64(localUsedSize) + loader.committedDiskSize
	usedLocalSizeAfterLoad := diskUsed

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
					log.Error("failed to get index size",
						zap.Int64("collectionID", loadInfo.CollectionID),
						zap.Int64("segmentID", loadInfo.SegmentID),
						zap.Int64("indexBuildID", fieldIndexInfo.BuildID),
						zap.Error(err),
					)
					return 0, 0, err
				}
				usedMemAfterLoad += neededMemSize
				usedLocalSizeAfterLoad += neededDiskSize
			} else {
				usedMemAfterLoad += uint64(getFieldSizeFromFieldBinlog(fieldBinlog))
			}
		}

		// get size of state data
		for _, fieldBinlog := range loadInfo.Statslogs {
			usedMemAfterLoad += uint64(getFieldSizeFromFieldBinlog(fieldBinlog))
		}

		// get size of delete data
		for _, fieldBinlog := range loadInfo.Deltalogs {
			usedMemAfterLoad += uint64(getFieldSizeFromFieldBinlog(fieldBinlog))
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
		float64(maxSegmentSize)*float64(concurrency)*paramtable.Get().QueryNodeCfg.LoadMemoryUsageFactor.GetAsFloat())
	log.Info("predict memory and disk usage while loading (in MiB)",
		zap.Int("concurrency", concurrency),
		zap.Uint64("memUsage", toMB(memLoadingUsage)),
		zap.Uint64("memUsageAfterLoad", toMB(usedMemAfterLoad)),
		zap.Uint64("diskUsageAfterLoad", toMB(usedLocalSizeAfterLoad)))

	if memLoadingUsage > uint64(float64(totalMem)*paramtable.Get().QueryNodeCfg.OverloadedMemoryThresholdPercentage.GetAsFloat()) {
		return 0, 0, fmt.Errorf("load segment failed, OOM if load, maxSegmentSize = %v MB, concurrency = %d, usedMemAfterLoad = %v MB, totalMem = %v MB, thresholdFactor = %f",
			toMB(maxSegmentSize),
			concurrency,
			toMB(usedMemAfterLoad),
			toMB(totalMem),
			paramtable.Get().QueryNodeCfg.OverloadedMemoryThresholdPercentage.GetAsFloat())
	}

	if usedLocalSizeAfterLoad > uint64(float64(paramtable.Get().QueryNodeCfg.DiskCapacityLimit.GetAsInt64())*paramtable.Get().QueryNodeCfg.MaxDiskUsagePercentage.GetAsFloat()) {
		return 0, 0, fmt.Errorf("load segment failed, disk space is not enough, usedDiskAfterLoad = %v MB, totalDisk = %v MB, thresholdFactor = %f",
			toMB(usedLocalSizeAfterLoad),
			toMB(uint64(paramtable.Get().QueryNodeCfg.DiskCapacityLimit.GetAsInt64())),
			paramtable.Get().QueryNodeCfg.MaxDiskUsagePercentage.GetAsFloat())
	}

	return memLoadingUsage - usedMem, usedLocalSizeAfterLoad - diskUsed, nil
}

func (loader *segmentLoader) getFieldType(segment *LocalSegment, fieldID int64) (schemapb.DataType, error) {
	collection := loader.manager.Collection.Get(segment.collectionID)
	if collection == nil {
		return 0, merr.WrapErrCollectionNotFound(segment.Collection())
	}

	for _, field := range collection.Schema().GetFields() {
		if field.GetFieldID() == fieldID {
			return field.GetDataType(), nil
		}
	}
	return 0, merr.WrapErrFieldNotFound(fieldID)
}

func getFieldSizeFromFieldBinlog(fieldBinlog *datapb.FieldBinlog) int64 {
	fieldSize := int64(0)
	for _, binlog := range fieldBinlog.Binlogs {
		fieldSize += binlog.LogSize
	}

	return fieldSize
}
