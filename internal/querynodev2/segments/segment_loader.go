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
	"errors"
	"fmt"
	"path"
	"runtime"
	"runtime/debug"
	"strconv"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/concurrency"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/hardware"
	"github.com/milvus-io/milvus/internal/util/indexparamcheck"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/timerecord"
	. "github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/panjf2000/ants/v2"
	"github.com/samber/lo"
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
	Load(ctx context.Context, collectionID int64, segmentType SegmentType, version int64, infos ...*querypb.SegmentLoadInfo) ([]Segment, error)

	LoadDeltaLogs(ctx context.Context, segment *LocalSegment, deltaLogs []*datapb.FieldBinlog) error

	// LoadRemote loads needed binlogs/statslog for RemoteSegment.
	LoadBloomFilterSet(ctx context.Context, collectionID int64, version int64, infos ...*querypb.SegmentLoadInfo) ([]*pkoracle.BloomFilterSet, error)
}

// segmentLoader is only responsible for loading the field data from binlog
type segmentLoader struct {
	manager CollectionManager
	cm      storage.ChunkManager // minio cm
	ioPool  *concurrency.Pool
}

var _ Loader = (*segmentLoader)(nil)

func (loader *segmentLoader) getFieldType(segment *LocalSegment, fieldID int64) (schemapb.DataType, error) {
	collection := loader.manager.Get(segment.collectionID)
	if collection == nil {
		return 0, WrapCollectionNotFound(segment.Collection())
	}

	for _, field := range collection.Schema().GetFields() {
		if field.GetFieldID() == fieldID {
			return field.GetDataType(), nil
		}
	}
	return 0, WrapFieldNotFound(fieldID)
}

func (loader *segmentLoader) Load(ctx context.Context,
	collectionID int64,
	segmentType SegmentType,
	version int64,
	infos ...*querypb.SegmentLoadInfo,
) ([]Segment, error) {
	log := log.Ctx(ctx).With(
		zap.Int64("collectionID", collectionID),
		zap.String("segmentType", segmentType.String()),
	)

	segmentNum := len(infos)
	if segmentNum == 0 {
		log.Info("no segment to load")
		return nil, nil
	}

	log.Info("start loading...", zap.Int("segmentNum", segmentNum))

	// Check memory limit
	var (
		concurrencyLevel = funcutil.Min(runtime.GOMAXPROCS(0), len(infos))
		err              error
	)
	for ; concurrencyLevel > 1; concurrencyLevel /= 2 {
		err = loader.checkSegmentSize(collectionID, infos, concurrencyLevel)
		if err == nil {
			break
		}
	}
	if err != nil {
		log.Error("load failed, OOM if loaded", zap.Error(err))
		return nil, err
	}

	newSegments := make(map[UniqueID]*LocalSegment, len(infos))
	clearAll := func() {
		for _, s := range newSegments {
			DeleteSegment(s)
		}
		debug.FreeOSMemory()
	}
	loadedSegments := NewConcurrentSet[*LocalSegment]()

	for _, info := range infos {
		segmentID := info.SegmentID
		partitionID := info.PartitionID
		collectionID := info.CollectionID
		shard := info.InsertChannel

		collection := loader.manager.Get(collectionID)
		if collection == nil {
			err := WrapCollectionNotFound(collectionID)
			log.Warn("failed to get collection", zap.Error(err))
			clearAll()
			return nil, err
		}
		segment, err := NewSegment(collection, segmentID, partitionID, collectionID, shard, segmentType, version, info.GetStartPosition(), info.GetEndPosition())
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
		loadedSegments.Insert(segment)

		metrics.QueryNodeLoadSegmentLatency.WithLabelValues(fmt.Sprint(paramtable.GetNodeID())).Observe(float64(tr.ElapseSpan().Milliseconds()))

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
		log.Warn("failed to load some segments", zap.Error(err))
	}

	loaded := loadedSegments.Collect()
	if len(loaded) != len(newSegments) {
		// Free the memory of segments which failed to load
		for _, segment := range newSegments {
			if !loadedSegments.Contain(segment) {
				DeleteSegment(segment)
			}
		}
		debug.FreeOSMemory()

		loadedIDs := lo.Map(loaded, func(segment *LocalSegment, _ int) int64 {
			return segment.ID()
		})
		log.Info("partial segments are loaded", zap.Int64s("loadedSegments", loadedIDs))
	}

	return lo.Map(loaded, func(segment *LocalSegment, _ int) Segment { return segment }), nil
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

	collection := loader.manager.Get(collectionID)
	if collection == nil {
		err := WrapCollectionNotFound(collectionID)
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
		pkStatsBinlogs := loader.filterPKStatsBinlogs(loadInfo.Statslogs, pkField.GetFieldID())
		err := loader.loadBloomFilter(ctx, segmentID, bfs, pkStatsBinlogs)
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

	collection := loader.manager.Get(segment.Collection())
	if collection == nil {
		err := WrapCollectionNotFound(segment.Collection())
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
		if err := loader.loadSealedSegmentFields(ctx, segment, fieldBinlogs, loadInfo); err != nil {
			return err
		}
	} else {
		if err := loader.loadGrowingSegmentFields(ctx, segment, loadInfo.BinlogPaths); err != nil {
			return err
		}
	}

	// load statslog if it's growing segment
	if segment.typ == SegmentTypeGrowing {
		log.Info("loading statslog...")
		pkStatsBinlogs := loader.filterPKStatsBinlogs(loadInfo.Statslogs, pkField.GetFieldID())
		err := loader.loadBloomFilter(ctx, segment.segmentID, segment.bloomFilterSet, pkStatsBinlogs)
		if err != nil {
			return err
		}
	}

	log.Info("loading delta...")
	return loader.LoadDeltaLogs(ctx, segment, loadInfo.Deltalogs)
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

func (loader *segmentLoader) loadGrowingSegmentFields(ctx context.Context, segment *LocalSegment, fieldBinlogs []*datapb.FieldBinlog) error {
	if len(fieldBinlogs) <= 0 {
		return nil
	}

	segmentType := segment.Type()
	iCodec := storage.InsertCodec{}

	// change all field bin log loading into concurrent
	loadFutures := make([]*concurrency.Future, 0, len(fieldBinlogs))
	for _, fieldBinlog := range fieldBinlogs {
		futures := loader.loadFieldBinlogsAsync(ctx, fieldBinlog)
		loadFutures = append(loadFutures, futures...)
	}

	// wait for async load results
	blobs := make([]*storage.Blob, len(loadFutures))
	for index, future := range loadFutures {
		if !future.OK() {
			return future.Err()
		}

		blob := future.Value().(*storage.Blob)
		blobs[index] = blob
	}
	log.Info("log field binlogs done",
		zap.Int64("collection", segment.collectionID),
		zap.Int64("segment", segment.segmentID),
		zap.Any("field", fieldBinlogs),
		zap.String("segmentType", segmentType.String()))

	_, _, insertData, err := iCodec.Deserialize(blobs)
	if err != nil {
		log.Warn("failed to deserialize", zap.Int64("segment", segment.segmentID), zap.Error(err))
		return err
	}

	switch segmentType {
	case SegmentTypeGrowing:
		tsData, ok := insertData.Data[common.TimeStampField]
		if !ok {
			return errors.New("cannot get timestamps from insert data")
		}
		utss := make([]uint64, tsData.RowNum())
		for i := 0; i < tsData.RowNum(); i++ {
			utss[i] = uint64(tsData.GetRow(i).(int64))
		}

		rowIDData, ok := insertData.Data[common.RowIDField]
		if !ok {
			return errors.New("cannot get row ids from insert data")
		}

		return loader.insertIntoSegment(segment, rowIDData.(*storage.Int64FieldData).Data, utss, insertData)

	default:
		err := fmt.Errorf("illegal segmentType=%s when load segment, collectionID=%v", segmentType.String(), segment.collectionID)
		return err
	}
}

func (loader *segmentLoader) loadSealedSegmentFields(ctx context.Context, segment *LocalSegment, fields []*datapb.FieldBinlog, loadInfo *querypb.SegmentLoadInfo) error {
	runningGroup, groupCtx := errgroup.WithContext(ctx)
	for _, field := range fields {
		fieldBinLog := field
		runningGroup.Go(func() error {
			return loader.loadSealedField(groupCtx, segment, fieldBinLog, loadInfo)
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

// async load field of sealed segment
func (loader *segmentLoader) loadSealedField(ctx context.Context, segment *LocalSegment, field *datapb.FieldBinlog, loadInfo *querypb.SegmentLoadInfo) error {
	iCodec := storage.InsertCodec{}

	// Avoid consuming too much memory if no CPU worker ready,
	// acquire a CPU worker before load field binlogs
	futures := loader.loadFieldBinlogsAsync(ctx, field)

	err := concurrency.AwaitAll(futures...)
	if err != nil {
		return err
	}

	blobs := make([]*storage.Blob, len(futures))
	for index, future := range futures {
		blob := future.Value().(*storage.Blob)
		blobs[index] = blob
	}

	insertData := storage.InsertData{
		Data: make(map[int64]storage.FieldData),
	}
	_, _, _, err = iCodec.DeserializeInto(blobs, int(loadInfo.GetNumOfRows()), &insertData)
	if err != nil {
		log.Warn("failed to load sealed field", zap.Int64("SegmentId", segment.segmentID), zap.Error(err))
		return err
	}

	return loader.loadSealedSegments(segment, &insertData)
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
	indexBuffer := make([][]byte, 0, len(indexInfo.IndexFilePaths))
	filteredPaths := make([]string, 0, len(indexInfo.IndexFilePaths))
	futures := make([]*concurrency.Future, 0, len(indexInfo.IndexFilePaths))
	indexCodec := storage.NewIndexFileBinlogCodec()

	// TODO, remove the load index info froam
	for _, indexPath := range indexInfo.IndexFilePaths {
		// get index params when detecting indexParamPrefix
		if path.Base(indexPath) == storage.IndexParamsKey {
			log.Info("load index params file", zap.String("path", indexPath))
			indexParamsBlob, err := loader.cm.Read(ctx, indexPath)
			if err != nil {
				return err
			}

			_, indexParams, _, _, err := indexCodec.Deserialize([]*storage.Blob{{Key: storage.IndexParamsKey, Value: indexParamsBlob}})
			if err != nil {
				return err
			}

			// update index params(dim...)
			newIndexParams := funcutil.KeyValuePair2Map(indexInfo.IndexParams)
			for key, value := range indexParams {
				newIndexParams[key] = value
			}
			indexInfo.IndexParams = funcutil.Map2KeyValuePair(newIndexParams)
			continue
		}

		filteredPaths = append(filteredPaths, indexPath)
	}

	// 2. use index bytes and index path to update segment
	indexInfo.IndexFilePaths = filteredPaths
	fieldType, err := loader.getFieldType(segment, indexInfo.FieldID)
	if err != nil {
		return err
	}

	indexParams := funcutil.KeyValuePair2Map(indexInfo.IndexParams)
	// load on disk index
	if indexParams["index_type"] == indexparamcheck.IndexDISKANN {
		return segment.LoadIndex(nil, indexInfo, fieldType)
	}
	// load in memory index
	for _, p := range indexInfo.IndexFilePaths {
		indexPath := p
		indexFuture := loader.ioPool.Submit(func() (interface{}, error) {
			log.Info("load index file", zap.String("path", indexPath))
			data, err := loader.cm.Read(ctx, indexPath)
			if err != nil {
				log.Warn("failed to load index file", zap.String("path", indexPath), zap.Error(err))
				return nil, err
			}
			blobs, _, _, _, err := indexCodec.Deserialize([]*storage.Blob{{Key: path.Base(indexPath), Value: data}})
			return blobs, err
		})

		futures = append(futures, indexFuture)
	}

	err = concurrency.AwaitAll(futures...)
	if err != nil {
		return err
	}

	for _, index := range futures {
		blobs := index.Value().([]*storage.Blob)
		indexBuffer = append(indexBuffer, blobs[0].Value)
	}

	return segment.LoadIndex(indexBuffer, indexInfo, fieldType)
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
		InsertRequest: internalpb.InsertRequest{
			CollectionID: segment.collectionID,
			Timestamps:   timestamps,
			RowIDs:       rowIDs,
			NumRows:      uint64(rowNum),
			FieldsData:   insertRecord.FieldsData,
			Version:      internalpb.InsertDataVersion_ColumnBased,
		},
	}
	collection := loader.manager.Get(segment.Collection())
	if collection == nil {
		err := WrapCollectionNotFound(segment.Collection())
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

func (loader *segmentLoader) loadSealedSegments(segment *LocalSegment, insertData *storage.InsertData) error {
	insertRecord, err := storage.TransferInsertDataToInsertRecord(insertData)
	if err != nil {
		return err
	}
	numRows := insertRecord.NumRows
	for _, fieldData := range insertRecord.FieldsData {
		err := segment.LoadField(numRows, fieldData)
		if err != nil {
			// TODO: return or continue?
			return err
		}
	}
	return nil
}

func (loader *segmentLoader) loadBloomFilter(ctx context.Context, segmentID int64, bfs *pkoracle.BloomFilterSet, binlogPaths []string) error {
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
		neededDiskSize := indexInfo.IndexSize - neededMemSize
		return uint64(neededMemSize), uint64(neededDiskSize), nil
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
		float64(maxSegmentSize)*float64(concurrency)*paramtable.Get().QueryNodeCfg.LoadMemoryUsageFactor.GetAsFloat())
	log.Info("predict memory and disk usage while loading (in MiB)",
		zap.Int64("collectionID", collectionID),
		zap.Int("concurrency", concurrency),
		zap.Uint64("memUsage", toMB(memLoadingUsage)),
		zap.Uint64("memUsageAfterLoad", toMB(usedMemAfterLoad)),
		zap.Uint64("diskUsageAfterLoad", toMB(usedLocalSizeAfterLoad)))

	if memLoadingUsage > uint64(float64(totalMem)*paramtable.Get().QueryNodeCfg.OverloadedMemoryThresholdPercentage.GetAsFloat()) {
		return fmt.Errorf("load segment failed, OOM if load, collectionID = %d, maxSegmentSize = %v MB, concurrency = %d, usedMemAfterLoad = %v MB, totalMem = %v MB, thresholdFactor = %f",
			collectionID,
			toMB(maxSegmentSize),
			concurrency,
			toMB(usedMemAfterLoad),
			toMB(totalMem),
			paramtable.Get().QueryNodeCfg.OverloadedMemoryThresholdPercentage.GetAsFloat())
	}

	if usedLocalSizeAfterLoad > uint64(float64(paramtable.Get().QueryNodeCfg.DiskCapacityLimit.GetAsInt64())*paramtable.Get().QueryNodeCfg.MaxDiskUsagePercentage.GetAsFloat()) {
		return fmt.Errorf("load segment failed, disk space is not enough, collectionID = %d, usedDiskAfterLoad = %v MB, totalDisk = %v MB, thresholdFactor = %f",
			collectionID,
			toMB(usedLocalSizeAfterLoad),
			toMB(uint64(paramtable.Get().QueryNodeCfg.DiskCapacityLimit.GetAsInt64())),
			paramtable.Get().QueryNodeCfg.MaxDiskUsagePercentage.GetAsFloat())
	}

	return nil
}

func NewLoader(
	manager CollectionManager,
	cm storage.ChunkManager,
	pool *concurrency.Pool) *segmentLoader {

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

	log.Info("SegmentLoader created", zap.Int("io-pool-size", ioPoolSize))

	loader := &segmentLoader{
		manager: manager,
		cm:      cm,
		ioPool:  ioPool,
	}

	return loader
}
