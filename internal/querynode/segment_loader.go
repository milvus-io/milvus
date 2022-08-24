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
	"path"
	"runtime"
	"runtime/debug"
	"strconv"

	"github.com/panjf2000/ants/v2"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/metrics"
	"github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/concurrency"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/timerecord"
)

const (
	requestConcurrencyLevelLimit = 8
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
	// cgoPool for all cgo invocation
	cgoPool *concurrency.Pool

	factory msgstream.Factory
}

func (loader *segmentLoader) getFieldType(segment *Segment, fieldID FieldID) (schemapb.DataType, error) {
	coll, err := loader.metaReplica.getCollectionByID(segment.collectionID)
	if err != nil {
		return schemapb.DataType_None, err
	}

	return coll.getFieldType(fieldID)
}

func (loader *segmentLoader) LoadSegment(req *querypb.LoadSegmentsRequest, segmentType segmentType) error {
	if req.Base == nil {
		return fmt.Errorf("nil base message when load segment, collectionID = %d", req.CollectionID)
	}

	// no segment needs to load, return
	segmentNum := len(req.Infos)

	if segmentNum == 0 {
		log.Warn("find no valid segment target, skip load segment", zap.Any("request", req))
		return nil
	}

	log.Info("segmentLoader start loading...",
		zap.Any("collectionID", req.CollectionID),
		zap.Any("segmentNum", segmentNum),
		zap.Any("segmentType", segmentType.String()))

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
	concurrencyLevel := min(loader.cpuPool.Cap(),
		len(req.Infos),
		requestConcurrencyLevelLimit)

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
		return err
	}

	newSegments := make(map[UniqueID]*Segment, len(req.Infos))
	segmentGC := func() {
		for _, s := range newSegments {
			deleteSegment(s)
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
			segmentGC()
			return err
		}

		segment, err := newSegment(collection, segmentID, partitionID, collectionID, vChannelID, segmentType, loader.cgoPool)
		if err != nil {
			log.Error("load segment failed when create new segment",
				zap.Int64("collectionID", collectionID),
				zap.Int64("partitionID", partitionID),
				zap.Int64("segmentID", segmentID),
				zap.String("segmentType", segmentType.String()),
				zap.Error(err))
			segmentGC()
			return err
		}

		newSegments[segmentID] = segment
	}

	loadFileFunc := func(idx int) error {
		loadInfo := req.Infos[idx]
		collectionID := loadInfo.CollectionID
		partitionID := loadInfo.PartitionID
		segmentID := loadInfo.SegmentID
		segment := newSegments[segmentID]

		tr := timerecord.NewTimeRecorder("loadDurationPerSegment")
		err := loader.loadFiles(segment, loadInfo)
		if err != nil {
			log.Error("load segment failed when load data into memory",
				zap.Int64("collectionID", collectionID),
				zap.Int64("partitionID", partitionID),
				zap.Int64("segmentID", segmentID),
				zap.String("segmentType", segmentType.String()),
				zap.Error(err))
			return err
		}

		metrics.QueryNodeLoadSegmentLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Observe(float64(tr.ElapseSpan().Milliseconds()))

		return nil
	}

	// start to load
	// Make sure we can always benefit from concurrency, and not spawn too many idle goroutines
	log.Debug("start to load segments in parallel",
		zap.Int("segmentNum", segmentNum),
		zap.Int("concurrencyLevel", concurrencyLevel))
	err = funcutil.ProcessFuncParallel(segmentNum,
		concurrencyLevel, loadFileFunc, "loadSegmentFunc")
	if err != nil {
		segmentGC()
		return err
	}

	// set segment to meta replica
	for _, s := range newSegments {
		err = loader.metaReplica.setSegment(s)
		if err != nil {
			log.Error("load segment failed, set segment to meta failed",
				zap.Int64("collectionID", s.collectionID),
				zap.Int64("partitionID", s.partitionID),
				zap.Int64("segmentID", s.segmentID),
				zap.Int64("loadSegmentRequest msgID", req.Base.MsgID),
				zap.Error(err))
			segmentGC()
			return err
		}
	}

	return nil
}

func (loader *segmentLoader) loadFiles(segment *Segment,
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
			if indexInfo != nil && indexInfo.EnableIndex {
				fieldID := indexInfo.FieldID
				fieldID2IndexInfo[fieldID] = indexInfo
			}
		}

		indexedFieldInfos := make(map[int64]*IndexedFieldInfo)
		fieldBinlogs := make([]*datapb.FieldBinlog, 0, len(loadInfo.BinlogPaths))

		for _, fieldBinlog := range loadInfo.BinlogPaths {
			fieldID := fieldBinlog.FieldID
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

		if err := loader.loadIndexedFieldData(segment, indexedFieldInfos); err != nil {
			return err
		}
		if err := loader.loadSealedSegmentFields(segment, fieldBinlogs, loadInfo); err != nil {
			return err
		}
	} else {
		if err := loader.loadGrowingSegmentFields(segment, loadInfo.BinlogPaths); err != nil {
			return err
		}
	}

	if pkFieldID == common.InvalidFieldID {
		log.Warn("segment primary key field doesn't exist when load segment")
	} else {
		log.Debug("loading bloom filter...", zap.Int64("segmentID", segmentID))
		pkStatsBinlogs := loader.filterPKStatsBinlogs(loadInfo.Statslogs, pkFieldID)
		err = loader.loadSegmentBloomFilter(segment, pkStatsBinlogs)
		if err != nil {
			return err
		}
	}

	log.Debug("loading delta...", zap.Int64("segmentID", segmentID))
	err = loader.loadDeltaLogs(segment, loadInfo.Deltalogs)
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

func (loader *segmentLoader) loadGrowingSegmentFields(segment *Segment, fieldBinlogs []*datapb.FieldBinlog) error {
	if len(fieldBinlogs) <= 0 {
		return nil
	}

	segmentType := segment.getType()
	iCodec := storage.InsertCodec{}

	// change all field bin log loading into concurrent
	loadFutures := make([]*concurrency.Future, 0, len(fieldBinlogs))
	for _, fieldBinlog := range fieldBinlogs {
		futures := loader.loadFieldBinlogsAsync(fieldBinlog)
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
	log.Info("load field binlogs done for growing segment",
		zap.Int64("collection", segment.collectionID),
		zap.Int64("segment", segment.segmentID),
		zap.Int("len(field)", len(fieldBinlogs)),
		zap.String("segmentType", segmentType.String()))

	_, _, insertData, err := iCodec.Deserialize(blobs)
	if err != nil {
		log.Warn(err.Error())
		return err
	}

	switch segmentType {
	case segmentTypeGrowing:
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

		return loader.loadGrowingSegments(segment, rowIDData.(*storage.Int64FieldData).Data, utss, insertData)

	default:
		err := fmt.Errorf("illegal segmentType=%s when load segment, collectionID=%v", segmentType.String(), segment.collectionID)
		return err
	}
}

func (loader *segmentLoader) loadSealedSegmentFields(segment *Segment, fields []*datapb.FieldBinlog, loadInfo *querypb.SegmentLoadInfo) error {
	// Load fields concurrently
	futures := make([]*concurrency.Future, 0, len(fields))
	for _, field := range fields {
		future := loader.loadSealedFieldAsync(segment, field, loadInfo)

		futures = append(futures, future)
	}

	err := concurrency.AwaitAll(futures...)
	if err != nil {
		return err
	}

	log.Info("load field binlogs done for sealed segment",
		zap.Int64("collection", segment.collectionID),
		zap.Int64("segment", segment.segmentID),
		zap.Int("len(field)", len(fields)),
		zap.String("segmentType", segment.getType().String()))

	return nil
}

// async load field of sealed segment
func (loader *segmentLoader) loadSealedFieldAsync(segment *Segment, field *datapb.FieldBinlog, loadInfo *querypb.SegmentLoadInfo) *concurrency.Future {
	iCodec := storage.InsertCodec{}

	// Avoid consuming too much memory if no CPU worker ready,
	// acquire a CPU worker before load field binlogs
	return loader.cpuPool.Submit(func() (interface{}, error) {
		futures := loader.loadFieldBinlogsAsync(field)

		blobs := make([]*storage.Blob, len(futures))
		for index, future := range futures {
			if !future.OK() {
				return nil, future.Err()
			}

			blob := future.Value().(*storage.Blob)
			blobs[index] = blob
		}

		insertData := storage.InsertData{
			Data: make(map[int64]storage.FieldData),
		}
		_, _, _, err := iCodec.DeserializeInto(blobs, int(loadInfo.GetNumOfRows()), &insertData)
		if err != nil {
			log.Warn(err.Error())
			return nil, err
		}

		return nil, loader.loadSealedSegments(segment, &insertData)
	})
}

// Load binlogs concurrently into memory from KV storage asyncly
func (loader *segmentLoader) loadFieldBinlogsAsync(field *datapb.FieldBinlog) []*concurrency.Future {
	futures := make([]*concurrency.Future, 0, len(field.Binlogs))
	for i := range field.Binlogs {
		path := field.Binlogs[i].GetLogPath()
		future := loader.ioPool.Submit(func() (interface{}, error) {
			binLog, err := loader.cm.Read(path)
			if err != nil {
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

func (loader *segmentLoader) loadIndexedFieldData(segment *Segment, vecFieldInfos map[int64]*IndexedFieldInfo) error {
	for fieldID, fieldInfo := range vecFieldInfos {
		indexInfo := fieldInfo.indexInfo
		err := loader.loadFieldIndexData(segment, indexInfo)
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

func (loader *segmentLoader) loadFieldIndexData(segment *Segment, indexInfo *querypb.FieldIndexInfo) error {
	indexBuffer := make([][]byte, 0, len(indexInfo.IndexFilePaths))
	filteredPaths := make([]string, 0, len(indexInfo.IndexFilePaths))
	futures := make([]*concurrency.Future, 0, len(indexInfo.IndexFilePaths))
	indexCodec := storage.NewIndexFileBinlogCodec()

	for _, p := range indexInfo.IndexFilePaths {
		indexPath := p
		if path.Base(indexPath) != storage.IndexParamsKey {
			indexFuture := loader.cpuPool.Submit(func() (interface{}, error) {
				indexBlobFuture := loader.ioPool.Submit(func() (interface{}, error) {
					log.Debug("load index file", zap.String("path", indexPath))
					return loader.cm.Read(indexPath)
				})

				indexBlob, err := indexBlobFuture.Await()
				if err != nil {
					return nil, err
				}

				data, _, _, _, err := indexCodec.Deserialize([]*storage.Blob{{Key: path.Base(indexPath), Value: indexBlob.([]byte)}})
				return data, err
			})

			futures = append(futures, indexFuture)
			filteredPaths = append(filteredPaths, indexPath)
		}
	}

	err := concurrency.AwaitAll(futures...)
	if err != nil {
		return err
	}

	for _, index := range futures {
		blobs := index.Value().([]*storage.Blob)
		indexBuffer = append(indexBuffer, blobs[0].Value)
	}

	// 2. use index bytes and index path to update segment
	indexInfo.IndexFilePaths = filteredPaths
	fieldType, err := loader.getFieldType(segment, indexInfo.FieldID)
	if err != nil {
		return err
	}
	return segment.segmentLoadIndexData(indexBuffer, indexInfo, fieldType)
}

func (loader *segmentLoader) loadGrowingSegments(segment *Segment,
	ids []UniqueID,
	timestamps []Timestamp,
	insertData *storage.InsertData) error {
	numRows := len(ids)
	if numRows != len(timestamps) || insertData == nil {
		return errors.New(fmt.Sprintln("illegal insert data when load segment, collectionID = ", segment.collectionID))
	}

	log.Info("start load growing segments...",
		zap.Any("collectionID", segment.collectionID),
		zap.Any("segmentID", segment.ID()),
		zap.Any("numRows", len(ids)),
	)

	// 1. do preInsert
	var numOfRecords = len(ids)
	offset, err := segment.segmentPreInsert(numOfRecords)
	if err != nil {
		return err
	}
	log.Debug("insertNode operator", zap.Int("insert size", numOfRecords), zap.Int64("insert offset", offset), zap.Int64("segment id", segment.ID()))

	// 2. update bloom filter
	insertRecord, err := storage.TransferInsertDataToInsertRecord(insertData)
	if err != nil {
		return err
	}
	tmpInsertMsg := &msgstream.InsertMsg{
		InsertRequest: internalpb.InsertRequest{
			CollectionID: segment.collectionID,
			Timestamps:   timestamps,
			RowIDs:       ids,
			NumRows:      uint64(numRows),
			FieldsData:   insertRecord.FieldsData,
			Version:      internalpb.InsertDataVersion_ColumnBased,
		},
	}
	pks, err := getPrimaryKeys(tmpInsertMsg, loader.metaReplica)
	if err != nil {
		return err
	}
	segment.updateBloomFilter(pks)

	// 3. do insert
	err = segment.segmentInsert(offset, ids, timestamps, insertRecord)
	if err != nil {
		return err
	}
	log.Info("Do insert done fro growing segment ", zap.Int("len", numOfRecords), zap.Int64("segmentID", segment.ID()), zap.Int64("collectionID", segment.collectionID))

	return nil
}

func (loader *segmentLoader) loadSealedSegments(segment *Segment, insertData *storage.InsertData) error {
	insertRecord, err := storage.TransferInsertDataToInsertRecord(insertData)
	if err != nil {
		return err
	}
	numRows := insertRecord.NumRows
	for _, fieldData := range insertRecord.FieldsData {
		fieldID := fieldData.FieldId
		if fieldID == common.TimeStampField {
			timestampsData := insertData.Data[fieldID].(*storage.Int64FieldData)
			segment.setIDBinlogRowSizes(timestampsData.NumRows)
		}

		err := segment.segmentLoadFieldData(fieldID, numRows, fieldData)
		if err != nil {
			// TODO: return or continue?
			return err
		}
	}
	return nil
}

func (loader *segmentLoader) loadSegmentBloomFilter(segment *Segment, binlogPaths []string) error {
	if len(binlogPaths) == 0 {
		log.Info("there are no stats logs saved with segment", zap.Any("segmentID", segment.segmentID))
		return nil
	}

	values, err := loader.cm.MultiRead(binlogPaths)
	if err != nil {
		return err
	}
	blobs := make([]*storage.Blob, 0)
	for i := 0; i < len(values); i++ {
		blobs = append(blobs, &storage.Blob{Value: values[i]})
	}

	stats, err := storage.DeserializeStats(blobs)
	if err != nil {
		return err
	}
	for _, stat := range stats {
		if stat.BF == nil {
			log.Warn("stat log with nil bloom filter", zap.Int64("segmentID", segment.segmentID), zap.Any("stat", stat))
			continue
		}
		err = segment.pkFilter.Merge(stat.BF)
		if err != nil {
			return err
		}
	}
	return nil
}

func (loader *segmentLoader) loadDeltaLogs(segment *Segment, deltaLogs []*datapb.FieldBinlog) error {
	dCodec := storage.DeleteCodec{}
	var blobs []*storage.Blob
	for _, deltaLog := range deltaLogs {
		for _, bLog := range deltaLog.GetBinlogs() {
			value, err := loader.cm.Read(bLog.GetLogPath())
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

func (loader *segmentLoader) FromDmlCPLoadDelete(ctx context.Context, collectionID int64, position *internalpb.MsgPosition) error {
	log.Info("from dml check point load delete", zap.Any("position", position))
	stream, err := loader.factory.NewMsgStream(ctx)
	if err != nil {
		return err
	}

	defer func() {
		metrics.QueryNodeNumConsumers.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.GetNodeID())).Dec()
		stream.Close()
	}()

	pChannelName := funcutil.ToPhysicalChannel(position.ChannelName)
	position.ChannelName = pChannelName

	stream.AsConsumer([]string{pChannelName}, fmt.Sprintf("querynode-%d-%d", Params.QueryNodeCfg.GetNodeID(), collectionID))
	lastMsgID, err := stream.GetLatestMsgID(pChannelName)
	if err != nil {
		return err
	}

	reachLatest, _ := lastMsgID.Equal(position.MsgID)
	if reachLatest || lastMsgID.AtEarliestPosition() {
		log.Info("there is no more delta msg", zap.Int64("Collection ID", collectionID), zap.String("channel", pChannelName))
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
		zap.Int64("Collection ID", collectionID), zap.String("channel", pChannelName), zap.Any("seek pos", position), zap.Any("last msg", lastMsgID))
	hasMore := true
	for hasMore {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msgPack, ok := <-stream.Chan():
			if !ok {
				err = fmt.Errorf("%w: pChannelName=%v, msgID=%v",
					ErrReadDeltaMsgFailed,
					pChannelName,
					position.GetMsgID())
				log.Warn("fail to read delta msg",
					zap.String("pChannelName", pChannelName),
					zap.ByteString("msgID", position.GetMsgID()),
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
					log.Debug("delete pk",
						zap.Any("pk", dmsg.PrimaryKeys),
						zap.String("vChannelName", position.GetChannelName()),
						zap.Any("msg id", position.GetMsgID()),
					)
					err = processDeleteMessages(loader.metaReplica, segmentTypeSealed, dmsg, delData)
					if err != nil {
						// TODO: panic?
						// error occurs when missing meta info or unexpected pk type, should not happen
						err = fmt.Errorf("deleteNode processDeleteMessages failed, collectionID = %d, err = %s", dmsg.CollectionID, err)
						log.Error(err.Error())
						return err
					}
				}

				ret, err := lastMsgID.LessOrEqualThan(tsMsg.Position().MsgID)
				if err != nil {
					log.Warn("check whether current MsgID less than last MsgID failed",
						zap.Int64("Collection ID", collectionID), zap.String("channel", pChannelName), zap.Error(err))
					return err
				}

				if ret {
					hasMore = false
					break
				}
			}
		}
	}

	log.Info("All data has been read, there is no more data", zap.Int64("Collection ID", collectionID),
		zap.String("channel", pChannelName), zap.Any("msg id", position.GetMsgID()))
	for segmentID, pks := range delData.deleteIDs {
		segment, err := loader.metaReplica.getSegmentByID(segmentID, segmentTypeSealed)
		if err != nil {
			log.Warn(err.Error())
			continue
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

	log.Info("from dml check point load done", zap.Any("msg id", position.GetMsgID()))
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

func (loader *segmentLoader) checkSegmentSize(collectionID UniqueID, segmentLoadInfos []*querypb.SegmentLoadInfo, concurrency int) error {
	usedMem := metricsinfo.GetUsedMemoryCount()
	totalMem := metricsinfo.GetMemoryCount()
	if len(segmentLoadInfos) < concurrency {
		concurrency = len(segmentLoadInfos)
	}

	if usedMem == 0 || totalMem == 0 {
		return fmt.Errorf("get memory failed when checkSegmentSize, collectionID = %d", collectionID)
	}

	usedMemAfterLoad := usedMem
	maxSegmentSize := uint64(0)
	for _, loadInfo := range segmentLoadInfos {
		segmentSize := uint64(loadInfo.SegmentSize)
		usedMemAfterLoad += segmentSize
		if segmentSize > maxSegmentSize {
			maxSegmentSize = segmentSize
		}
	}

	toMB := func(mem uint64) uint64 {
		return mem / 1024 / 1024
	}

	// when load segment, data will be copied from go memory to c++ memory
	loadingUsage := usedMemAfterLoad + uint64(
		float64(maxSegmentSize)*float64(concurrency)*Params.QueryNodeCfg.LoadMemoryUsageFactor)
	log.Debug("predict memory usage while loading (in MiB)",
		zap.Int64("collectionID", collectionID),
		zap.Int("concurrency", concurrency),
		zap.Uint64("usage", toMB(loadingUsage)),
		zap.Uint64("usageAfterLoad", toMB(usedMemAfterLoad)))

	if loadingUsage > uint64(float64(totalMem)*Params.QueryNodeCfg.OverloadedMemoryThresholdPercentage) {
		return fmt.Errorf("load segment failed, OOM if load, collectionID = %d, maxSegmentSize = %v MB, concurrency = %d, usedMemAfterLoad = %v MB, totalMem = %v MB, thresholdFactor = %f",
			collectionID,
			toMB(maxSegmentSize),
			concurrency,
			toMB(usedMemAfterLoad),
			toMB(totalMem),
			Params.QueryNodeCfg.OverloadedMemoryThresholdPercentage)
	}

	return nil
}

func newSegmentLoader(
	metaReplica ReplicaInterface,
	etcdKV *etcdkv.EtcdKV,
	cm storage.ChunkManager,
	factory msgstream.Factory,
	pool *concurrency.Pool) *segmentLoader {

	cpuNum := runtime.GOMAXPROCS(0)
	// This error is not nil only if the options of creating pool is invalid
	cpuPool, err := concurrency.NewPool(cpuNum, ants.WithPreAlloc(true))
	if err != nil {
		log.Error("failed to create goroutine pool for segment loader",
			zap.Error(err))
		panic(err)
	}

	ioPoolSize := cpuNum * 2
	if ioPoolSize < 32 {
		ioPoolSize = 32
	}
	ioPool, err := concurrency.NewPool(ioPoolSize, ants.WithPreAlloc(true))
	if err != nil {
		log.Error("failed to create goroutine pool for segment loader",
			zap.Error(err))
		panic(err)
	}

	log.Info("SegmentLoader created",
		zap.Int("cpu-pool-size", cpuNum),
		zap.Int("io-pool-size", ioPoolSize))

	loader := &segmentLoader{
		metaReplica: metaReplica,

		cm:     cm,
		etcdKV: etcdKV,

		// init them later
		ioPool:  ioPool,
		cpuPool: cpuPool,
		cgoPool: pool,

		factory: factory,
	}

	return loader
}
