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
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/common"
	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	minioKV "github.com/milvus-io/milvus/internal/kv/minio"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
)

const timeoutForEachRead = 10 * time.Second

// segmentLoader is only responsible for loading the field data from binlog
type segmentLoader struct {
	historicalReplica ReplicaInterface
	streamingReplica  ReplicaInterface

	dataCoord types.DataCoord

	minioKV kv.DataKV // minio minioKV
	etcdKV  *etcdkv.EtcdKV

	indexLoader *indexLoader

	factory msgstream.Factory
}

func (loader *segmentLoader) loadSegment(req *querypb.LoadSegmentsRequest, segmentType segmentType) error {
	// no segment needs to load, return
	if len(req.Infos) == 0 {
		return nil
	}

	log.Debug("segmentLoader start loading...",
		zap.Any("collectionID", req.CollectionID),
		zap.Any("numOfSegments", len(req.Infos)),
		zap.Any("loadType", segmentType),
	)

	newSegments := make(map[UniqueID]*Segment)
	segmentGC := func() {
		for _, s := range newSegments {
			deleteSegment(s)
		}
	}

	segmentFieldBinLogs := make(map[UniqueID][]*datapb.FieldBinlog)
	segmentIndexedFieldIDs := make(map[UniqueID][]FieldID)
	segmentSizes := make(map[UniqueID]int64)

	// prepare and estimate segments size
	for _, info := range req.Infos {
		segmentID := info.SegmentID
		partitionID := info.PartitionID
		collectionID := info.CollectionID

		collection, err := loader.historicalReplica.getCollectionByID(collectionID)
		if err != nil {
			segmentGC()
			return err
		}
		segment := newSegment(collection, segmentID, partitionID, collectionID, "", segmentType, true)
		newSegments[segmentID] = segment
		fieldBinlog, indexedFieldID, err := loader.getFieldAndIndexInfo(segment, info)
		if err != nil {
			segmentGC()
			return err
		}
		segmentSize, err := loader.estimateSegmentSize(segment, fieldBinlog, indexedFieldID)
		if err != nil {
			segmentGC()
			return err
		}
		segmentFieldBinLogs[segmentID] = fieldBinlog
		segmentIndexedFieldIDs[segmentID] = indexedFieldID
		segmentSizes[segmentID] = segmentSize
	}

	// check memory limit
	err := loader.checkSegmentSize(req.Infos[0].CollectionID, segmentSizes)
	if err != nil {
		segmentGC()
		return err
	}

	// start to load
	for _, info := range req.Infos {
		segmentID := info.SegmentID
		if newSegments[segmentID] == nil || segmentFieldBinLogs[segmentID] == nil || segmentIndexedFieldIDs[segmentID] == nil {
			segmentGC()
			return errors.New(fmt.Sprintln("unexpected error, cannot find load infos, this error should not happen, collectionID = ", req.Infos[0].CollectionID))
		}
		err = loader.loadSegmentInternal(newSegments[segmentID],
			segmentFieldBinLogs[segmentID],
			segmentIndexedFieldIDs[segmentID],
			info,
			segmentType)
		if err != nil {
			segmentGC()
			return err
		}
	}

	// set segments
	switch segmentType {
	case segmentTypeGrowing:
		for _, s := range newSegments {
			err := loader.streamingReplica.setSegment(s)
			if err != nil {
				segmentGC()
				return err
			}
		}
	case segmentTypeSealed:
		for _, s := range newSegments {
			err := loader.historicalReplica.setSegment(s)
			if err != nil {
				segmentGC()
				return err
			}
		}
	default:
		err := errors.New(fmt.Sprintln("illegal segment type when load segment, collectionID = ", req.CollectionID))
		segmentGC()
		return err
	}
	return nil
}

func (loader *segmentLoader) loadSegmentInternal(segment *Segment,
	fieldBinLogs []*datapb.FieldBinlog,
	indexFieldIDs []FieldID,
	segmentLoadInfo *querypb.SegmentLoadInfo,
	segmentType segmentType) error {
	log.Debug("loading insert...",
		zap.Any("collectionID", segment.collectionID),
		zap.Any("segmentID", segment.ID()),
		zap.Any("segmentType", segmentType),
		zap.Any("fieldBinLogs", fieldBinLogs),
		zap.Any("indexFieldIDs", indexFieldIDs),
	)
	err := loader.loadSegmentFieldsData(segment, fieldBinLogs, segmentType)
	if err != nil {
		return err
	}

	pkIDField, err := loader.historicalReplica.getPKFieldIDByCollectionID(segment.collectionID)
	if err != nil {
		return err
	}
	if pkIDField == common.InvalidFieldID {
		log.Warn("segment primary key field doesn't exist when load segment")
	} else {
		log.Debug("loading bloom filter...")
		pkStatsBinlogs := loader.filterPKStatsBinlogs(segmentLoadInfo.Statslogs, pkIDField)
		err = loader.loadSegmentBloomFilter(segment, pkStatsBinlogs)
		if err != nil {
			return err
		}
	}

	log.Debug("loading delta...")
	err = loader.loadDeltaLogs(segment, segmentLoadInfo.Deltalogs)
	if err != nil {
		return err
	}

	for _, id := range indexFieldIDs {
		log.Debug("loading index...")
		err = loader.indexLoader.loadIndex(segment, id)
		if err != nil {
			return err
		}
	}

	return nil
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

func (loader *segmentLoader) filterFieldBinlogs(fieldBinlogs []*datapb.FieldBinlog, skipFieldIDs []int64) []*datapb.FieldBinlog {
	result := make([]*datapb.FieldBinlog, 0)
	for _, fieldBinlog := range fieldBinlogs {
		if !funcutil.SliceContain(skipFieldIDs, fieldBinlog.FieldID) {
			result = append(result, fieldBinlog)
		}
	}
	return result
}

func (loader *segmentLoader) loadSegmentFieldsData(segment *Segment, fieldBinlogs []*datapb.FieldBinlog, segmentType segmentType) error {
	iCodec := storage.InsertCodec{}
	blobs := make([]*storage.Blob, 0)
	for _, fb := range fieldBinlogs {
		log.Debug("load segment fields data",
			zap.Int64("segmentID", segment.segmentID),
			zap.Any("fieldID", fb.FieldID),
			zap.String("paths", fmt.Sprintln(fb.Binlogs)),
		)
		for _, path := range fb.Binlogs {
			binLog, err := loader.minioKV.Load(path.GetLogPath())
			if err != nil {
				// TODO: return or continue?
				return err
			}
			blob := &storage.Blob{
				Key:   path.GetLogPath(),
				Value: []byte(binLog),
			}
			blobs = append(blobs, blob)
		}
	}

	_, _, insertData, err := iCodec.Deserialize(blobs)
	if err != nil {
		log.Warn(err.Error())
		return err
	}

	for i := range insertData.Infos {
		log.Debug("segmentLoader deserialize fields",
			zap.Any("collectionID", segment.collectionID),
			zap.Any("segmentID", segment.ID()),
			zap.Any("numRows", insertData.Infos[i].Length),
		)
	}

	switch segmentType {
	case segmentTypeGrowing:
		timestamps, ids, rowData, err := storage.TransferColumnBasedInsertDataToRowBased(insertData)
		if err != nil {
			return err
		}
		return loader.loadGrowingSegments(segment, ids, timestamps, rowData)
	case segmentTypeSealed:
		return loader.loadSealedSegments(segment, insertData)
	default:
		err := errors.New(fmt.Sprintln("illegal segment type when load segment, collectionID = ", segment.collectionID))
		return err
	}
}

func (loader *segmentLoader) loadGrowingSegments(segment *Segment,
	ids []UniqueID,
	timestamps []Timestamp,
	records []*commonpb.Blob) error {
	if len(ids) != len(timestamps) || len(timestamps) != len(records) {
		return errors.New(fmt.Sprintln("illegal insert data when load segment, collectionID = ", segment.collectionID))
	}

	log.Debug("start load growing segments...",
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
	tmpInsertMsg := &msgstream.InsertMsg{
		InsertRequest: internalpb.InsertRequest{
			CollectionID: segment.collectionID,
			Timestamps:   timestamps,
			RowIDs:       ids,
			RowData:      records,
		},
	}
	pks, err := getPrimaryKeys(tmpInsertMsg, loader.streamingReplica)
	if err != nil {
		return err
	}
	segment.updateBloomFilter(pks)

	// 3. do insert
	err = segment.segmentInsert(offset, &ids, &timestamps, &records)
	if err != nil {
		return err
	}
	log.Debug("Do insert done in segment loader", zap.Int("len", numOfRecords), zap.Int64("segmentID", segment.ID()), zap.Int64("collectionID", segment.collectionID))

	return nil
}

func (loader *segmentLoader) loadSealedSegments(segment *Segment, insertData *storage.InsertData) error {
	log.Debug("start load sealed segments...",
		zap.Any("collectionID", segment.collectionID),
		zap.Any("segmentID", segment.ID()),
		zap.Any("numFields", len(insertData.Data)),
	)
	for fieldID, value := range insertData.Data {
		var numRows []int64
		var data interface{}
		switch fieldData := value.(type) {
		case *storage.BoolFieldData:
			numRows = fieldData.NumRows
			data = fieldData.Data
		case *storage.Int8FieldData:
			numRows = fieldData.NumRows
			data = fieldData.Data
		case *storage.Int16FieldData:
			numRows = fieldData.NumRows
			data = fieldData.Data
		case *storage.Int32FieldData:
			numRows = fieldData.NumRows
			data = fieldData.Data
		case *storage.Int64FieldData:
			numRows = fieldData.NumRows
			data = fieldData.Data
		case *storage.FloatFieldData:
			numRows = fieldData.NumRows
			data = fieldData.Data
		case *storage.DoubleFieldData:
			numRows = fieldData.NumRows
			data = fieldData.Data
		case *storage.StringFieldData:
			numRows = fieldData.NumRows
			data = fieldData.Data
		case *storage.FloatVectorFieldData:
			numRows = fieldData.NumRows
			data = fieldData.Data
		case *storage.BinaryVectorFieldData:
			numRows = fieldData.NumRows
			data = fieldData.Data
		default:
			return errors.New("unexpected field data type")
		}
		if fieldID == common.TimeStampField {
			segment.setIDBinlogRowSizes(numRows)
		}
		totalNumRows := int64(0)
		for _, numRow := range numRows {
			totalNumRows += numRow
		}
		err := segment.segmentLoadFieldData(fieldID, int(totalNumRows), data)
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

	values, err := loader.minioKV.MultiLoad(binlogPaths)
	if err != nil {
		return err
	}
	blobs := make([]*storage.Blob, 0)
	for i := 0; i < len(values); i++ {
		blobs = append(blobs, &storage.Blob{Value: []byte(values[i])})
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
		for _, log := range deltaLog.GetBinlogs() {
			value, err := loader.minioKV.Load(log.GetLogPath())
			if err != nil {
				return err
			}
			blob := &storage.Blob{
				Key:   log.GetLogPath(),
				Value: []byte(value),
			}
			blobs = append(blobs, blob)
		}
	}
	if len(blobs) == 0 {
		log.Info("there are no delta logs saved with segment", zap.Any("segmentID", segment.segmentID))
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
	log.Debug("from dml check point load delete", zap.Any("position", position), zap.Any("msg id", position.MsgID))
	stream, err := loader.factory.NewMsgStream(ctx)
	if err != nil {
		return err
	}
	defer stream.Close()
	pChannelName := rootcoord.ToPhysicalChannel(position.ChannelName)
	position.ChannelName = pChannelName
	stream.AsReader([]string{pChannelName}, fmt.Sprintf("querynode-%d-%d", Params.QueryNodeCfg.QueryNodeID, collectionID))
	err = stream.SeekReaders([]*internalpb.MsgPosition{position})
	if err != nil {
		return err
	}

	delData := &deleteData{
		deleteIDs:        make(map[UniqueID][]int64),
		deleteTimestamps: make(map[UniqueID][]Timestamp),
		deleteOffset:     make(map[UniqueID]int64),
	}
	log.Debug("start read msg from stream reader", zap.Any("msg id", position.GetMsgID()))
	for stream.HasNext(pChannelName) {
		ctx, cancel := context.WithTimeout(ctx, timeoutForEachRead)
		tsMsg, err := stream.Next(ctx, pChannelName)
		if err != nil {
			log.Warn("fail to load delete", zap.String("pChannelName", pChannelName), zap.Any("msg id", position.GetMsgID()), zap.Error(err))
			cancel()
			return err
		}
		if tsMsg == nil {
			cancel()
			continue
		}

		if tsMsg.Type() == commonpb.MsgType_Delete {
			dmsg := tsMsg.(*msgstream.DeleteMsg)
			if dmsg.CollectionID != collectionID {
				cancel()
				continue
			}
			log.Debug("delete pk",
				zap.Any("pk", dmsg.PrimaryKeys),
				zap.String("vChannelName", position.GetChannelName()),
				zap.Any("msg id", position.GetMsgID()),
			)
			processDeleteMessages(loader.historicalReplica, dmsg, delData)
		}
		cancel()
	}
	log.Debug("All data has been read, there is no more data", zap.String("channel", pChannelName), zap.Any("msg id", position.GetMsgID()))
	for segmentID, pks := range delData.deleteIDs {
		segment, err := loader.historicalReplica.getSegmentByID(segmentID)
		if err != nil {
			log.Debug(err.Error())
			continue
		}
		offset := segment.segmentPreDelete(len(pks))
		delData.deleteOffset[segmentID] = offset
	}

	wg := sync.WaitGroup{}
	for segmentID := range delData.deleteOffset {
		wg.Add(1)
		go deletePk(loader.historicalReplica, delData, segmentID, &wg)
	}
	wg.Wait()
	log.Debug("from dml check point load done", zap.Any("msg id", position.GetMsgID()))
	return nil
}

func deletePk(replica ReplicaInterface, deleteData *deleteData, segmentID UniqueID, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Debug("QueryNode::iNode::delete", zap.Any("SegmentID", segmentID))
	targetSegment, err := replica.getSegmentByID(segmentID)
	if err != nil {
		log.Error(err.Error())
		return
	}

	if targetSegment.getType() != segmentTypeSealed && targetSegment.getType() != segmentTypeIndexing {
		return
	}

	ids := deleteData.deleteIDs[segmentID]
	timestamps := deleteData.deleteTimestamps[segmentID]
	offset := deleteData.deleteOffset[segmentID]

	err = targetSegment.segmentDelete(offset, &ids, &timestamps)
	if err != nil {
		log.Warn("QueryNode: targetSegmentDelete failed", zap.Error(err))
		return
	}
	log.Debug("Do delete done", zap.Int("len", len(deleteData.deleteIDs[segmentID])), zap.Int64("segmentID", segmentID), zap.Any("segmentType", targetSegment.getType()))
}

// JoinIDPath joins ids to path format.
func JoinIDPath(ids ...UniqueID) string {
	idStr := make([]string, 0, len(ids))
	for _, id := range ids {
		idStr = append(idStr, strconv.FormatInt(id, 10))
	}
	return path.Join(idStr...)
}

func (loader *segmentLoader) getFieldAndIndexInfo(segment *Segment,
	segmentLoadInfo *querypb.SegmentLoadInfo) ([]*datapb.FieldBinlog, []FieldID, error) {
	collectionID := segment.collectionID
	vectorFieldIDs, err := loader.historicalReplica.getVecFieldIDsByCollectionID(collectionID)
	if err != nil {
		return nil, nil, err
	}
	if len(vectorFieldIDs) <= 0 {
		return nil, nil, fmt.Errorf("no vector field in collection %d", collectionID)
	}

	// add VectorFieldInfo for vector fields
	for _, fieldBinlog := range segmentLoadInfo.BinlogPaths {
		if funcutil.SliceContain(vectorFieldIDs, fieldBinlog.FieldID) {
			vectorFieldInfo := newVectorFieldInfo(fieldBinlog)
			segment.setVectorFieldInfo(fieldBinlog.FieldID, vectorFieldInfo)
		}
	}

	indexedFieldIDs := make([]FieldID, 0)
	if idxInfo, err := loader.indexLoader.getIndexInfo(collectionID, segment); err != nil {
		log.Warn(err.Error())
	} else {
		loader.indexLoader.setIndexInfo(segment, idxInfo)
		indexedFieldIDs = append(indexedFieldIDs, idxInfo.fieldID)
	}

	// we don't need to load raw data for indexed vector field
	fieldBinlogs := loader.filterFieldBinlogs(segmentLoadInfo.BinlogPaths, indexedFieldIDs)
	return fieldBinlogs, indexedFieldIDs, nil
}

func (loader *segmentLoader) estimateSegmentSize(segment *Segment,
	fieldBinLogs []*datapb.FieldBinlog,
	indexFieldIDs []FieldID) (int64, error) {
	segmentSize := int64(0)
	// get fields data size, if len(indexFieldIDs) == 0, vector field would be involved in fieldBinLogs
	for _, fb := range fieldBinLogs {
		log.Debug("estimate segment fields size",
			zap.Any("collectionID", segment.collectionID),
			zap.Any("segmentID", segment.ID()),
			zap.Any("fieldID", fb.FieldID),
			zap.Any("paths", fb.Binlogs),
		)
		for _, binlogPath := range fb.Binlogs {
			logSize, err := storage.EstimateMemorySize(loader.minioKV, binlogPath.GetLogPath())
			if err != nil {
				logSize, err = storage.GetBinlogSize(loader.minioKV, binlogPath.GetLogPath())
				if err != nil {
					return 0, err
				}
			}
			segmentSize += logSize
		}
	}
	// get index size
	for _, fieldID := range indexFieldIDs {
		indexSize, err := loader.indexLoader.estimateIndexBinlogSize(segment, fieldID)
		if err != nil {
			return 0, err
		}
		segmentSize += indexSize
	}
	return segmentSize, nil
}

func (loader *segmentLoader) checkSegmentSize(collectionID UniqueID, segmentSizes map[UniqueID]int64) error {
	usedMem := metricsinfo.GetUsedMemoryCount()
	totalMem := metricsinfo.GetMemoryCount()

	if usedMem == 0 || totalMem == 0 {
		return errors.New(fmt.Sprintln("get memory failed when checkSegmentSize, collectionID = ", collectionID))
	}

	segmentTotalSize := int64(0)
	for _, size := range segmentSizes {
		segmentTotalSize += size
	}

	for segmentID, size := range segmentSizes {
		log.Debug("memory stats when load segment",
			zap.Any("collectionIDs", collectionID),
			zap.Any("segmentID", segmentID),
			zap.Any("totalMem", totalMem),
			zap.Any("usedMem", usedMem),
			zap.Any("segmentTotalSize", segmentTotalSize),
			zap.Any("currentSegmentSize", size),
			zap.Any("thresholdFactor", Params.QueryNodeCfg.OverloadedMemoryThresholdPercentage),
		)
		if int64(usedMem)+segmentTotalSize+size > int64(float64(totalMem)*Params.QueryNodeCfg.OverloadedMemoryThresholdPercentage) {
			return errors.New(fmt.Sprintln("load segment failed, OOM if load, "+
				"collectionID = ", collectionID, ", ",
				"usedMem = ", usedMem, ", ",
				"segmentTotalSize = ", segmentTotalSize, ", ",
				"currentSegmentSize = ", size, ", ",
				"totalMem = ", totalMem, ", ",
				"thresholdFactor = ", Params.QueryNodeCfg.OverloadedMemoryThresholdPercentage,
			))
		}
	}

	return nil
}

func newSegmentLoader(ctx context.Context,
	rootCoord types.RootCoord,
	indexCoord types.IndexCoord,
	historicalReplica ReplicaInterface,
	streamingReplica ReplicaInterface,
	etcdKV *etcdkv.EtcdKV,
	factory msgstream.Factory) *segmentLoader {
	option := &minioKV.Option{
		Address:           Params.QueryNodeCfg.MinioEndPoint,
		AccessKeyID:       Params.QueryNodeCfg.MinioAccessKeyID,
		SecretAccessKeyID: Params.QueryNodeCfg.MinioSecretAccessKey,
		UseSSL:            Params.QueryNodeCfg.MinioUseSSLStr,
		CreateBucket:      true,
		BucketName:        Params.QueryNodeCfg.MinioBucketName,
	}

	client, err := minioKV.NewMinIOKV(ctx, option)
	if err != nil {
		panic(err)
	}

	iLoader := newIndexLoader(ctx, rootCoord, indexCoord, historicalReplica)
	return &segmentLoader{
		historicalReplica: historicalReplica,
		streamingReplica:  streamingReplica,

		minioKV: client,
		etcdKV:  etcdKV,

		indexLoader: iLoader,

		factory: factory,
	}
}
