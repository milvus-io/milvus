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
	"github.com/milvus-io/milvus/internal/metrics"
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
	"github.com/milvus-io/milvus/internal/util/timerecord"
)

const timeoutForEachRead = 10 * time.Second

// segmentLoader is only responsible for loading the field data from binlog
type segmentLoader struct {
	historicalReplica ReplicaInterface
	streamingReplica  ReplicaInterface

	dataCoord types.DataCoord

	minioKV kv.DataKV // minio minioKV
	etcdKV  *etcdkv.EtcdKV

	factory msgstream.Factory
}

func (loader *segmentLoader) loadSegment(req *querypb.LoadSegmentsRequest, segmentType segmentType) error {
	if req.Base == nil {
		return fmt.Errorf("nil base message when load segment, collectionID = %d", req.CollectionID)
	}

	// no segment needs to load, return
	if len(req.Infos) == 0 {
		return nil
	}

	var metaReplica ReplicaInterface
	switch segmentType {
	case segmentTypeGrowing:
		metaReplica = loader.streamingReplica
	case segmentTypeSealed:
		metaReplica = loader.historicalReplica
	default:
		err := fmt.Errorf("illegal segment type when load segment, collectionID = %d", req.CollectionID)
		log.Error("load segment failed, illegal segment type", zap.Int64("loadSegmentRequest msgID", req.Base.MsgID), zap.Error(err))
		return err
	}

	log.Debug("segmentLoader start loading...",
		zap.Any("collectionID", req.CollectionID),
		zap.Any("numOfSegments", len(req.Infos)),
		zap.Any("loadType", segmentType),
	)
	// check memory limit
	err := loader.checkSegmentSize(req.CollectionID, req.Infos)
	if err != nil {
		log.Error("load failed, OOM if loaded", zap.Int64("loadSegmentRequest msgID", req.Base.MsgID), zap.Error(err))
		return err
	}

	newSegments := make(map[UniqueID]*Segment)
	segmentGC := func() {
		for _, s := range newSegments {
			deleteSegment(s)
		}
	}

	for _, info := range req.Infos {
		segmentID := info.SegmentID
		partitionID := info.PartitionID
		collectionID := info.CollectionID

		collection, err := loader.historicalReplica.getCollectionByID(collectionID)
		if err != nil {
			segmentGC()
			return err
		}
		segment, err := newSegment(collection, segmentID, partitionID, collectionID, "", segmentType, true)
		if err != nil {
			log.Error("load segment failed when create new segment",
				zap.Int64("collectionID", collectionID),
				zap.Int64("partitionID", partitionID),
				zap.Int64("segmentID", segmentID),
				zap.Int32("segment type", int32(segmentType)),
				zap.Error(err))
			segmentGC()
			return err
		}

		newSegments[segmentID] = segment
	}

	// start to load
	for _, loadInfo := range req.Infos {
		collectionID := loadInfo.CollectionID
		partitionID := loadInfo.PartitionID
		segmentID := loadInfo.SegmentID
		segment := newSegments[segmentID]
		tr := timerecord.NewTimeRecorder("loadDurationPerSegment")
		err = loader.loadSegmentInternal(segment, loadInfo)
		if err != nil {
			log.Error("load segment failed when load data into memory",
				zap.Int64("collectionID", collectionID),
				zap.Int64("partitionID", partitionID),
				zap.Int64("segmentID", segmentID),
				zap.Int32("segment type", int32(segmentType)),
				zap.Error(err))
			segmentGC()
			return err
		}
		metrics.QueryNodeLoadSegmentLatency.WithLabelValues(fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Observe(float64(tr.ElapseSpan().Milliseconds()))
	}

	// set segment to meta replica
	for _, s := range newSegments {
		err = metaReplica.setSegment(s)
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

func (loader *segmentLoader) loadSegmentInternal(segment *Segment,
	loadInfo *querypb.SegmentLoadInfo) error {
	collectionID := loadInfo.CollectionID
	partitionID := loadInfo.PartitionID
	segmentID := loadInfo.SegmentID
	log.Debug("start loading segment data into memory",
		zap.Int64("collectionID", collectionID),
		zap.Int64("partitionID", partitionID),
		zap.Int64("segmentID", segmentID))
	vecFieldIDs, err := loader.historicalReplica.getVecFieldIDsByCollectionID(collectionID)
	if err != nil {
		return err
	}
	pkFieldID, err := loader.historicalReplica.getPKFieldIDByCollectionID(collectionID)
	if err != nil {
		return err
	}

	var nonVecFieldBinlogs []*datapb.FieldBinlog
	if segment.getType() == segmentTypeSealed {
		fieldID2IndexInfo := make(map[int64]*querypb.VecFieldIndexInfo)
		for _, indexInfo := range loadInfo.IndexInfos {
			fieldID := indexInfo.FieldID
			fieldID2IndexInfo[fieldID] = indexInfo
		}

		vecFieldInfos := make(map[int64]*VectorFieldInfo)

		for _, fieldBinlog := range loadInfo.BinlogPaths {
			fieldID := fieldBinlog.FieldID
			if funcutil.SliceContain(vecFieldIDs, fieldID) {
				fieldInfo := &VectorFieldInfo{
					fieldBinlog: fieldBinlog,
				}
				if indexInfo, ok := fieldID2IndexInfo[fieldID]; ok {
					fieldInfo.indexInfo = indexInfo
				}
				vecFieldInfos[fieldID] = fieldInfo
			} else {
				nonVecFieldBinlogs = append(nonVecFieldBinlogs, fieldBinlog)
			}
		}

		err = loader.loadVecFieldData(segment, vecFieldInfos)
		if err != nil {
			return err
		}
	} else {
		nonVecFieldBinlogs = loadInfo.BinlogPaths
	}
	err = loader.loadFiledBinlogData(segment, nonVecFieldBinlogs)
	if err != nil {
		return err
	}

	if pkFieldID == common.InvalidFieldID {
		log.Warn("segment primary key field doesn't exist when load segment")
	} else {
		log.Debug("loading bloom filter...")
		pkStatsBinlogs := loader.filterPKStatsBinlogs(loadInfo.Statslogs, pkFieldID)
		err = loader.loadSegmentBloomFilter(segment, pkStatsBinlogs)
		if err != nil {
			return err
		}
	}

	log.Debug("loading delta...")
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

func (loader *segmentLoader) loadFiledBinlogData(segment *Segment, fieldBinlogs []*datapb.FieldBinlog) error {
	segmentType := segment.getType()
	iCodec := storage.InsertCodec{}
	blobs := make([]*storage.Blob, 0)
	for _, fieldBinlog := range fieldBinlogs {
		for _, path := range fieldBinlog.Binlogs {
			binLog, err := loader.minioKV.Load(path.GetLogPath())
			if err != nil {
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

func (loader *segmentLoader) loadVecFieldData(segment *Segment, vecFieldInfos map[int64]*VectorFieldInfo) error {
	for fieldID, fieldInfo := range vecFieldInfos {
		if fieldInfo.indexInfo == nil || !fieldInfo.indexInfo.EnableIndex {
			fieldBinlog := fieldInfo.fieldBinlog
			err := loader.loadFiledBinlogData(segment, []*datapb.FieldBinlog{fieldBinlog})
			if err != nil {
				return err
			}
			log.Debug("load vector field's binlog data done", zap.Int64("segmentID", segment.ID()), zap.Int64("fieldID", fieldID))
		} else {
			indexInfo := fieldInfo.indexInfo
			err := loader.loadVecFieldIndexData(segment, indexInfo)
			if err != nil {
				return err
			}
			log.Debug("load vector field's index data done", zap.Int64("segmentID", segment.ID()), zap.Int64("fieldID", fieldID))
		}
		segment.setVectorFieldInfo(fieldID, fieldInfo)
	}

	return nil
}

func (loader *segmentLoader) loadVecFieldIndexData(segment *Segment, indexInfo *querypb.VecFieldIndexInfo) error {
	indexBuffer := make([][]byte, 0)
	indexCodec := storage.NewIndexFileBinlogCodec()
	for _, p := range indexInfo.IndexFilePaths {
		log.Debug("load index file", zap.String("path", p))
		indexPiece, err := loader.minioKV.Load(p)
		if err != nil {
			return err
		}

		if path.Base(p) != storage.IndexParamsKey {
			data, _, _, _, err := indexCodec.Deserialize([]*storage.Blob{{Key: path.Base(p), Value: []byte(indexPiece)}})
			if err != nil {
				return err
			}
			indexBuffer = append(indexBuffer, data[0].Value)
		}
	}
	// 2. use index bytes and index path to update segment
	err := segment.segmentLoadIndexData(indexBuffer, indexInfo)
	return err
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
	metrics.QueryNodeNumReaders.WithLabelValues(fmt.Sprint(collectionID), fmt.Sprint(Params.QueryNodeCfg.QueryNodeID)).Inc()
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

	if targetSegment.segmentType != segmentTypeSealed {
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
	log.Debug("Do delete done", zap.Int("len", len(deleteData.deleteIDs[segmentID])), zap.Int64("segmentID", segmentID), zap.Any("segmentType", targetSegment.segmentType))
}

// JoinIDPath joins ids to path format.
func JoinIDPath(ids ...UniqueID) string {
	idStr := make([]string, 0, len(ids))
	for _, id := range ids {
		idStr = append(idStr, strconv.FormatInt(id, 10))
	}
	return path.Join(idStr...)
}

func (loader *segmentLoader) checkSegmentSize(collectionID UniqueID, segmentLoadInfos []*querypb.SegmentLoadInfo) error {
	usedMem := metricsinfo.GetUsedMemoryCount()
	totalMem := metricsinfo.GetMemoryCount()

	if usedMem == 0 || totalMem == 0 {
		return fmt.Errorf("get memory failed when checkSegmentSize, collectionID = %d", collectionID)
	}

	usedMemAfterLoad := int64(usedMem)
	maxSegmentSize := int64(0)
	for _, loadInfo := range segmentLoadInfos {
		segmentSize := loadInfo.SegmentSize
		usedMemAfterLoad += segmentSize
		if segmentSize > maxSegmentSize {
			maxSegmentSize = segmentSize
		}
	}

	// when load segment, data will be copied from go memory to c++ memory
	if usedMemAfterLoad+maxSegmentSize > int64(float64(totalMem)*Params.QueryNodeCfg.OverloadedMemoryThresholdPercentage) {
		return fmt.Errorf("load segment failed, OOM if load, collectionID = %d, maxSegmentSize = %d, usedMemAfterLoad = %d, totalMem = %d, thresholdFactor = %f",
			collectionID, maxSegmentSize, usedMemAfterLoad, totalMem, Params.QueryNodeCfg.OverloadedMemoryThresholdPercentage)
	}

	return nil
}

func newSegmentLoader(ctx context.Context,
	historicalReplica ReplicaInterface,
	streamingReplica ReplicaInterface,
	etcdKV *etcdkv.EtcdKV,
	factory msgstream.Factory) *segmentLoader {
	option := &minioKV.Option{
		Address:           Params.MinioCfg.Address,
		AccessKeyID:       Params.MinioCfg.AccessKeyID,
		SecretAccessKeyID: Params.MinioCfg.SecretAccessKey,
		UseSSL:            Params.MinioCfg.UseSSL,
		BucketName:        Params.MinioCfg.BucketName,
		CreateBucket:      true,
	}

	client, err := minioKV.NewMinIOKV(ctx, option)
	if err != nil {
		panic(err)
	}

	return &segmentLoader{
		historicalReplica: historicalReplica,
		streamingReplica:  streamingReplica,

		minioKV: client,
		etcdKV:  etcdKV,

		factory: factory,
	}
}
