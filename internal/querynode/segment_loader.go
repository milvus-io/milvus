// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package querynode

import (
	"context"
	"errors"
	"fmt"

	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	minioKV "github.com/milvus-io/milvus/internal/kv/minio"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/rootcoord"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	queryCoordSegmentMetaPrefix = "queryCoord-segmentMeta"
	queryNodeSegmentMetaPrefix  = "queryNode-segmentMeta"
)

// segmentLoader is only responsible for loading the field data from binlog
type segmentLoader struct {
	historicalReplica ReplicaInterface

	dataCoord types.DataCoord

	minioKV kv.BaseKV // minio minioKV
	etcdKV  *etcdkv.EtcdKV

	indexLoader *indexLoader
}

func (loader *segmentLoader) loadSegmentOfConditionHandOff(req *querypb.LoadSegmentsRequest) error {
	return errors.New("TODO: implement hand off")
}

func (loader *segmentLoader) loadSegmentOfConditionLoadBalance(req *querypb.LoadSegmentsRequest) error {
	return loader.loadSegment(req, false)
}

func (loader *segmentLoader) loadSegmentOfConditionGRPC(req *querypb.LoadSegmentsRequest) error {
	return loader.loadSegment(req, true)
}

func (loader *segmentLoader) loadSegmentOfConditionNodeDown(req *querypb.LoadSegmentsRequest) error {
	return loader.loadSegment(req, true)
}

func (loader *segmentLoader) loadSegment(req *querypb.LoadSegmentsRequest, onService bool) error {
	// no segment needs to load, return
	if len(req.Infos) == 0 {
		return nil
	}

	err := loader.checkSegmentMemory(req.Infos)
	if err != nil {
		return err
	}

	newSegments := make([]*Segment, 0)
	segmentGC := func() {
		for _, s := range newSegments {
			deleteSegment(s)
		}
	}
	setSegments := func() {
		for _, s := range newSegments {
			err := loader.historicalReplica.setSegment(s)
			if err != nil {
				log.Warn(err.Error())
				deleteSegment(s)
			}
		}
	}

	// start to load
	for _, info := range req.Infos {
		segmentID := info.SegmentID
		partitionID := info.PartitionID
		collectionID := info.CollectionID

		collection, err := loader.historicalReplica.getCollectionByID(collectionID)
		if err != nil {
			log.Warn(err.Error())
			segmentGC()
			return err
		}
		segment := newSegment(collection, segmentID, partitionID, collectionID, "", segmentTypeSealed, onService)
		err = loader.loadSegmentInternal(collectionID, segment, info)
		if err != nil {
			deleteSegment(segment)
			log.Warn(err.Error())
			segmentGC()
			return err
		}
		if onService {
			key := fmt.Sprintf("%s/%d", queryCoordSegmentMetaPrefix, segmentID)
			value, err := loader.etcdKV.Load(key)
			if err != nil {
				deleteSegment(segment)
				log.Warn("error when load segment info from etcd", zap.Any("error", err.Error()))
				segmentGC()
				return err
			}
			segmentInfo := &querypb.SegmentInfo{}
			err = proto.UnmarshalText(value, segmentInfo)
			if err != nil {
				deleteSegment(segment)
				log.Warn("error when unmarshal segment info from etcd", zap.Any("error", err.Error()))
				segmentGC()
				return err
			}
			segmentInfo.SegmentState = querypb.SegmentState_sealed
			newKey := fmt.Sprintf("%s/%d", queryNodeSegmentMetaPrefix, segmentID)
			err = loader.etcdKV.Save(newKey, proto.MarshalTextString(segmentInfo))
			if err != nil {
				deleteSegment(segment)
				log.Warn("error when update segment info to etcd", zap.Any("error", err.Error()))
				segmentGC()
				return err
			}
		}
		newSegments = append(newSegments, segment)
	}
	setSegments()

	return nil
}

func (loader *segmentLoader) loadSegmentInternal(collectionID UniqueID, segment *Segment, segmentLoadInfo *querypb.SegmentLoadInfo) error {
	vectorFieldIDs, err := loader.historicalReplica.getVecFieldIDsByCollectionID(collectionID)
	if err != nil {
		return err
	}
	if len(vectorFieldIDs) <= 0 {
		return fmt.Errorf("no vector field in collection %d", collectionID)
	}

	// add VectorFieldInfo for vector fields
	for _, fieldBinlog := range segmentLoadInfo.BinlogPaths {
		if funcutil.SliceContain(vectorFieldIDs, fieldBinlog.FieldID) {
			vectorFieldInfo := newVectorFieldInfo(fieldBinlog)
			segment.setVectorFieldInfo(fieldBinlog.FieldID, vectorFieldInfo)
		}
	}

	indexedFieldIDs := make([]int64, 0)
	for _, vecFieldID := range vectorFieldIDs {
		err = loader.indexLoader.setIndexInfo(collectionID, segment, vecFieldID)
		if err != nil {
			log.Warn(err.Error())
			continue
		}
		indexedFieldIDs = append(indexedFieldIDs, vecFieldID)
	}

	// we don't need to load raw data for indexed vector field
	fieldBinlogs := loader.filterFieldBinlogs(segmentLoadInfo.BinlogPaths, indexedFieldIDs)

	log.Debug("loading insert...")
	err = loader.loadSegmentFieldsData(segment, fieldBinlogs)
	if err != nil {
		return err
	}
	for _, id := range indexedFieldIDs {
		log.Debug("loading index...")
		err = loader.indexLoader.loadIndex(segment, id)
		if err != nil {
			return err
		}
	}

	return nil
}

func (loader *segmentLoader) checkSegmentMemory(segmentLoadInfos []*querypb.SegmentLoadInfo) error {
	totalRAM := metricsinfo.GetMemoryCount()
	usedRAM := metricsinfo.GetUsedMemoryCount()

	segmentTotalSize := uint64(0)
	for _, segInfo := range segmentLoadInfos {
		collectionID := segInfo.CollectionID
		segmentID := segInfo.SegmentID

		col, err := loader.historicalReplica.getCollectionByID(collectionID)
		if err != nil {
			return err
		}

		sizePerRecord, err := typeutil.EstimateSizePerRecord(col.schema)
		if err != nil {
			return err
		}

		segmentSize := uint64(int64(sizePerRecord) * segInfo.NumOfRows)
		segmentTotalSize += segmentSize
		// TODO: get 0.9 from param table
		thresholdMemSize := float64(totalRAM) * 0.9

		log.Debug("memory size[byte] stats when load segment",
			zap.Any("collectionIDs", collectionID),
			zap.Any("segmentID", segmentID),
			zap.Any("numOfRows", segInfo.NumOfRows),
			zap.Any("totalRAM", totalRAM),
			zap.Any("usedRAM", usedRAM),
			zap.Any("segmentSize", segmentSize),
			zap.Any("segmentTotalSize", segmentTotalSize),
			zap.Any("thresholdMemSize", thresholdMemSize),
		)
		if usedRAM+segmentTotalSize > uint64(thresholdMemSize) {
			return errors.New("load segment failed, OOM if load, collectionID = " + fmt.Sprintln(collectionID))
		}
	}

	return nil
}

//func (loader *segmentLoader) GetSegmentStates(segmentID UniqueID) (*datapb.GetSegmentStatesResponse, error) {
//	ctx := context.TODO()
//	if loader.dataCoord == nil {
//		return nil, errors.New("null data service client")
//	}
//
//	segmentStatesRequest := &datapb.GetSegmentStatesRequest{
//		SegmentIDs: []int64{segmentID},
//	}
//	statesResponse, err := loader.dataCoord.GetSegmentStates(ctx, segmentStatesRequest)
//	if err != nil || statesResponse.Status.ErrorCode != commonpb.ErrorCode_Success {
//		return nil, err
//	}
//	if len(statesResponse.States) != 1 {
//		return nil, errors.New("segment states' len should be 1")
//	}
//
//	return statesResponse, nil
//}

func (loader *segmentLoader) filterFieldBinlogs(fieldBinlogs []*datapb.FieldBinlog, skipFieldIDs []int64) []*datapb.FieldBinlog {
	result := make([]*datapb.FieldBinlog, 0)
	for _, fieldBinlog := range fieldBinlogs {
		if !funcutil.SliceContain(skipFieldIDs, fieldBinlog.FieldID) {
			result = append(result, fieldBinlog)
		}
	}
	return result
}

func (loader *segmentLoader) loadSegmentFieldsData(segment *Segment, fieldBinlogs []*datapb.FieldBinlog) error {
	iCodec := storage.InsertCodec{}
	defer func() {
		err := iCodec.Close()
		if err != nil {
			log.Warn(err.Error())
		}
	}()
	blobs := make([]*storage.Blob, 0)
	for _, fb := range fieldBinlogs {
		log.Debug("load segment fields data",
			zap.Int64("segmentID", segment.segmentID),
			zap.Any("fieldID", fb.FieldID),
			zap.String("paths", fmt.Sprintln(fb.Binlogs)),
		)
		for _, path := range fb.Binlogs {
			p := path
			binLog, err := loader.minioKV.Load(path)
			if err != nil {
				// TODO: return or continue?
				return err
			}
			blob := &storage.Blob{
				Key:   p,
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
		case storage.StringFieldData:
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
		if fieldID == rootcoord.TimeStampField {
			segment.setIDBinlogRowSizes(numRows)
		}
		totalNumRows := int64(0)
		for _, numRow := range numRows {
			totalNumRows += numRow
		}
		err = segment.segmentLoadFieldData(fieldID, int(totalNumRows), data)
		if err != nil {
			// TODO: return or continue?
			return err
		}
	}

	return nil
}

func newSegmentLoader(ctx context.Context, rootCoord types.RootCoord, indexCoord types.IndexCoord, replica ReplicaInterface, etcdKV *etcdkv.EtcdKV) *segmentLoader {
	option := &minioKV.Option{
		Address:           Params.MinioEndPoint,
		AccessKeyID:       Params.MinioAccessKeyID,
		SecretAccessKeyID: Params.MinioSecretAccessKey,
		UseSSL:            Params.MinioUseSSLStr,
		CreateBucket:      true,
		BucketName:        Params.MinioBucketName,
	}

	client, err := minioKV.NewMinIOKV(ctx, option)
	if err != nil {
		panic(err)
	}

	iLoader := newIndexLoader(ctx, rootCoord, indexCoord, replica)
	return &segmentLoader{
		historicalReplica: replica,

		minioKV: client,
		etcdKV:  etcdKV,

		indexLoader: iLoader,
	}
}
