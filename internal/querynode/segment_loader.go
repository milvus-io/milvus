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
	"strconv"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	minioKV "github.com/milvus-io/milvus/internal/kv/minio"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	queryPb "github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
)

// segmentLoader is only responsible for loading the field data from binlog
type segmentLoader struct {
	historicalReplica ReplicaInterface

	dataService types.DataService

	kv kv.BaseKV // minio kv

	indexLoader *indexLoader
}

func (loader *segmentLoader) loadSegmentOfConditionHandOff(req *queryPb.LoadSegmentsRequest) error {
	return errors.New("TODO: implement hand off")
}

func (loader *segmentLoader) loadSegmentOfConditionLoadBalance(req *queryPb.LoadSegmentsRequest) error {
	return loader.loadSegment(req, false)
}

func (loader *segmentLoader) loadSegmentOfConditionGRPC(req *queryPb.LoadSegmentsRequest) error {
	return loader.loadSegment(req, true)
}

func (loader *segmentLoader) loadSegmentOfConditionNodeDown(req *queryPb.LoadSegmentsRequest) error {
	return loader.loadSegment(req, true)
}

func (loader *segmentLoader) loadSegment(req *queryPb.LoadSegmentsRequest, onService bool) error {
	// no segment needs to load, return
	if len(req.Infos) == 0 {
		return nil
	}

	// start to load
	for _, info := range req.Infos {
		segmentID := info.SegmentID
		partitionID := info.PartitionID
		collectionID := info.CollectionID

		// init replica
		hasCollectionInHistorical := loader.historicalReplica.hasCollection(collectionID)
		hasPartitionInHistorical := loader.historicalReplica.hasPartition(partitionID)
		if !hasCollectionInHistorical {
			err := loader.historicalReplica.addCollection(collectionID, req.Schema)
			if err != nil {
				return err
			}
		}
		if !hasPartitionInHistorical {
			err := loader.historicalReplica.addPartition(collectionID, partitionID)
			if err != nil {
				return err
			}
		}

		collection, err := loader.historicalReplica.getCollectionByID(collectionID)
		if err != nil {
			log.Warn(err.Error())
			continue
		}
		segment := newSegment(collection, segmentID, partitionID, collectionID, "", segmentTypeSealed, onService)
		err = loader.loadSegmentInternal(collectionID, segment, info.BinlogPaths)
		if err != nil {
			deleteSegment(segment)
			log.Error(err.Error())
			continue
		}
		err = loader.historicalReplica.setSegment(segment)
		if err != nil {
			deleteSegment(segment)
			log.Error(err.Error())
		}
	}

	// sendQueryNodeStats
	return loader.indexLoader.sendQueryNodeStats()
}

func (loader *segmentLoader) loadSegmentInternal(collectionID UniqueID,
	segment *Segment,
	binlogPaths []*datapb.FieldBinlog) error {

	vectorFieldIDs, err := loader.historicalReplica.getVecFieldIDsByCollectionID(collectionID)
	if err != nil {
		return err
	}

	loadIndexFieldIDs := make([]int64, 0)
	for _, vecFieldID := range vectorFieldIDs {
		err = loader.indexLoader.setIndexInfo(collectionID, segment, vecFieldID)
		if err != nil {
			log.Warn(err.Error())
			continue
		}
		loadIndexFieldIDs = append(loadIndexFieldIDs, vecFieldID)
	}
	// we don't need load to vector fields
	binlogPaths = loader.filterOutVectorFields(binlogPaths, loadIndexFieldIDs)

	log.Debug("loading insert...")
	err = loader.loadSegmentFieldsData(segment, binlogPaths)
	if err != nil {
		return err
	}
	for _, id := range loadIndexFieldIDs {
		log.Debug("loading index...")
		err = loader.indexLoader.loadIndex(segment, id)
		if err != nil {
			return err
		}
	}

	return nil
}

func (loader *segmentLoader) GetSegmentStates(segmentID UniqueID) (*datapb.GetSegmentStatesResponse, error) {
	ctx := context.TODO()
	if loader.dataService == nil {
		return nil, errors.New("null data service client")
	}

	segmentStatesRequest := &datapb.GetSegmentStatesRequest{
		SegmentIDs: []int64{segmentID},
	}
	statesResponse, err := loader.dataService.GetSegmentStates(ctx, segmentStatesRequest)
	if err != nil || statesResponse.Status.ErrorCode != commonpb.ErrorCode_Success {
		return nil, err
	}
	if len(statesResponse.States) != 1 {
		return nil, errors.New("segment states' len should be 1")
	}

	return statesResponse, nil
}

func (loader *segmentLoader) filterOutVectorFields(binlogPaths []*datapb.FieldBinlog,
	vectorFields []int64) []*datapb.FieldBinlog {

	containsFunc := func(s []int64, e int64) bool {
		for _, a := range s {
			if a == e {
				return true
			}
		}
		return false
	}
	targetFields := make([]*datapb.FieldBinlog, 0)
	for _, path := range binlogPaths {
		if !containsFunc(vectorFields, path.FieldID) {
			targetFields = append(targetFields, path)
		}
	}
	return targetFields
}

func (loader *segmentLoader) loadSegmentFieldsData(segment *Segment, binlogPaths []*datapb.FieldBinlog) error {
	iCodec := storage.InsertCodec{}
	defer func() {
		err := iCodec.Close()
		if err != nil {
			log.Error(err.Error())
		}
	}()
	blobs := make([]*storage.Blob, 0)
	for _, binlogPath := range binlogPaths {
		fieldID := binlogPath.FieldID
		if fieldID == timestampFieldID {
			// seg core doesn't need timestamp field
			continue
		}

		paths := binlogPath.Binlogs
		log.Debug("load segment fields data",
			zap.Int64("segmentID", segment.segmentID),
			zap.Any("fieldID", fieldID),
			zap.String("paths", fmt.Sprintln(paths)),
		)
		blob := &storage.Blob{
			Key:   strconv.FormatInt(fieldID, 10),
			Value: make([]byte, 0),
		}
		for _, path := range paths {
			binLog, err := loader.kv.Load(path)
			if err != nil {
				// TODO: return or continue?
				return err
			}
			blob.Value = append(blob.Value, []byte(binLog)...)
		}
		blobs = append(blobs, blob)
	}

	_, _, insertData, err := iCodec.Deserialize(blobs)
	if err != nil {
		log.Error(err.Error())
		return err
	}
	for fieldID, value := range insertData.Data {
		var numRows int
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
		err = segment.segmentLoadFieldData(fieldID, numRows, data)
		if err != nil {
			// TODO: return or continue?
			return err
		}
	}

	return nil
}

func newSegmentLoader(ctx context.Context, masterService types.MasterService, indexService types.IndexService, dataService types.DataService, replica ReplicaInterface) *segmentLoader {
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

	iLoader := newIndexLoader(ctx, masterService, indexService, replica)
	return &segmentLoader{
		historicalReplica: replica,

		dataService: dataService,

		kv: client,

		indexLoader: iLoader,
	}
}
