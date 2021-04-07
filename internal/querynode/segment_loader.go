package querynode

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/kv"
	minioKV "github.com/zilliztech/milvus-distributed/internal/kv/minio"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/storage"
	"github.com/zilliztech/milvus-distributed/internal/types"
)

// segmentLoader is only responsible for loading the field data from binlog
type segmentLoader struct {
	replica ReplicaInterface

	dataService types.DataService

	kv kv.Base // minio kv

	indexLoader *indexLoader
}

func (loader *segmentLoader) getInsertBinlogPaths(segmentID UniqueID) ([]*internalpb.StringList, []int64, error) {
	ctx := context.TODO()
	if loader.dataService == nil {
		return nil, nil, errors.New("null data service client")
	}

	insertBinlogPathRequest := &datapb.GetInsertBinlogPathsRequest{
		SegmentID: segmentID,
	}

	pathResponse, err := loader.dataService.GetInsertBinlogPaths(ctx, insertBinlogPathRequest)
	if err != nil || pathResponse.Status.ErrorCode != commonpb.ErrorCode_Success {
		return nil, nil, err
	}

	if len(pathResponse.FieldIDs) != len(pathResponse.Paths) || len(pathResponse.FieldIDs) <= 0 {
		return nil, nil, errors.New("illegal GetInsertBinlogPathsResponse")
	}

	return pathResponse.Paths, pathResponse.FieldIDs, nil
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

func (loader *segmentLoader) filterOutVectorFields(fieldIDs []int64, vectorFields []int64) []int64 {
	containsFunc := func(s []int64, e int64) bool {
		for _, a := range s {
			if a == e {
				return true
			}
		}
		return false
	}
	targetFields := make([]int64, 0)
	for _, id := range fieldIDs {
		if !containsFunc(vectorFields, id) {
			targetFields = append(targetFields, id)
		}
	}
	return targetFields
}

func (loader *segmentLoader) checkTargetFields(paths []*internalpb.StringList, srcFieldIDs []int64, dstFieldIDs []int64) (map[int64]*internalpb.StringList, error) {
	targetFields := make(map[int64]*internalpb.StringList)

	containsFunc := func(s []int64, e int64) bool {
		for _, a := range s {
			if a == e {
				return true
			}
		}
		return false
	}

	for _, fieldID := range dstFieldIDs {
		if !containsFunc(srcFieldIDs, fieldID) {
			return nil, errors.New("uncompleted fields")
		}
	}

	for i := range srcFieldIDs {
		targetFields[srcFieldIDs[i]] = paths[i]
	}

	return targetFields, nil
}

func (loader *segmentLoader) loadSegmentFieldsData(segment *Segment, targetFields map[int64]*internalpb.StringList) error {
	iCodec := storage.InsertCodec{}
	defer iCodec.Close()
	for id, p := range targetFields {
		if id == timestampFieldID {
			// seg core doesn't need timestamp field
			continue
		}

		paths := p.Values
		blobs := make([]*storage.Blob, 0)
		log.Debug("loadSegmentFieldsData", zap.Int64("segmentID", segment.segmentID), zap.String("path", fmt.Sprintln(paths)))
		for _, path := range paths {
			binLog, err := loader.kv.Load(path)
			if err != nil {
				// TODO: return or continue?
				return err
			}
			blobs = append(blobs, &storage.Blob{
				Key:   strconv.FormatInt(id, 10), // TODO: key???
				Value: []byte(binLog),
			})
		}
		_, _, insertData, err := iCodec.Deserialize(blobs)
		if err != nil {
			// TODO: return or continue
			return err
		}
		if len(insertData.Data) != 1 {
			return errors.New("we expect only one field in deserialized insert data")
		}

		for _, value := range insertData.Data {
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
			err = segment.segmentLoadFieldData(id, numRows, data)
			if err != nil {
				// TODO: return or continue?
				return err
			}
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
		replica: replica,

		dataService: dataService,

		kv: client,

		indexLoader: iLoader,
	}
}
