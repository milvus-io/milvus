package querynode

import (
	"context"
	"errors"
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/kv"
	minioKV "github.com/zilliztech/milvus-distributed/internal/kv/minio"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/storage"
)

// segmentLoader is only responsible for loading the field data from binlog
type segmentLoader struct {
	replica collectionReplica

	dmStream msgstream.MsgStream

	dataClient DataServiceInterface

	kv     kv.Base // minio kv
	iCodec *storage.InsertCodec

	indexLoader *indexLoader
}

func (loader *segmentLoader) releaseSegment(segmentID UniqueID) error {
	err := loader.replica.removeSegment(segmentID)
	return err
}

func (loader *segmentLoader) seekSegment(position *internalpb2.MsgPosition) error {
	// TODO: open seek
	//for _, position := range positions {
	//	err := s.dmStream.Seek(position)
	//	if err != nil {
	//		return err
	//	}
	//}
	return nil
}

func (loader *segmentLoader) getInsertBinlogPaths(segmentID UniqueID) ([]*internalpb2.StringList, []int64, error) {
	if loader.dataClient == nil {
		return nil, nil, errors.New("null data service client")
	}

	insertBinlogPathRequest := &datapb.InsertBinlogPathRequest{
		SegmentID: segmentID,
	}

	pathResponse, err := loader.dataClient.GetInsertBinlogPaths(insertBinlogPathRequest)
	if err != nil || pathResponse.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		return nil, nil, err
	}

	if len(pathResponse.FieldIDs) != len(pathResponse.Paths) || len(pathResponse.FieldIDs) <= 0 {
		return nil, nil, errors.New("illegal InsertBinlogPathsResponse")
	}

	return pathResponse.Paths, pathResponse.FieldIDs, nil
}

func (loader *segmentLoader) GetSegmentStates(segmentID UniqueID) (*datapb.SegmentStatesResponse, error) {
	if loader.dataClient == nil {
		return nil, errors.New("null data service client")
	}

	segmentStatesRequest := &datapb.SegmentStatesRequest{
		SegmentIDs: []int64{segmentID},
	}
	statesResponse, err := loader.dataClient.GetSegmentStates(segmentStatesRequest)
	if err != nil || statesResponse.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
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

func (loader *segmentLoader) checkTargetFields(paths []*internalpb2.StringList, srcFieldIDs []int64, dstFieldIDs []int64) (map[int64]*internalpb2.StringList, error) {
	targetFields := make(map[int64]*internalpb2.StringList)

	containsFunc := func(s []int64, e int64) bool {
		for _, a := range s {
			if a == e {
				return true
			}
		}
		return false
	}

	for i, fieldID := range dstFieldIDs {
		if !containsFunc(srcFieldIDs, fieldID) {
			return nil, errors.New("uncompleted fields")
		}
		targetFields[fieldID] = paths[i]
	}

	return targetFields, nil
}

func (loader *segmentLoader) loadSegmentFieldsData(segment *Segment, targetFields map[int64]*internalpb2.StringList) error {
	for id, p := range targetFields {
		if id == timestampFieldID {
			// seg core doesn't need timestamp field
			continue
		}

		paths := p.Values
		blobs := make([]*storage.Blob, 0)
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
		_, _, insertData, err := loader.iCodec.Deserialize(blobs)
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

func newSegmentLoader(ctx context.Context, masterClient MasterServiceInterface, indexClient IndexServiceInterface, dataClient DataServiceInterface, replica collectionReplica, dmStream msgstream.MsgStream) *segmentLoader {
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

	iLoader := newIndexLoader(ctx, masterClient, indexClient, replica)
	return &segmentLoader{
		replica: replica,

		dmStream: dmStream,

		dataClient: dataClient,

		kv:     client,
		iCodec: &storage.InsertCodec{},

		indexLoader: iLoader,
	}
}
