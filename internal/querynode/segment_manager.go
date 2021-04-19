package querynode

import (
	"context"
	"errors"
	"unsafe"

	"github.com/zilliztech/milvus-distributed/internal/kv"
	miniokv "github.com/zilliztech/milvus-distributed/internal/kv/minio"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	queryPb "github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	"github.com/zilliztech/milvus-distributed/internal/storage"
)

type segmentManager struct {
	replica collectionReplica

	loadIndexReqChan chan []msgstream.TsMsg

	// TODO: replace by client instead of grpc client
	dataClient         datapb.DataServiceClient
	indexBuilderClient indexpb.IndexServiceClient

	kv     kv.Base // minio kv
	iCodec *storage.InsertCodec
}

func newSegmentManager(ctx context.Context, replica collectionReplica, loadIndexReqChan chan []msgstream.TsMsg) *segmentManager {
	bucketName := Params.MinioBucketName
	option := &miniokv.Option{
		Address:           Params.MinioEndPoint,
		AccessKeyID:       Params.MinioAccessKeyID,
		SecretAccessKeyID: Params.MinioSecretAccessKey,
		UseSSL:            Params.MinioUseSSLStr,
		BucketName:        bucketName,
		CreateBucket:      true,
	}

	minioKV, err := miniokv.NewMinIOKV(ctx, option)
	if err != nil {
		panic(err)
	}

	return &segmentManager{
		replica:          replica,
		loadIndexReqChan: loadIndexReqChan,

		// TODO: init clients
		dataClient:         nil,
		indexBuilderClient: nil,

		kv:     minioKV,
		iCodec: &storage.InsertCodec{},
	}
}

func (s *segmentManager) loadSegment(segmentID UniqueID, partitionID UniqueID, collectionID UniqueID, fieldIDs *[]int64) error {
	insertBinlogPathRequest := &datapb.InsertBinlogPathRequest{
		SegmentID: segmentID,
	}

	pathResponse, err := s.dataClient.GetInsertBinlogPaths(context.TODO(), insertBinlogPathRequest)
	if err != nil {
		return err
	}

	if len(pathResponse.FieldIDs) != len(pathResponse.Paths) {
		return errors.New("illegal InsertBinlogPathsResponse")
	}

	// create segment
	err = s.replica.addSegment(segmentID, partitionID, collectionID, segTypeSealed)
	if err != nil {
		return err
	}

	containsFunc := func(s []int64, e int64) bool {
		for _, a := range s {
			if a == e {
				return true
			}
		}
		return false
	}

	for i, fieldID := range pathResponse.FieldIDs {
		// filter out the needless fields
		if !containsFunc(*fieldIDs, fieldID) {
			continue
		}

		paths := pathResponse.Paths[i].Values
		blobs := make([]*storage.Blob, 0)
		for _, path := range paths {
			binLog, err := s.kv.Load(path)
			if err != nil {
				// TODO: return or continue?
				return err
			}
			blobs = append(blobs, &storage.Blob{
				Key:   "", // TODO: key???
				Value: []byte(binLog),
			})
		}
		_, _, insertData, err := s.iCodec.Deserialize(blobs)
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
			case storage.BoolFieldData:
				numRows = fieldData.NumRows
				data = fieldData.Data
			case storage.Int8FieldData:
				numRows = fieldData.NumRows
				data = fieldData.Data
			case storage.Int16FieldData:
				numRows = fieldData.NumRows
				data = fieldData.Data
			case storage.Int32FieldData:
				numRows = fieldData.NumRows
				data = fieldData.Data
			case storage.Int64FieldData:
				numRows = fieldData.NumRows
				data = fieldData.Data
			case storage.FloatFieldData:
				numRows = fieldData.NumRows
				data = fieldData.Data
			case storage.DoubleFieldData:
				numRows = fieldData.NumRows
				data = fieldData.Data
			case storage.StringFieldData:
				numRows = fieldData.NumRows
				data = fieldData.Data
			case storage.FloatVectorFieldData:
				// segment to be loaded doesn't need vector field,
				// so we ignore the type of vector field data
				continue
			case storage.BinaryVectorFieldData:
				continue
			default:
				return errors.New("unexpected field data type")
			}

			segment, err := s.replica.getSegmentByID(segmentID)
			if err != nil {
				// TODO: return or continue?
				return err
			}
			err = segment.segmentLoadFieldData(fieldID, numRows, unsafe.Pointer(&data))
			if err != nil {
				// TODO: return or continue?
				return err
			}
		}
	}

	return nil
}

func (s *segmentManager) loadIndex(segmentID UniqueID, indexID UniqueID) error {
	indexFilePathRequest := &indexpb.IndexFilePathRequest{
		IndexID: indexID,
	}
	pathResponse, err := s.indexBuilderClient.GetIndexFilePaths(context.TODO(), indexFilePathRequest)
	if err != nil || pathResponse.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		return err
	}

	// get vector field ids from schema to load index
	vecFieldIDs, err := s.replica.getVecFieldsBySegmentID(segmentID)
	if err != nil {
		return err
	}
	for id, name := range vecFieldIDs {
		var targetIndexParam indexParam
		// TODO: get index param from master
		// non-blocking send
		go s.sendLoadIndex(pathResponse.IndexFilePaths, segmentID, id, name, targetIndexParam)
	}

	return nil
}

func (s *segmentManager) sendLoadIndex(indexPaths []string,
	segmentID int64,
	fieldID int64,
	fieldName string,
	indexParams map[string]string) {
	var indexParamsKV []*commonpb.KeyValuePair
	for key, value := range indexParams {
		indexParamsKV = append(indexParamsKV, &commonpb.KeyValuePair{
			Key:   key,
			Value: value,
		})
	}

	loadIndexRequest := internalPb.LoadIndex{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_kSearchResult,
		},
		SegmentID:   segmentID,
		FieldName:   fieldName,
		FieldID:     fieldID,
		IndexPaths:  indexPaths,
		IndexParams: indexParamsKV,
	}

	loadIndexMsg := &msgstream.LoadIndexMsg{
		LoadIndex: loadIndexRequest,
	}

	messages := []msgstream.TsMsg{loadIndexMsg}
	s.loadIndexReqChan <- messages
}

func (s *segmentManager) releaseSegment(in *queryPb.ReleaseSegmentRequest) error {
	// TODO: implement
	// TODO: release specific field, we need segCore supply relevant interface
	return nil
}
