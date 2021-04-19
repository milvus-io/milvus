package querynode

import (
	"context"
	"errors"
	"fmt"

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

func (s *segmentManager) loadSegment(segmentID UniqueID, fieldIDs *[]int64) error {
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
			switch fieldData := value.(type) {
			case storage.BoolFieldData:
				numRows := fieldData.NumRows
				data := fieldData.Data
				fmt.Println(numRows, data, fieldID)
				// TODO: s.replica.addSegment()
			case storage.Int8FieldData:
				// TODO: s.replica.addSegment()
			case storage.Int16FieldData:
				// TODO: s.replica.addSegment()
			case storage.Int32FieldData:
				// TODO: s.replica.addSegment()
			case storage.Int64FieldData:
				// TODO: s.replica.addSegment()
			case storage.FloatFieldData:
				// TODO: s.replica.addSegment()
			case storage.DoubleFieldData:
				// TODO: s.replica.addSegment()
			case storage.StringFieldData:
				// TODO: s.replica.addSegment()
			case storage.FloatVectorFieldData:
				// segment to be loaded doesn't need vector field,
				// so we ignore the type of vector field data
				continue
			case storage.BinaryVectorFieldData:
				continue
			default:
				return errors.New("unexpected field data type")
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
	targetSegment, err := s.replica.getSegmentByID(segmentID)
	if err != nil {
		return err
	}

	// get vector field ids from schema to load index
	vecFieldIDs, err := s.replica.getVecFieldIDsBySegmentID(segmentID)
	if err != nil {
		return err
	}
	for _, vecFieldID := range vecFieldIDs {
		targetIndexParam, ok := targetSegment.indexParam[vecFieldID]
		if !ok {
			return errors.New(fmt.Sprint("cannot found index params in segment ", segmentID, " with field = ", vecFieldID))
		}
		// non-blocking send
		go s.sendLoadIndex(pathResponse.IndexFilePaths, segmentID, vecFieldID, "", targetIndexParam)
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
