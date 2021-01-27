package querynode

import (
	"context"
	"errors"
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/kv"
	miniokv "github.com/zilliztech/milvus-distributed/internal/kv/minio"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/storage"
)

type segmentManager struct {
	replica collectionReplica

	dmStream         msgstream.MsgStream
	loadIndexReqChan chan []msgstream.TsMsg

	masterClient MasterServiceInterface
	dataClient   DataServiceInterface
	indexClient  IndexServiceInterface

	kv     kv.Base // minio kv
	iCodec *storage.InsertCodec
}

func (s *segmentManager) seekSegment(positions []*internalPb.MsgPosition) error {
	// TODO: open seek
	//for _, position := range positions {
	//	err := s.dmStream.Seek(position)
	//	if err != nil {
	//		return err
	//	}
	//}
	return nil
}

func (s *segmentManager) getIndexInfo(collectionID UniqueID, segmentID UniqueID) (UniqueID, indexParam, error) {
	req := &milvuspb.DescribeSegmentRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_kDescribeSegment,
		},
		CollectionID: collectionID,
		SegmentID:    segmentID,
	}
	response, err := s.masterClient.DescribeSegment(req)
	if err != nil {
		return 0, nil, err
	}
	if len(response.IndexDescription.Params) <= 0 {
		return 0, nil, errors.New("null index param")
	}

	var targetIndexParam = make(map[string]string)
	for _, param := range response.IndexDescription.Params {
		targetIndexParam[param.Key] = param.Value
	}
	return response.IndexID, targetIndexParam, nil
}

func (s *segmentManager) loadSegment(collectionID UniqueID, partitionID UniqueID, segmentIDs []UniqueID, fieldIDs []int64) error {
	// TODO: interim solution
	if len(fieldIDs) == 0 {
		collection, err := s.replica.getCollectionByID(collectionID)
		if err != nil {
			return err
		}
		fieldIDs = make([]int64, 0)
		for _, field := range collection.Schema().Fields {
			fieldIDs = append(fieldIDs, field.FieldID)
		}
	}
	for _, segmentID := range segmentIDs {
		indexID, indexParams, err := s.getIndexInfo(collectionID, segmentID)
		if err != nil {
			return err
		}
		paths, srcFieldIDs, err := s.getInsertBinlogPaths(segmentID)
		if err != nil {
			return err
		}

		targetFields := s.filterOutNeedlessFields(paths, srcFieldIDs, fieldIDs)
		// replace segment
		err = s.replica.removeSegment(segmentID)
		if err != nil {
			return err
		}
		err = s.replica.addSegment(segmentID, partitionID, collectionID, segTypeSealed)
		if err != nil {
			return err
		}
		err = s.loadSegmentFieldsData(segmentID, targetFields)
		if err != nil {
			return err
		}
		indexPaths, err := s.getIndexPaths(indexID)
		if err != nil {
			return err
		}
		err = s.loadIndex(segmentID, indexPaths, indexParams)
		if err != nil {
			// TODO: return or continue?
			return err
		}
	}
	return nil
}

func (s *segmentManager) releaseSegment(segmentID UniqueID) error {
	err := s.replica.removeSegment(segmentID)
	return err
}

//------------------------------------------------------------------------------------------------- internal functions
func (s *segmentManager) getInsertBinlogPaths(segmentID UniqueID) ([]*internalPb.StringList, []int64, error) {
	if s.dataClient == nil {
		return nil, nil, errors.New("null data service client")
	}

	insertBinlogPathRequest := &datapb.InsertBinlogPathRequest{
		SegmentID: segmentID,
	}

	pathResponse, err := s.dataClient.GetInsertBinlogPaths(insertBinlogPathRequest)
	if err != nil {
		return nil, nil, err
	}

	if len(pathResponse.FieldIDs) != len(pathResponse.Paths) {
		return nil, nil, errors.New("illegal InsertBinlogPathsResponse")
	}

	return pathResponse.Paths, pathResponse.FieldIDs, nil
}

func (s *segmentManager) filterOutNeedlessFields(paths []*internalPb.StringList, srcFieldIDS []int64, dstFields []int64) map[int64]*internalPb.StringList {
	targetFields := make(map[int64]*internalPb.StringList)

	containsFunc := func(s []int64, e int64) bool {
		for _, a := range s {
			if a == e {
				return true
			}
		}
		return false
	}

	for i, fieldID := range srcFieldIDS {
		if containsFunc(dstFields, fieldID) {
			targetFields[fieldID] = paths[i]
		}
	}

	return targetFields
}

func (s *segmentManager) loadSegmentFieldsData(segmentID UniqueID, targetFields map[int64]*internalPb.StringList) error {
	for id, p := range targetFields {
		paths := p.Values
		blobs := make([]*storage.Blob, 0)
		for _, path := range paths {
			binLog, err := s.kv.Load(path)
			if err != nil {
				// TODO: return or continue?
				return err
			}
			blobs = append(blobs, &storage.Blob{
				Key:   strconv.FormatInt(id, 10), // TODO: key???
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
				// segment to be loaded doesn't need vector field,
				// so we ignore the type of vector field data
				continue
			case *storage.BinaryVectorFieldData:
				continue
			default:
				return errors.New("unexpected field data type")
			}

			segment, err := s.replica.getSegmentByID(segmentID)
			if err != nil {
				// TODO: return or continue?
				return err
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

func (s *segmentManager) getIndexPaths(indexID UniqueID) ([]string, error) {
	if s.indexClient == nil {
		return nil, errors.New("null index service client")
	}

	indexFilePathRequest := &indexpb.IndexFilePathsRequest{
		IndexIDs: []UniqueID{indexID},
	}
	pathResponse, err := s.indexClient.GetIndexFilePaths(indexFilePathRequest)
	if err != nil || pathResponse.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		return nil, err
	}

	if len(pathResponse.FilePaths) <= 0 {
		return nil, errors.New("illegal index file paths")
	}

	return pathResponse.FilePaths[0].IndexFilePaths, nil
}

func (s *segmentManager) loadIndex(segmentID UniqueID, indexPaths []string, indexParam indexParam) error {
	// get vector field ids from schema to load index
	vecFieldIDs, err := s.replica.getVecFieldsBySegmentID(segmentID)
	if err != nil {
		return err
	}
	for id, name := range vecFieldIDs {
		// non-blocking send
		go s.sendLoadIndex(indexPaths, segmentID, id, name, indexParam)
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

func newSegmentManager(ctx context.Context, masterClient MasterServiceInterface, dataClient DataServiceInterface, indexClient IndexServiceInterface, replica collectionReplica, dmStream msgstream.MsgStream, loadIndexReqChan chan []msgstream.TsMsg) *segmentManager {
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
		dmStream:         dmStream,
		loadIndexReqChan: loadIndexReqChan,

		masterClient: masterClient,
		dataClient:   dataClient,
		indexClient:  indexClient,

		kv:     minioKV,
		iCodec: &storage.InsertCodec{},
	}
}
