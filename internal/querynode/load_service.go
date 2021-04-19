package querynode

import (
	"context"
	"errors"
	"fmt"
	"log"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/kv"
	minioKV "github.com/zilliztech/milvus-distributed/internal/kv/minio"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/util"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/storage"
)

const indexCheckInterval = 1

type loadService struct {
	ctx    context.Context
	cancel context.CancelFunc

	replica collectionReplica

	fieldIndexes   map[string][]*internalpb2.IndexStats
	fieldStatsChan chan []*internalpb2.FieldStats

	dmStream msgstream.MsgStream

	masterClient MasterServiceInterface
	dataClient   DataServiceInterface
	indexClient  IndexServiceInterface

	kv     kv.Base // minio kv
	iCodec *storage.InsertCodec
}

type loadIndex struct {
	segmentID  UniqueID
	fieldID    int64
	fieldName  string
	indexPaths []string
}

// -------------------------------------------- load index -------------------------------------------- //
func (s *loadService) start() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-time.After(indexCheckInterval * time.Second):
			collectionIDs, segmentIDs := s.replica.getSealedSegments()
			if len(collectionIDs) <= 0 {
				continue
			}
			fmt.Println("do load index for segments:", segmentIDs)
			for i := range collectionIDs {
				// we don't need index id yet
				_, buildID, err := s.getIndexInfo(collectionIDs[i], segmentIDs[i])
				if err != nil {
					indexPaths, err := s.getIndexPaths(buildID)
					if err != nil {
						log.Println(err)
						continue
					}
					err = s.loadIndexDelayed(collectionIDs[i], segmentIDs[i], indexPaths)
					if err != nil {
						log.Println(err)
						continue
					}
				}
			}
			// sendQueryNodeStats
			err := s.sendQueryNodeStats()
			if err != nil {
				log.Println(err)
				continue
			}
		}
	}
}

func (s *loadService) execute(l *loadIndex) error {
	// 1. use msg's index paths to get index bytes
	var err error
	var indexBuffer [][]byte
	var indexParams indexParam
	fn := func() error {
		indexBuffer, indexParams, err = s.loadIndex(l.indexPaths)
		if err != nil {
			return err
		}
		return nil
	}
	err = util.Retry(5, time.Millisecond*200, fn)
	if err != nil {
		return err
	}
	ok, err := s.checkIndexReady(indexParams, l)
	if err != nil {
		return err
	}
	if ok {
		// no error
		return errors.New("")
	}
	// 2. use index bytes and index path to update segment
	err = s.updateSegmentIndex(indexParams, indexBuffer, l)
	if err != nil {
		return err
	}
	//3. update segment index stats
	err = s.updateSegmentIndexStats(indexParams, l)
	if err != nil {
		return err
	}
	fmt.Println("load index done")
	return nil
}

func (s *loadService) close() {
	s.cancel()
}

func (s *loadService) printIndexParams(index []*commonpb.KeyValuePair) {
	fmt.Println("=================================================")
	for i := 0; i < len(index); i++ {
		fmt.Println(index[i])
	}
}

func (s *loadService) indexParamsEqual(index1 []*commonpb.KeyValuePair, index2 []*commonpb.KeyValuePair) bool {
	if len(index1) != len(index2) {
		return false
	}

	for i := 0; i < len(index1); i++ {
		kv1 := *index1[i]
		kv2 := *index2[i]
		if kv1.Key != kv2.Key || kv1.Value != kv2.Value {
			return false
		}
	}

	return true
}

func (s *loadService) fieldsStatsIDs2Key(collectionID UniqueID, fieldID UniqueID) string {
	return strconv.FormatInt(collectionID, 10) + "/" + strconv.FormatInt(fieldID, 10)
}

func (s *loadService) fieldsStatsKey2IDs(key string) (UniqueID, UniqueID, error) {
	ids := strings.Split(key, "/")
	if len(ids) != 2 {
		return 0, 0, errors.New("illegal fieldsStatsKey")
	}
	collectionID, err := strconv.ParseInt(ids[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	fieldID, err := strconv.ParseInt(ids[1], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	return collectionID, fieldID, nil
}

func (s *loadService) updateSegmentIndexStats(indexParams indexParam, l *loadIndex) error {
	targetSegment, err := s.replica.getSegmentByID(l.segmentID)
	if err != nil {
		return err
	}

	fieldStatsKey := s.fieldsStatsIDs2Key(targetSegment.collectionID, l.fieldID)
	_, ok := s.fieldIndexes[fieldStatsKey]
	newIndexParams := make([]*commonpb.KeyValuePair, 0)
	for k, v := range indexParams {
		newIndexParams = append(newIndexParams, &commonpb.KeyValuePair{
			Key:   k,
			Value: v,
		})
	}

	// sort index params by key
	sort.Slice(newIndexParams, func(i, j int) bool { return newIndexParams[i].Key < newIndexParams[j].Key })
	if !ok {
		s.fieldIndexes[fieldStatsKey] = make([]*internalpb2.IndexStats, 0)
		s.fieldIndexes[fieldStatsKey] = append(s.fieldIndexes[fieldStatsKey],
			&internalpb2.IndexStats{
				IndexParams:        newIndexParams,
				NumRelatedSegments: 1,
			})
	} else {
		isNewIndex := true
		for _, index := range s.fieldIndexes[fieldStatsKey] {
			if s.indexParamsEqual(newIndexParams, index.IndexParams) {
				index.NumRelatedSegments++
				isNewIndex = false
			}
		}
		if isNewIndex {
			s.fieldIndexes[fieldStatsKey] = append(s.fieldIndexes[fieldStatsKey],
				&internalpb2.IndexStats{
					IndexParams:        newIndexParams,
					NumRelatedSegments: 1,
				})
		}
	}
	return targetSegment.setIndexParam(l.fieldID, newIndexParams)
}

func (s *loadService) loadIndex(indexPath []string) ([][]byte, indexParam, error) {
	index := make([][]byte, 0)

	var indexParams indexParam
	for _, p := range indexPath {
		fmt.Println("load path = ", indexPath)
		indexPiece, err := s.kv.Load(p)
		if err != nil {
			return nil, nil, err
		}
		// get index params when detecting indexParamPrefix
		if path.Base(p) == storage.IndexParamsFile {
			indexCodec := storage.NewIndexCodec()
			_, indexParams, err = indexCodec.Deserialize([]*storage.Blob{
				{
					Key:   storage.IndexParamsFile,
					Value: []byte(indexPiece),
				},
			})
			if err != nil {
				return nil, nil, err
			}
		} else {
			index = append(index, []byte(indexPiece))
		}
	}

	if len(indexParams) <= 0 {
		return nil, nil, errors.New("cannot find index param")
	}
	return index, indexParams, nil
}

func (s *loadService) updateSegmentIndex(indexParams indexParam, bytesIndex [][]byte, l *loadIndex) error {
	segment, err := s.replica.getSegmentByID(l.segmentID)
	if err != nil {
		return err
	}

	loadIndexInfo, err := newLoadIndexInfo()
	defer deleteLoadIndexInfo(loadIndexInfo)
	if err != nil {
		return err
	}
	err = loadIndexInfo.appendFieldInfo(l.fieldName, l.fieldID)
	if err != nil {
		return err
	}
	for k, v := range indexParams {
		err = loadIndexInfo.appendIndexParam(k, v)
		if err != nil {
			return err
		}
	}
	err = loadIndexInfo.appendIndex(bytesIndex, l.indexPaths)
	if err != nil {
		return err
	}
	return segment.updateSegmentIndex(loadIndexInfo)
}

func (s *loadService) sendQueryNodeStats() error {
	resultFieldsStats := make([]*internalpb2.FieldStats, 0)
	for fieldStatsKey, indexStats := range s.fieldIndexes {
		colID, fieldID, err := s.fieldsStatsKey2IDs(fieldStatsKey)
		if err != nil {
			return err
		}
		fieldStats := internalpb2.FieldStats{
			CollectionID: colID,
			FieldID:      fieldID,
			IndexStats:   indexStats,
		}
		resultFieldsStats = append(resultFieldsStats, &fieldStats)
	}

	s.fieldStatsChan <- resultFieldsStats
	fmt.Println("sent field stats")
	return nil
}

func (s *loadService) checkIndexReady(indexParams indexParam, l *loadIndex) (bool, error) {
	segment, err := s.replica.getSegmentByID(l.segmentID)
	if err != nil {
		return false, err
	}
	if !segment.matchIndexParam(l.fieldID, indexParams) {
		return false, nil
	}
	return true, nil
}

func (s *loadService) getIndexInfo(collectionID UniqueID, segmentID UniqueID) (UniqueID, UniqueID, error) {
	req := &milvuspb.DescribeSegmentRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_kDescribeSegment,
		},
		CollectionID: collectionID,
		SegmentID:    segmentID,
	}
	response, err := s.masterClient.DescribeSegment(req)
	if err != nil {
		return 0, 0, err
	}
	return response.IndexID, response.BuildID, nil
}

// -------------------------------------------- load segment -------------------------------------------- //
func (s *loadService) loadSegment(collectionID UniqueID, partitionID UniqueID, segmentIDs []UniqueID, fieldIDs []int64) error {
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
		// we don't need index id yet
		_, buildID, errIndex := s.getIndexInfo(collectionID, segmentID)
		if errIndex == nil {
			// we don't need load to vector fields
			vectorFields, err := s.replica.getVecFieldsByCollectionID(segmentID)
			if err != nil {
				return err
			}
			fieldIDs = s.filterOutVectorFields(fieldIDs, vectorFields)
		}
		paths, srcFieldIDs, err := s.getInsertBinlogPaths(segmentID)
		if err != nil {
			return err
		}

		targetFields := s.getTargetFields(paths, srcFieldIDs, fieldIDs)
		collection, err := s.replica.getCollectionByID(collectionID)
		if err != nil {
			return err
		}
		segment := newSegment(collection, segmentID, partitionID, collectionID, segTypeSealed)
		err = s.loadSegmentFieldsData(segment, targetFields)
		if err != nil {
			return err
		}
		if errIndex == nil {
			indexPaths, err := s.getIndexPaths(buildID)
			if err != nil {
				return err
			}
			err = s.loadIndexImmediate(segment, indexPaths)
			if err != nil {
				// TODO: return or continue?
				return err
			}
		}
	}
	return nil
}

func (s *loadService) releaseSegment(segmentID UniqueID) error {
	err := s.replica.removeSegment(segmentID)
	return err
}

func (s *loadService) seekSegment(positions []*internalpb2.MsgPosition) error {
	// TODO: open seek
	//for _, position := range positions {
	//	err := s.dmStream.Seek(position)
	//	if err != nil {
	//		return err
	//	}
	//}
	return nil
}

func (s *loadService) getIndexPaths(buildID UniqueID) ([]string, error) {
	if s.indexClient == nil {
		return nil, errors.New("null index service client")
	}

	indexFilePathRequest := &indexpb.IndexFilePathsRequest{
		// TODO: rename indexIDs to buildIDs
		IndexIDs: []UniqueID{buildID},
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

func (s *loadService) loadIndexImmediate(segment *Segment, indexPaths []string) error {
	// get vector field ids from schema to load index
	vecFieldIDs, err := s.replica.getVecFieldsByCollectionID(segment.collectionID)
	if err != nil {
		return err
	}
	for id, name := range vecFieldIDs {
		l := &loadIndex{
			segmentID:  segment.ID(),
			fieldName:  name,
			fieldID:    id,
			indexPaths: indexPaths,
		}

		err = s.execute(l)
		if err != nil {
			return err
		}
		// replace segment
		err = s.replica.replaceGrowingSegmentBySealedSegment(segment)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *loadService) loadIndexDelayed(collectionID, segmentID UniqueID, indexPaths []string) error {
	// get vector field ids from schema to load index
	vecFieldIDs, err := s.replica.getVecFieldsByCollectionID(collectionID)
	if err != nil {
		return err
	}
	for id, name := range vecFieldIDs {
		l := &loadIndex{
			segmentID:  segmentID,
			fieldName:  name,
			fieldID:    id,
			indexPaths: indexPaths,
		}

		err = s.execute(l)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *loadService) getInsertBinlogPaths(segmentID UniqueID) ([]*internalpb2.StringList, []int64, error) {
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

func (s *loadService) filterOutVectorFields(fieldIDs []int64, vectorFields map[int64]string) []int64 {
	targetFields := make([]int64, 0)
	for _, id := range fieldIDs {
		if _, ok := vectorFields[id]; !ok {
			targetFields = append(targetFields, id)
		}
	}
	return targetFields
}

func (s *loadService) getTargetFields(paths []*internalpb2.StringList, srcFieldIDS []int64, dstFields []int64) map[int64]*internalpb2.StringList {
	targetFields := make(map[int64]*internalpb2.StringList)

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

func (s *loadService) loadSegmentFieldsData(segment *Segment, targetFields map[int64]*internalpb2.StringList) error {
	for id, p := range targetFields {
		if id == timestampFieldID {
			// seg core doesn't need timestamp field
			continue
		}

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

func newLoadService(ctx context.Context, masterClient MasterServiceInterface, dataClient DataServiceInterface, indexClient IndexServiceInterface, replica collectionReplica, dmStream msgstream.MsgStream) *loadService {
	ctx1, cancel := context.WithCancel(ctx)

	option := &minioKV.Option{
		Address:           Params.MinioEndPoint,
		AccessKeyID:       Params.MinioAccessKeyID,
		SecretAccessKeyID: Params.MinioSecretAccessKey,
		UseSSL:            Params.MinioUseSSLStr,
		CreateBucket:      true,
		BucketName:        Params.MinioBucketName,
	}

	client, err := minioKV.NewMinIOKV(ctx1, option)
	if err != nil {
		panic(err)
	}

	return &loadService{
		ctx:    ctx1,
		cancel: cancel,

		replica: replica,

		fieldIndexes:   make(map[string][]*internalpb2.IndexStats),
		fieldStatsChan: make(chan []*internalpb2.FieldStats, 1),

		dmStream: dmStream,

		masterClient: masterClient,
		dataClient:   dataClient,
		indexClient:  indexClient,

		kv:     client,
		iCodec: &storage.InsertCodec{},
	}
}
