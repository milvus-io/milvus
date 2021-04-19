package querynode

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"errors"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-distributed/internal/kv"
	minioKV "github.com/zilliztech/milvus-distributed/internal/kv/minio"
	"github.com/zilliztech/milvus-distributed/internal/log"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/util"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/storage"
)

type indexLoader struct {
	replica ReplicaInterface

	fieldIndexes   map[string][]*internalpb2.IndexStats
	fieldStatsChan chan []*internalpb2.FieldStats

	masterClient MasterServiceInterface
	indexClient  IndexServiceInterface

	kv kv.Base // minio kv
}

type loadIndex struct {
	segmentID  UniqueID
	fieldID    int64
	indexPaths []string
}

func (loader *indexLoader) doLoadIndex(wg *sync.WaitGroup) {
	collectionIDs, _, segmentIDs := loader.replica.getSegmentsBySegmentType(segTypeSealed)
	if len(collectionIDs) <= 0 {
		wg.Done()
		return
	}
	log.Debug("do load index for sealed segments:", zap.String("segmentIDs", fmt.Sprintln(segmentIDs)))
	for i := range collectionIDs {
		// we don't need index id yet
		_, buildID, err := loader.getIndexInfo(collectionIDs[i], segmentIDs[i])
		if err != nil {
			log.Warn(err.Error())
			continue
		}
		indexPaths, err := loader.getIndexPaths(buildID)
		if err != nil {
			log.Warn(err.Error())
			continue
		}
		err = loader.loadIndexDelayed(collectionIDs[i], segmentIDs[i], indexPaths)
		if err != nil {
			log.Warn(err.Error())
		}
	}
	// sendQueryNodeStats
	err := loader.sendQueryNodeStats()
	if err != nil {
		log.Error(err.Error())
		wg.Done()
		return
	}

	wg.Done()
}

func (loader *indexLoader) execute(l *loadIndex) error {
	// 1. use msg's index paths to get index bytes
	var err error
	var indexBuffer [][]byte
	var indexParams indexParam
	var indexName string
	var indexID UniqueID
	fn := func() error {
		indexBuffer, indexParams, indexName, indexID, err = loader.loadIndex(l.indexPaths)
		if err != nil {
			return err
		}
		return nil
	}
	err = util.Retry(5, time.Millisecond*200, fn)
	if err != nil {
		return err
	}
	ok, err := loader.checkIndexReady(indexParams, l)
	if err != nil {
		return err
	}
	if ok {
		// no error
		return errors.New("")
	}
	// 2. use index bytes and index path to update segment
	err = loader.updateSegmentIndex(indexParams, indexBuffer, l)
	if err != nil {
		return err
	}
	// 3. update segment index stats
	err = loader.updateSegmentIndexStats(indexParams, indexName, indexID, l)
	if err != nil {
		return err
	}
	log.Debug("load index done")
	return nil
}

func (loader *indexLoader) printIndexParams(index []*commonpb.KeyValuePair) {
	log.Debug("=================================================")
	for i := 0; i < len(index); i++ {
		log.Debug(fmt.Sprintln(index[i]))
	}
}

func (loader *indexLoader) indexParamsEqual(index1 []*commonpb.KeyValuePair, index2 []*commonpb.KeyValuePair) bool {
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

func (loader *indexLoader) fieldsStatsIDs2Key(collectionID UniqueID, fieldID UniqueID) string {
	return strconv.FormatInt(collectionID, 10) + "/" + strconv.FormatInt(fieldID, 10)
}

func (loader *indexLoader) fieldsStatsKey2IDs(key string) (UniqueID, UniqueID, error) {
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

func (loader *indexLoader) updateSegmentIndexStats(indexParams indexParam, indexName string, indexID UniqueID, l *loadIndex) error {
	targetSegment, err := loader.replica.getSegmentByID(l.segmentID)
	if err != nil {
		return err
	}

	fieldStatsKey := loader.fieldsStatsIDs2Key(targetSegment.collectionID, l.fieldID)
	_, ok := loader.fieldIndexes[fieldStatsKey]
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
		loader.fieldIndexes[fieldStatsKey] = make([]*internalpb2.IndexStats, 0)
		loader.fieldIndexes[fieldStatsKey] = append(loader.fieldIndexes[fieldStatsKey],
			&internalpb2.IndexStats{
				IndexParams:        newIndexParams,
				NumRelatedSegments: 1,
			})
	} else {
		isNewIndex := true
		for _, index := range loader.fieldIndexes[fieldStatsKey] {
			if loader.indexParamsEqual(newIndexParams, index.IndexParams) {
				index.NumRelatedSegments++
				isNewIndex = false
			}
		}
		if isNewIndex {
			loader.fieldIndexes[fieldStatsKey] = append(loader.fieldIndexes[fieldStatsKey],
				&internalpb2.IndexStats{
					IndexParams:        newIndexParams,
					NumRelatedSegments: 1,
				})
		}
	}
	err = targetSegment.setIndexParam(l.fieldID, newIndexParams)
	if err != nil {
		return err
	}
	targetSegment.setIndexName(indexName)
	targetSegment.setIndexID(indexID)

	return nil
}

func (loader *indexLoader) loadIndex(indexPath []string) ([][]byte, indexParam, string, UniqueID, error) {
	index := make([][]byte, 0)

	var indexParams indexParam
	var indexName string
	var indexID UniqueID
	for _, p := range indexPath {
		log.Debug("", zap.String("load path", fmt.Sprintln(indexPath)))
		indexPiece, err := loader.kv.Load(p)
		if err != nil {
			return nil, nil, "", -1, err
		}
		// get index params when detecting indexParamPrefix
		if path.Base(p) == storage.IndexParamsFile {
			indexCodec := storage.NewIndexCodec()
			_, indexParams, indexName, indexID, err = indexCodec.Deserialize([]*storage.Blob{
				{
					Key:   storage.IndexParamsFile,
					Value: []byte(indexPiece),
				},
			})
			if err != nil {
				return nil, nil, "", -1, err
			}
		} else {
			index = append(index, []byte(indexPiece))
		}
	}

	if len(indexParams) <= 0 {
		return nil, nil, "", -1, errors.New("cannot find index param")
	}
	return index, indexParams, indexName, indexID, nil
}

func (loader *indexLoader) updateSegmentIndex(indexParams indexParam, bytesIndex [][]byte, l *loadIndex) error {
	segment, err := loader.replica.getSegmentByID(l.segmentID)
	if err != nil {
		return err
	}

	loadIndexInfo, err := newLoadIndexInfo()
	defer deleteLoadIndexInfo(loadIndexInfo)
	if err != nil {
		return err
	}
	err = loadIndexInfo.appendFieldInfo(l.fieldID)
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

func (loader *indexLoader) sendQueryNodeStats() error {
	resultFieldsStats := make([]*internalpb2.FieldStats, 0)
	for fieldStatsKey, indexStats := range loader.fieldIndexes {
		colID, fieldID, err := loader.fieldsStatsKey2IDs(fieldStatsKey)
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

	loader.fieldStatsChan <- resultFieldsStats
	log.Debug("sent field stats")
	return nil
}

func (loader *indexLoader) checkIndexReady(indexParams indexParam, l *loadIndex) (bool, error) {
	segment, err := loader.replica.getSegmentByID(l.segmentID)
	if err != nil {
		return false, err
	}
	if !segment.matchIndexParam(l.fieldID, indexParams) {
		return false, nil
	}
	return true, nil
}

func (loader *indexLoader) getIndexInfo(collectionID UniqueID, segmentID UniqueID) (UniqueID, UniqueID, error) {
	ctx := context.TODO()
	req := &milvuspb.DescribeSegmentRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_kDescribeSegment,
		},
		CollectionID: collectionID,
		SegmentID:    segmentID,
	}
	response, err := loader.masterClient.DescribeSegment(ctx, req)
	if err != nil {
		return 0, 0, err
	}
	if response.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		return -1, -1, errors.New(response.Status.Reason)
	}
	return response.IndexID, response.BuildID, nil
}

func (loader *indexLoader) getIndexPaths(indexBuildID UniqueID) ([]string, error) {
	ctx := context.TODO()
	if loader.indexClient == nil {
		return nil, errors.New("null index service client")
	}

	indexFilePathRequest := &indexpb.IndexFilePathsRequest{
		IndexBuildIDs: []UniqueID{indexBuildID},
	}
	pathResponse, err := loader.indexClient.GetIndexFilePaths(ctx, indexFilePathRequest)
	if err != nil || pathResponse.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
		return nil, err
	}

	if len(pathResponse.FilePaths) <= 0 {
		return nil, errors.New("illegal index file paths")
	}

	return pathResponse.FilePaths[0].IndexFilePaths, nil
}

func (loader *indexLoader) loadIndexImmediate(segment *Segment, indexPaths []string) error {
	// get vector field ids from schema to load index
	vecFieldIDs, err := loader.replica.getVecFieldIDsByCollectionID(segment.collectionID)
	if err != nil {
		return err
	}
	for _, id := range vecFieldIDs {
		l := &loadIndex{
			segmentID:  segment.ID(),
			fieldID:    id,
			indexPaths: indexPaths,
		}

		err = loader.execute(l)
		if err != nil {
			return err
		}
	}
	return nil
}

func (loader *indexLoader) loadIndexDelayed(collectionID, segmentID UniqueID, indexPaths []string) error {
	// get vector field ids from schema to load index
	vecFieldIDs, err := loader.replica.getVecFieldIDsByCollectionID(collectionID)
	if err != nil {
		return err
	}
	for _, id := range vecFieldIDs {
		l := &loadIndex{
			segmentID:  segmentID,
			fieldID:    id,
			indexPaths: indexPaths,
		}

		err = loader.execute(l)
		if err != nil {
			return err
		}
	}

	return nil
}

func newIndexLoader(ctx context.Context, masterClient MasterServiceInterface, indexClient IndexServiceInterface, replica ReplicaInterface) *indexLoader {
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

	return &indexLoader{
		replica: replica,

		fieldIndexes:   make(map[string][]*internalpb2.IndexStats),
		fieldStatsChan: make(chan []*internalpb2.FieldStats, 1),

		masterClient: masterClient,
		indexClient:  indexClient,

		kv: client,
	}
}
