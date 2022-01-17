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
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/kv"
	minioKV "github.com/milvus-io/milvus/internal/kv/minio"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/retry"
)

type indexParam = map[string]string

// indexLoader is in charge of loading index in query node
type indexLoader struct {
	ctx     context.Context
	replica ReplicaInterface

	fieldIndexes   map[string][]*internalpb.IndexStats
	fieldStatsChan chan []*internalpb.FieldStats

	rootCoord  types.RootCoord
	indexCoord types.IndexCoord

	kv kv.DataKV // minio kv
}

// loadIndex would load index to segment
func (loader *indexLoader) loadIndex(segment *Segment, fieldID FieldID) error {
	// 1. use msg's index paths to get index bytes
	var err error
	var indexBuffer [][]byte
	var indexParams indexParam
	var indexName string
	fn := func() error {
		indexPaths := segment.getIndexPaths(fieldID)
		indexBuffer, indexParams, indexName, err = loader.getIndexBinlog(indexPaths)
		if err != nil {
			return err
		}
		return nil
	}
	//TODO retry should be set by config
	err = retry.Do(loader.ctx, fn, retry.Attempts(10),
		retry.Sleep(time.Second*1), retry.MaxSleepTime(time.Second*10))

	if err != nil {
		return err
	}
	err = segment.setIndexName(fieldID, indexName)
	if err != nil {
		return err
	}
	err = segment.setIndexParam(fieldID, indexParams)
	if err != nil {
		return err
	}
	ok := segment.checkIndexReady(fieldID)
	if !ok {
		// no error
		return errors.New("index info is not set correctly")
	}
	// 2. use index bytes and index path to update segment
	err = segment.updateSegmentIndex(indexBuffer, fieldID)
	if err != nil {
		return err
	}
	// 3. drop vector field data if index loaded successfully
	err = segment.dropFieldData(fieldID)
	if err != nil {
		return err
	}
	log.Debug("load index done")
	return nil
}

// printIndexParams prints the index params
func (loader *indexLoader) printIndexParams(index []*commonpb.KeyValuePair) {
	log.Debug("=================================================")
	for i := 0; i < len(index); i++ {
		log.Debug(fmt.Sprintln(index[i]))
	}
}

// getIndexBinlog would load index and index params from storage
func (loader *indexLoader) getIndexBinlog(indexPath []string) ([][]byte, indexParam, string, error) {
	index := make([][]byte, 0)

	var indexParams indexParam
	var indexName string
	indexCodec := storage.NewIndexFileBinlogCodec()
	for _, p := range indexPath {
		log.Debug("", zap.String("load path", fmt.Sprintln(p)))
		indexPiece, err := loader.kv.Load(p)
		if err != nil {
			return nil, nil, "", err
		}
		// get index params when detecting indexParamPrefix
		if path.Base(p) == storage.IndexParamsKey {
			_, indexParams, indexName, _, err = indexCodec.Deserialize([]*storage.Blob{
				{
					Key:   storage.IndexParamsKey,
					Value: []byte(indexPiece),
				},
			})
			if err != nil {
				return nil, nil, "", err
			}
		} else {
			data, _, _, _, err := indexCodec.Deserialize([]*storage.Blob{
				{
					Key:   path.Base(p), // though key is not important here
					Value: []byte(indexPiece),
				},
			})
			if err != nil {
				return nil, nil, "", err
			}
			index = append(index, data[0].Value)
		}
	}

	if len(indexParams) <= 0 {
		return nil, nil, "", errors.New("cannot find index param")
	}
	return index, indexParams, indexName, nil
}

// estimateIndexBinlogSize returns estimated index size
func (loader *indexLoader) estimateIndexBinlogSize(segment *Segment, fieldID FieldID) (int64, error) {
	indexSize := int64(0)
	indexPaths := segment.getIndexPaths(fieldID)
	for _, p := range indexPaths {
		logSize, err := storage.EstimateMemorySize(loader.kv, p)
		if err != nil {
			logSize, err = storage.GetBinlogSize(loader.kv, p)
			if err != nil {
				return 0, err
			}
		}
		indexSize += logSize
	}
	log.Debug("estimate segment index size",
		zap.Any("collectionID", segment.collectionID),
		zap.Any("segmentID", segment.ID()),
		zap.Any("fieldID", fieldID),
		zap.Any("indexPaths", indexPaths),
	)
	return indexSize, nil
}

// getIndexInfo gets indexInfo from RootCoord and IndexCoord
func (loader *indexLoader) getIndexInfo(collectionID UniqueID, segment *Segment) (*indexInfo, error) {
	if loader.indexCoord == nil || loader.rootCoord == nil {
		return nil, errors.New("null indexcoord client or rootcoord client, collectionID = " +
			fmt.Sprintln(collectionID))
	}

	// request for segment info
	req := &milvuspb.DescribeSegmentRequest{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_DescribeSegment,
		},
		CollectionID: collectionID,
		SegmentID:    segment.segmentID,
	}
	resp, err := loader.rootCoord.DescribeSegment(loader.ctx, req)
	if err != nil {
		return nil, err
	}
	if resp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return nil, errors.New(resp.Status.Reason)
	}

	if !resp.EnableIndex {
		log.Warn("index not enabled", zap.Int64("collection id", collectionID),
			zap.Int64("segment id", segment.segmentID))
		return nil, errors.New("there are no indexes on this segment")
	}

	// request for index info
	indexFilePathReq := &indexpb.GetIndexFilePathsRequest{
		IndexBuildIDs: []UniqueID{resp.BuildID},
	}
	pathResp, err := loader.indexCoord.GetIndexFilePaths(loader.ctx, indexFilePathReq)
	if err != nil {
		return nil, err
	}
	if pathResp.Status.ErrorCode != commonpb.ErrorCode_Success {
		return nil, errors.New(pathResp.Status.Reason)
	}

	if len(pathResp.FilePaths) <= 0 {
		log.Warn("illegal index file path", zap.Int64("collection id", collectionID),
			zap.Int64("segment id", segment.segmentID), zap.Int64("build id", resp.BuildID))
		return nil, errors.New("illegal index file paths")
	}
	if len(pathResp.FilePaths[0].IndexFilePaths) == 0 {
		log.Warn("empty index path", zap.Int64("collection id", collectionID),
			zap.Int64("segment id", segment.segmentID), zap.Int64("build id", resp.BuildID))
		return nil, errors.New("empty index paths")
	}

	return &indexInfo{
		indexID:    resp.IndexID,
		buildID:    resp.BuildID,
		fieldID:    resp.FieldID,
		indexPaths: pathResp.FilePaths[0].IndexFilePaths,
		readyLoad:  true,
	}, nil
}

// setIndexInfo sets indexInfo for segment
func (loader *indexLoader) setIndexInfo(segment *Segment, info *indexInfo) {
	segment.setEnableIndex(true)
	segment.setIndexInfo(info.fieldID, info)
}

// newIndexLoader returns a new indexLoader
func newIndexLoader(ctx context.Context, rootCoord types.RootCoord, indexCoord types.IndexCoord, replica ReplicaInterface) *indexLoader {
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

	return &indexLoader{
		ctx:     ctx,
		replica: replica,

		fieldIndexes:   make(map[string][]*internalpb.IndexStats),
		fieldStatsChan: make(chan []*internalpb.FieldStats, 1024),

		rootCoord:  rootCoord,
		indexCoord: indexCoord,

		kv: client,
	}
}

//// deprecated
//func (loader *indexLoader) doLoadIndex(wg *sync.WaitGroup) {
//	collectionIDs, _, segmentIDs := loader.replica.getSegmentsBySegmentType(segmentTypeSealed)
//	if len(collectionIDs) <= 0 {
//		wg.Done()
//		return
//	}
//	log.Debug("do load index for sealed segments:", zap.String("segmentIDs", fmt.Sprintln(segmentIDs)))
//	for i := range collectionIDs {
//		// we don't need index id yet
//		segment, err := loader.replica.getSegmentByID(segmentIDs[i])
//		if err != nil {
//			log.Warn(err.Error())
//			continue
//		}
//		vecFieldIDs, err := loader.replica.getVecFieldIDsByCollectionID(collectionIDs[i])
//		if err != nil {
//			log.Warn(err.Error())
//			continue
//		}
//		for _, fieldID := range vecFieldIDs {
//			err = loader.setIndexInfo(collectionIDs[i], segment, fieldID)
//			if err != nil {
//				log.Warn(err.Error())
//				continue
//			}
//
//			err = loader.loadIndex(segment, fieldID)
//			if err != nil {
//				log.Warn(err.Error())
//				continue
//			}
//		}
//	}
//	// sendQueryNodeStats
//	err := loader.sendQueryNodeStats()
//	if err != nil {
//		log.Warn(err.Error())
//		wg.Done()
//		return
//	}
//
//	wg.Done()
//}
//
//func (loader *indexLoader) getIndexPaths(indexBuildID UniqueID) ([]string, error) {
//	ctx := context.TODO()
//	if loader.indexCoord == nil {
//		return nil, errors.New("null index coordinator client")
//	}
//
//	indexFilePathRequest := &indexpb.GetIndexFilePathsRequest{
//		IndexBuildIDs: []UniqueID{indexBuildID},
//	}
//	pathResponse, err := loader.indexCoord.GetIndexFilePaths(ctx, indexFilePathRequest)
//	if err != nil || pathResponse.Status.ErrorCode != commonpb.ErrorCode_Success {
//		return nil, err
//	}
//
//	if len(pathResponse.FilePaths) <= 0 {
//		return nil, errors.New("illegal index file paths")
//	}
//
//	return pathResponse.FilePaths[0].IndexFilePaths, nil
//}
//
//func (loader *indexLoader) indexParamsEqual(index1 []*commonpb.KeyValuePair, index2 []*commonpb.KeyValuePair) bool {
//	if len(index1) != len(index2) {
//		return false
//	}
//
//	for i := 0; i < len(index1); i++ {
//		kv1 := *index1[i]
//		kv2 := *index2[i]
//		if kv1.Key != kv2.Key || kv1.Value != kv2.Value {
//			return false
//		}
//	}
//
//	return true
//}
//
//func (loader *indexLoader) fieldsStatsIDs2Key(collectionID UniqueID, fieldID UniqueID) string {
//	return strconv.FormatInt(collectionID, 10) + "/" + strconv.FormatInt(fieldID, 10)
//}
//
//func (loader *indexLoader) fieldsStatsKey2IDs(key string) (UniqueID, UniqueID, error) {
//	ids := strings.Split(key, "/")
//	if len(ids) != 2 {
//		return 0, 0, errors.New("illegal fieldsStatsKey")
//	}
//	collectionID, err := strconv.ParseInt(ids[0], 10, 64)
//	if err != nil {
//		return 0, 0, err
//	}
//	fieldID, err := strconv.ParseInt(ids[1], 10, 64)
//	if err != nil {
//		return 0, 0, err
//	}
//	return collectionID, fieldID, nil
//}
//
//func (loader *indexLoader) updateSegmentIndexStats(segment *Segment) error {
//	for fieldID := range segment.indexInfos {
//		fieldStatsKey := loader.fieldsStatsIDs2Key(segment.collectionID, fieldID)
//		_, ok := loader.fieldIndexes[fieldStatsKey]
//		newIndexParams := make([]*commonpb.KeyValuePair, 0)
//		indexParams := segment.getIndexParams(fieldID)
//		for k, v := range indexParams {
//			newIndexParams = append(newIndexParams, &commonpb.KeyValuePair{
//				Key:   k,
//				Value: v,
//			})
//		}
//
//		// sort index params by key
//		sort.Slice(newIndexParams, func(i, j int) bool { return newIndexParams[i].Key < newIndexParams[j].Key })
//		if !ok {
//			loader.fieldIndexes[fieldStatsKey] = make([]*internalpb.IndexStats, 0)
//			loader.fieldIndexes[fieldStatsKey] = append(loader.fieldIndexes[fieldStatsKey],
//				&internalpb.IndexStats{
//					IndexParams:        newIndexParams,
//					NumRelatedSegments: 1,
//				})
//		} else {
//			isNewIndex := true
//			for _, index := range loader.fieldIndexes[fieldStatsKey] {
//				if loader.indexParamsEqual(newIndexParams, index.IndexParams) {
//					index.NumRelatedSegments++
//					isNewIndex = false
//				}
//			}
//			if isNewIndex {
//				loader.fieldIndexes[fieldStatsKey] = append(loader.fieldIndexes[fieldStatsKey],
//					&internalpb.IndexStats{
//						IndexParams:        newIndexParams,
//						NumRelatedSegments: 1,
//					})
//			}
//		}
//	}
//
//	return nil
//}
//
//func (loader *indexLoader) sendQueryNodeStats() error {
//	resultFieldsStats := make([]*internalpb.FieldStats, 0)
//	for fieldStatsKey, indexStats := range loader.fieldIndexes {
//		colID, fieldID, err := loader.fieldsStatsKey2IDs(fieldStatsKey)
//		if err != nil {
//			return err
//		}
//		fieldStats := internalpb.FieldStats{
//			CollectionID: colID,
//			FieldID:      fieldID,
//			IndexStats:   indexStats,
//		}
//		resultFieldsStats = append(resultFieldsStats, &fieldStats)
//	}
//
//	loader.fieldStatsChan <- resultFieldsStats
//	log.Debug("sent field stats")
//	return nil
//}
