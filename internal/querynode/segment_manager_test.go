package querynode

import (
	"context"
	"fmt"
	"math/rand"
	"path"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-distributed/internal/indexnode"
	minioKV "github.com/zilliztech/milvus-distributed/internal/kv/minio"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/storage"
)

func generateInsertBinLog(collectionID UniqueID, partitionID UniqueID, segmentID UniqueID) ([]*internalPb.StringList, []int64, error) {
	const (
		msgLength = 1000
		DIM       = 16
	)

	idData := make([]int64, 0)
	for n := 0; n < msgLength; n++ {
		idData = append(idData, int64(n))
	}

	var timestamps []int64
	for n := 0; n < msgLength; n++ {
		timestamps = append(timestamps, int64(n+1))
	}

	var fieldAgeData []int32
	for n := 0; n < msgLength; n++ {
		fieldAgeData = append(fieldAgeData, int32(n))
	}

	fieldVecData := make([]float32, 0)
	for n := 0; n < msgLength; n++ {
		for i := 0; i < DIM; i++ {
			fieldVecData = append(fieldVecData, float32(n*i)*0.1)
		}
	}

	insertData := &storage.InsertData{
		Data: map[int64]storage.FieldData{
			0: &storage.Int64FieldData{
				NumRows: msgLength,
				Data:    idData,
			},
			1: &storage.Int64FieldData{
				NumRows: msgLength,
				Data:    timestamps,
			},
			100: &storage.FloatVectorFieldData{
				NumRows: msgLength,
				Data:    fieldVecData,
				Dim:     DIM,
			},
			101: &storage.Int32FieldData{
				NumRows: msgLength,
				Data:    fieldAgeData,
			},
		},
	}

	// buffer data to binLogs
	collMeta := genTestCollectionMeta("collection0", collectionID, false)
	collMeta.Schema.Fields = append(collMeta.Schema.Fields, &schemapb.FieldSchema{
		FieldID:  0,
		Name:     "uid",
		DataType: schemapb.DataType_INT64,
	})
	collMeta.Schema.Fields = append(collMeta.Schema.Fields, &schemapb.FieldSchema{
		FieldID:  1,
		Name:     "timestamp",
		DataType: schemapb.DataType_INT64,
	})
	inCodec := storage.NewInsertCodec(collMeta)
	binLogs, err := inCodec.Serialize(partitionID, segmentID, insertData)

	if err != nil {
		return nil, nil, err
	}

	// create minio client
	bucketName := Params.MinioBucketName
	option := &minioKV.Option{
		Address:           Params.MinioEndPoint,
		AccessKeyID:       Params.MinioAccessKeyID,
		SecretAccessKeyID: Params.MinioSecretAccessKey,
		UseSSL:            Params.MinioUseSSLStr,
		BucketName:        bucketName,
		CreateBucket:      true,
	}
	kv, err := minioKV.NewMinIOKV(context.Background(), option)
	if err != nil {
		return nil, nil, err
	}

	// binLogs -> minIO/S3
	collIDStr := strconv.FormatInt(collectionID, 10)
	partitionIDStr := strconv.FormatInt(partitionID, 10)
	segIDStr := strconv.FormatInt(segmentID, 10)
	keyPrefix := path.Join("query-node-seg-manager-test-minio-prefix", collIDStr, partitionIDStr, segIDStr)

	paths := make([]*internalPb.StringList, 0)
	fieldIDs := make([]int64, 0)
	fmt.Println(".. saving binlog to MinIO ...", len(binLogs))
	for _, blob := range binLogs {
		uid := rand.Int63n(100000000)
		key := path.Join(keyPrefix, blob.Key, strconv.FormatInt(uid, 10))
		err = kv.Save(key, string(blob.Value[:]))
		if err != nil {
			return nil, nil, err
		}
		paths = append(paths, &internalPb.StringList{
			Values: []string{key},
		})
		fieldID, err := strconv.Atoi(blob.Key)
		if err != nil {
			return nil, nil, err
		}
		fieldIDs = append(fieldIDs, int64(fieldID))
	}

	return paths, fieldIDs, nil
}

func generateIndex(segmentID UniqueID) ([]string, indexParam, error) {
	const (
		msgLength = 1000
		DIM       = 16
	)

	indexParams := make(map[string]string)
	indexParams["index_type"] = "IVF_PQ"
	indexParams["index_mode"] = "cpu"
	indexParams["dim"] = "16"
	indexParams["k"] = "10"
	indexParams["nlist"] = "100"
	indexParams["nprobe"] = "10"
	indexParams["m"] = "4"
	indexParams["nbits"] = "8"
	indexParams["metric_type"] = "L2"
	indexParams["SLICE_SIZE"] = "4"

	var indexParamsKV []*commonpb.KeyValuePair
	for key, value := range indexParams {
		indexParamsKV = append(indexParamsKV, &commonpb.KeyValuePair{
			Key:   key,
			Value: value,
		})
	}

	typeParams := make(map[string]string)
	typeParams["dim"] = strconv.Itoa(DIM)
	var indexRowData []float32
	for n := 0; n < msgLength; n++ {
		for i := 0; i < DIM; i++ {
			indexRowData = append(indexRowData, float32(n*i))
		}
	}

	index, err := indexnode.NewCIndex(typeParams, indexParams)
	if err != nil {
		return nil, nil, err
	}

	err = index.BuildFloatVecIndexWithoutIds(indexRowData)
	if err != nil {
		return nil, nil, err
	}

	option := &minioKV.Option{
		Address:           Params.MinioEndPoint,
		AccessKeyID:       Params.MinioAccessKeyID,
		SecretAccessKeyID: Params.MinioSecretAccessKey,
		UseSSL:            Params.MinioUseSSLStr,
		BucketName:        Params.MinioBucketName,
		CreateBucket:      true,
	}

	kv, err := minioKV.NewMinIOKV(context.Background(), option)
	if err != nil {
		return nil, nil, err
	}

	//save index to minio
	binarySet, err := index.Serialize()
	if err != nil {
		return nil, nil, err
	}

	indexPaths := make([]string, 0)
	for _, index := range binarySet {
		path := strconv.Itoa(int(segmentID)) + "/" + index.Key
		indexPaths = append(indexPaths, path)
		err := kv.Save(path, string(index.Value))
		if err != nil {
			return nil, nil, err
		}
	}

	return indexPaths, indexParams, nil
}

func TestSegmentManager_load_and_release(t *testing.T) {
	collectionID := UniqueID(0)
	partitionID := UniqueID(1)
	segmentID := UniqueID(2)
	fieldIDs := []int64{101}

	node := newQueryNodeMock()
	defer node.Stop()

	ctx := node.queryNodeLoopCtx
	node.loadIndexService = newLoadIndexService(ctx, node.replica)
	node.segManager = newSegmentManager(ctx, node.replica, node.loadIndexService.loadIndexReqChan)
	go node.loadIndexService.start()

	collectionName := "collection0"
	initTestMeta(t, node, collectionName, collectionID, 0)

	err := node.replica.addPartition(collectionID, partitionID)
	assert.NoError(t, err)

	err = node.replica.addSegment(segmentID, partitionID, collectionID, segTypeSealed)
	assert.NoError(t, err)

	paths, srcFieldIDs, err := generateInsertBinLog(collectionID, partitionID, segmentID)
	assert.NoError(t, err)

	fieldsMap := node.segManager.filterOutNeedlessFields(paths, srcFieldIDs, fieldIDs)
	assert.Equal(t, len(fieldsMap), 1)

	err = node.segManager.loadSegmentFieldsData(segmentID, fieldsMap)
	assert.NoError(t, err)

	indexPaths, indexParams, err := generateIndex(segmentID)
	assert.NoError(t, err)

	err = node.segManager.loadIndex(segmentID, indexPaths, indexParams)
	assert.NoError(t, err)

	<-ctx.Done()
}
