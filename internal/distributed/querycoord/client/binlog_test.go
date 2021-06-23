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

package grpcquerycoordclient

//import (
//	"context"
//	"encoding/binary"
//	"math"
//	"path"
//	"strconv"
//	"testing"
//	"time"
//
//	"github.com/stretchr/testify/assert"
//
//	"github.com/milvus-io/milvus/internal/indexnode"
//	minioKV "github.com/milvus-io/milvus/internal/kv/minio"
//	"github.com/milvus-io/milvus/internal/msgstream"
//	"github.com/milvus-io/milvus/internal/msgstream/pulsarms"
//	"github.com/milvus-io/milvus/internal/proto/commonpb"
//	"github.com/milvus-io/milvus/internal/proto/etcdpb"
//	"github.com/milvus-io/milvus/internal/proto/internalpb"
//	"github.com/milvus-io/milvus/internal/proto/schemapb"
//	"github.com/milvus-io/milvus/internal/storage"
//	"github.com/milvus-io/milvus/internal/util/typeutil"
//)
//
////generate insert data
//const msgLength = 100
//const receiveBufSize = 1024
//const pulsarBufSize = 1024
//const DIM = 16
//
//type UniqueID = typeutil.UniqueID
//
//func genInsert(collectionID int64,
//	partitionID int64,
//	timeStart int,
//	numDmChannels int,
//	binlog bool) (*msgstream.MsgPack, *msgstream.MsgPack) {
//	msgs := make([]msgstream.TsMsg, 0)
//	for n := timeStart; n < timeStart+msgLength; n++ {
//		rowData := make([]byte, 0)
//		if binlog {
//			id := make([]byte, 8)
//			binary.BigEndian.PutUint64(id, uint64(n))
//			rowData = append(rowData, id...)
//			time := make([]byte, 8)
//			binary.BigEndian.PutUint64(time, uint64(n))
//			rowData = append(rowData, time...)
//		}
//		for i := 0; i < DIM; i++ {
//			vec := make([]byte, 4)
//			binary.BigEndian.PutUint32(vec, math.Float32bits(float32(n*i)))
//			rowData = append(rowData, vec...)
//		}
//		age := make([]byte, 4)
//		binary.BigEndian.PutUint32(age, 1)
//		rowData = append(rowData, age...)
//		blob := &commonpb.Blob{
//			Value: rowData,
//		}
//
//		var insertMsg msgstream.TsMsg = &msgstream.InsertMsg{
//			BaseMsg: msgstream.BaseMsg{
//				HashValues: []uint32{uint32((n - 1) % numDmChannels)},
//			},
//			InsertRequest: internalpb.InsertRequest{
//				Base: &commonpb.MsgBase{
//					MsgType:   commonpb.MsgType_kInsert,
//					MsgID:     0,
//					Timestamp: uint64(n),
//					SourceID:  0,
//				},
//				CollectionID: collectionID,
//				PartitionID:  partitionID,
//				SegmentID:    UniqueID(((n - 1) % numDmChannels) + ((n-1)/(numDmChannels*msgLength))*numDmChannels),
//				ChannelID:    "0",
//				Timestamps:   []uint64{uint64(n)},
//				RowIDs:       []int64{int64(n)},
//				RowData:      []*commonpb.Blob{blob},
//			},
//		}
//		//fmt.Println("hash value = ", insertMsg.(*msgstream.InsertMsg).HashValues, "segmentID = ", insertMsg.(*msgstream.InsertMsg).SegmentID)
//		msgs = append(msgs, insertMsg)
//	}
//
//	insertMsgPack := &msgstream.MsgPack{
//		BeginTs: uint64(timeStart),
//		EndTs:   uint64(timeStart + msgLength),
//		Msgs:    msgs,
//	}
//
//	// generate timeTick
//	timeTickMsg := &msgstream.TimeTickMsg{
//		BaseMsg: msgstream.BaseMsg{
//			BeginTimestamp: 0,
//			EndTimestamp:   0,
//			HashValues:     []uint32{0},
//		},
//		TimeTickMsg: internalpb.TimeTickMsg{
//			Base: &commonpb.MsgBase{
//				MsgType:   commonpb.MsgType_kTimeTick,
//				MsgID:     0,
//				Timestamp: uint64(timeStart + msgLength),
//				SourceID:  0,
//			},
//		},
//	}
//	timeTickMsgPack := &msgstream.MsgPack{
//		Msgs: []msgstream.TsMsg{timeTickMsg},
//	}
//	return insertMsgPack, timeTickMsgPack
//}
//
//func genSchema(collectionID int64) *schemapb.CollectionSchema {
//	fieldID := schemapb.FieldSchema{
//		FieldID:      UniqueID(0),
//		Name:         "RowID",
//		IsPrimaryKey: false,
//		DataType:     schemapb.DataType_INT64,
//	}
//
//	fieldTime := schemapb.FieldSchema{
//		FieldID:      UniqueID(1),
//		Name:         "Timestamp",
//		IsPrimaryKey: false,
//		DataType:     schemapb.DataType_INT64,
//	}
//
//	fieldVec := schemapb.FieldSchema{
//		FieldID:      UniqueID(100),
//		Name:         "vec",
//		IsPrimaryKey: false,
//		DataType:     schemapb.DataType_VECTOR_FLOAT,
//		TypeParams: []*commonpb.KeyValuePair{
//			{
//				Key:   "dim",
//				Value: "16",
//			},
//		},
//		IndexParams: []*commonpb.KeyValuePair{
//			{
//				Key:   "metric_type",
//				Value: "L2",
//			},
//		},
//	}
//
//	fieldInt := schemapb.FieldSchema{
//		FieldID:      UniqueID(101),
//		Name:         "age",
//		IsPrimaryKey: false,
//		DataType:     schemapb.DataType_INT32,
//	}
//
//	return &schemapb.CollectionSchema{
//		Name:   "collection-" + strconv.FormatInt(collectionID, 10),
//		AutoID: true,
//		Fields: []*schemapb.FieldSchema{
//			&fieldID, &fieldTime, &fieldVec, &fieldInt,
//		},
//	}
//}
//
//func getMinioKV(ctx context.Context) (*minioKV.MinIOKV, error) {
//	minioAddress := "localhost:9000"
//	accessKeyID := "minioadmin"
//	secretAccessKey := "minioadmin"
//	useSSL := false
//	bucketName := "a-bucket"
//
//	option := &minioKV.Option{
//		Address:           minioAddress,
//		AccessKeyID:       accessKeyID,
//		SecretAccessKeyID: secretAccessKey,
//		UseSSL:            useSSL,
//		BucketName:        bucketName,
//		CreateBucket:      true,
//	}
//
//	return minioKV.NewMinIOKV(ctx, option)
//}
//
//func TestWriteBinLog(t *testing.T) {
//	const (
//		debug          = true
//		consumeSubName = "test-load-collection-sub-name"
//	)
//	var ctx context.Context
//	if debug {
//		ctx = context.Background()
//	} else {
//		var cancel context.CancelFunc
//		ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(2*time.Second))
//		defer cancel()
//	}
//
//	// produce msg
//	insertChannels := []string{"insert-0", "insert-1", "insert-2", "insert-3"}
//	pulsarAddress := "pulsar://127.0.0.1:6650"
//
//	factory := pulsarms.NewFactory(pulsarAddress, receiveBufSize, pulsarBufSize)
//
//	insertStream, _ := factory.NewTtMsgStream(ctx)
//	insertStream.AsProducer(insertChannels)
//	insertStream.AsConsumer(insertChannels, consumeSubName)
//	insertStream.Start()
//
//	for i := 0; i < 12; i++ {
//		insertMsgPack, timeTickMsgPack := genInsert(1, 1, i*msgLength+1, 4, true)
//		err := insertStream.Produce(insertMsgPack)
//		assert.NoError(t, err)
//		err = insertStream.Broadcast(timeTickMsgPack)
//		assert.NoError(t, err)
//	}
//
//	//consume msg
//	segmentData := make([]*storage.InsertData, 12)
//	idData := make([][]int64, 12)
//	timestamps := make([][]int64, 12)
//	fieldAgeData := make([][]int32, 12)
//	fieldVecData := make([][]float32, 12)
//	for i := 0; i < 12; i++ {
//		idData[i] = make([]int64, 0)
//		timestamps[i] = make([]int64, 0)
//		fieldAgeData[i] = make([]int32, 0)
//		fieldVecData[i] = make([]float32, 0)
//	}
//	for i := 0; i < 12; i++ {
//		msgPack := insertStream.Consume()
//
//		for n := 0; n < msgLength; n++ {
//			segmentID := msgPack.Msgs[n].(*msgstream.InsertMsg).SegmentID
//			blob := msgPack.Msgs[n].(*msgstream.InsertMsg).RowData[0].Value
//			id := binary.BigEndian.Uint64(blob[0:8])
//			idData[segmentID] = append(idData[segmentID], int64(id))
//			t := binary.BigEndian.Uint64(blob[8:16])
//			timestamps[segmentID] = append(timestamps[segmentID], int64(t))
//			for i := 0; i < DIM; i++ {
//				bits := binary.BigEndian.Uint32(blob[16+4*i : 16+4*(i+1)])
//				floatVec := math.Float32frombits(bits)
//				fieldVecData[segmentID] = append(fieldVecData[segmentID], floatVec)
//			}
//			ageValue := binary.BigEndian.Uint32(blob[80:84])
//			fieldAgeData[segmentID] = append(fieldAgeData[segmentID], int32(ageValue))
//		}
//	}
//	for i := 0; i < 12; i++ {
//		insertData := &storage.InsertData{
//			Data: map[int64]storage.FieldData{
//				0: &storage.Int64FieldData{
//					NumRows: msgLength,
//					Data:    idData[i],
//				},
//				1: &storage.Int64FieldData{
//					NumRows: msgLength,
//					Data:    timestamps[i],
//				},
//				100: &storage.FloatVectorFieldData{
//					NumRows: msgLength,
//					Data:    fieldVecData[i],
//					Dim:     DIM,
//				},
//				101: &storage.Int32FieldData{
//					NumRows: msgLength,
//					Data:    fieldAgeData[i],
//				},
//			},
//		}
//		segmentData[i] = insertData
//	}
//
//	//gen inCodec
//	collectionMeta := &etcdpb.CollectionMeta{
//		ID:           1,
//		Schema:       genSchema(1),
//		CreateTime:   0,
//		PartitionIDs: []int64{1},
//		SegmentIDs:   []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
//	}
//	inCodec := storage.NewInsertCodec(collectionMeta)
//	indexCodec := storage.NewIndexCodec()
//
//	// get minio client
//	kv, err := getMinioKV(context.Background())
//	assert.Nil(t, err)
//
//	// write binlog minio
//	collectionStr := strconv.FormatInt(1, 10)
//	for i := 0; i < 12; i++ {
//		binLogs, err := inCodec.Serialize(1, storage.UniqueID(i), segmentData[i])
//		assert.Nil(t, err)
//		assert.Equal(t, len(binLogs), 4)
//		keyPrefix := "distributed-query-test-binlog"
//		segmentStr := strconv.FormatInt(int64(i), 10)
//
//		for _, blob := range binLogs {
//			key := path.Join(keyPrefix, collectionStr, segmentStr, blob.Key)
//			err = kv.Save(key, string(blob.Value[:]))
//			assert.Nil(t, err)
//		}
//	}
//
//	// gen index build's indexParams
//	indexParams := make(map[string]string)
//	indexParams["index_type"] = "IVF_PQ"
//	indexParams["index_mode"] = "cpu"
//	indexParams["dim"] = "16"
//	indexParams["k"] = "10"
//	indexParams["nlist"] = "100"
//	indexParams["nprobe"] = "10"
//	indexParams["m"] = "4"
//	indexParams["nbits"] = "8"
//	indexParams["metric_type"] = "L2"
//	indexParams["SLICE_SIZE"] = "400"
//
//	var indexParamsKV []*commonpb.KeyValuePair
//	for key, value := range indexParams {
//		indexParamsKV = append(indexParamsKV, &commonpb.KeyValuePair{
//			Key:   key,
//			Value: value,
//		})
//	}
//
//	// generator index and  write index to minio
//	for i := 0; i < 12; i++ {
//		typeParams := make(map[string]string)
//		typeParams["dim"] = "16"
//		index, err := indexnode.NewCIndex(typeParams, indexParams)
//		assert.Nil(t, err)
//		err = index.BuildFloatVecIndexWithoutIds(fieldVecData[i])
//		assert.Equal(t, err, nil)
//		binarySet, err := index.Serialize()
//		assert.Equal(t, len(binarySet), 1)
//		assert.Nil(t, err)
//		codecIndex, err := indexCodec.Serialize(binarySet, indexParams, "test_index", UniqueID(i))
//		assert.Equal(t, len(codecIndex), 2)
//		assert.Nil(t, err)
//		keyPrefix := "distributed-query-test-index"
//		segmentStr := strconv.FormatInt(int64(i), 10)
//		key1 := path.Join(keyPrefix, collectionStr, segmentStr, "IVF")
//		key2 := path.Join(keyPrefix, collectionStr, segmentStr, "indexParams")
//		kv.Save(key1, string(codecIndex[0].Value))
//		kv.Save(key2, string(codecIndex[1].Value))
//	}
//}
