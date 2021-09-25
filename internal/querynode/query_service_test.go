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

package querynode

import (
	"context"
	"encoding/binary"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
)

func loadFields(segment *Segment, DIM int, N int) error {
	// generate vector field
	vectorFieldID := int64(100)
	vectors := make([]float32, N*DIM)
	for i := 0; i < N*DIM; i++ {
		vectors[i] = rand.Float32()
	}

	// generate int field
	agesFieldID := int64(101)
	ages := make([]int32, N)
	for i := 0; i < N; i++ {
		ages[i] = int32(N)
	}

	err := segment.segmentLoadFieldData(vectorFieldID, N, vectors)
	if err != nil {
		return err
	}
	err = segment.segmentLoadFieldData(agesFieldID, N, ages)
	if err != nil {
		return err
	}
	rowIDs := ages
	err = segment.segmentLoadFieldData(rowIDFieldID, N, rowIDs)
	return err
}

func sendSearchRequest(ctx context.Context, DIM int) error {
	// init message stream
	msFactory, err := newMessageStreamFactory()
	if err != nil {
		return err
	}
	searchProducerChannels := []string{"test-query"}

	searchStream, _ := msFactory.NewMsgStream(ctx)
	searchStream.AsProducer(searchProducerChannels)
	searchStream.Start()

	// generate search rawData
	var vec = make([]float32, DIM)
	for i := 0; i < DIM; i++ {
		vec[i] = rand.Float32()
	}
	dslString := "{\"bool\": { \n\"vector\": {\n \"vec\": {\n \"metric_type\": \"L2\", \n \"params\": {\n \"nprobe\": 10 \n},\n \"query\": \"$0\",\"topk\": 10 \n } \n } \n } \n }"
	var searchRawData1 []byte
	var searchRawData2 []byte
	for i, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele+float32(i*2)))
		searchRawData1 = append(searchRawData1, buf...)
	}
	for i, ele := range vec {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele+float32(i*4)))
		searchRawData2 = append(searchRawData2, buf...)
	}

	// generate placeholder
	placeholderValue := milvuspb.PlaceholderValue{
		Tag:    "$0",
		Type:   milvuspb.PlaceholderType_FloatVector,
		Values: [][]byte{searchRawData1, searchRawData2},
	}
	placeholderGroup := milvuspb.PlaceholderGroup{
		Placeholders: []*milvuspb.PlaceholderValue{&placeholderValue},
	}
	placeGroupByte, err := proto.Marshal(&placeholderGroup)
	if err != nil {
		return err
	}

	// generate searchMsg
	searchMsg := &msgstream.SearchMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{0},
		},
		SearchRequest: internalpb.SearchRequest{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Search,
				MsgID:     1,
				Timestamp: Timestamp(10),
				SourceID:  1,
			},
			ResultChannelID:  "0",
			Dsl:              dslString,
			PlaceholderGroup: placeGroupByte,
			DslType:          commonpb.DslType_Dsl,
		},
	}
	msgPackSearch := msgstream.MsgPack{}
	msgPackSearch.Msgs = append(msgPackSearch.Msgs, searchMsg)

	// produce search message
	err = searchStream.Produce(&msgPackSearch)
	return err
}

func TestSearch_Search(t *testing.T) {
	const N = 10000
	const DIM = 16

	// init queryNode
	collectionID := UniqueID(0)
	segmentID := UniqueID(1)
	node := newQueryNodeMock()
	initTestMeta(t, node, collectionID, UniqueID(0))

	msFactory, err := newMessageStreamFactory()
	assert.NoError(t, err)

	// start search service
	node.queryService = newQueryService(node.queryNodeLoopCtx,
		node.historical,
		node.streaming,
		msFactory)

	// load segment
	err = node.historical.replica.addSegment(segmentID, defaultPartitionID, collectionID, "", segmentTypeSealed, true)
	assert.NoError(t, err)
	segment, err := node.historical.replica.getSegmentByID(segmentID)
	assert.NoError(t, err)
	err = loadFields(segment, DIM, N)
	assert.NoError(t, err)

	err = node.queryService.addQueryCollection(collectionID)
	assert.Error(t, err)

	err = sendSearchRequest(node.queryNodeLoopCtx, DIM)
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	err = node.Stop()
	assert.NoError(t, err)
}

func TestSearch_SearchMultiSegments(t *testing.T) {
	const N = 10000
	const DIM = 16

	// init queryNode
	collectionID := UniqueID(0)
	segmentID1 := UniqueID(1)
	segmentID2 := UniqueID(2)
	node := newQueryNodeMock()
	initTestMeta(t, node, collectionID, UniqueID(0))

	msFactory, err := newMessageStreamFactory()
	assert.NoError(t, err)

	// start search service
	node.queryService = newQueryService(node.queryNodeLoopCtx,
		node.historical,
		node.streaming,
		msFactory)
	err = node.queryService.addQueryCollection(collectionID)
	assert.Error(t, err)

	// load segments
	err = node.historical.replica.addSegment(segmentID1, defaultPartitionID, collectionID, "", segmentTypeSealed, true)
	assert.NoError(t, err)
	segment1, err := node.historical.replica.getSegmentByID(segmentID1)
	assert.NoError(t, err)
	err = loadFields(segment1, DIM, N)
	assert.NoError(t, err)

	err = node.historical.replica.addSegment(segmentID2, defaultPartitionID, collectionID, "", segmentTypeSealed, true)
	assert.NoError(t, err)
	segment2, err := node.historical.replica.getSegmentByID(segmentID2)
	assert.NoError(t, err)
	err = loadFields(segment2, DIM, N)
	assert.NoError(t, err)

	err = sendSearchRequest(node.queryNodeLoopCtx, DIM)
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)

	err = node.Stop()
	assert.NoError(t, err)
}

func TestQueryService_addQueryCollection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	his, err := genSimpleHistorical(ctx)
	assert.NoError(t, err)

	str, err := genSimpleStreaming(ctx)
	assert.NoError(t, err)

	fac, err := genFactory()
	assert.NoError(t, err)

	// start search service
	qs := newQueryService(ctx, his, str, fac)
	assert.NotNil(t, qs)

	err = qs.addQueryCollection(defaultCollectionID)
	assert.NoError(t, err)
	assert.Len(t, qs.queryCollections, 1)

	err = qs.addQueryCollection(defaultCollectionID)
	assert.Error(t, err)
	assert.Len(t, qs.queryCollections, 1)

	const invalidCollectionID = 10000
	err = qs.addQueryCollection(invalidCollectionID)
	assert.Error(t, err)
	assert.Len(t, qs.queryCollections, 1)
}
