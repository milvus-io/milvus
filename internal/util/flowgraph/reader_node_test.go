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

package flowgraph

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"github.com/milvus-io/milvus/internal/util/paramtable"
)

const (
	msgLength    = 10
	readPosition = 5
)

var Params paramtable.BaseTable
var testChannelName = funcutil.RandomString(8)

func TestMain(m *testing.M) {
	Params.Init()
	exitCode := m.Run()
	os.Exit(exitCode)
}

func genFactory() (msgstream.Factory, error) {
	const receiveBufSize = 1024
	pulsarAddress, _ := Params.Load("_PulsarAddress")

	msFactory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"receiveBufSize": receiveBufSize,
		"pulsarAddress":  pulsarAddress,
		"pulsarBufSize":  1024}
	err := msFactory.SetParams(m)
	if err != nil {
		return nil, err
	}
	return msFactory, nil
}

func getProducer(ctx context.Context, producerChannels []string) (msgstream.MsgStream, error) {
	msFactory, err := genFactory()
	if err != nil {
		return nil, err
	}

	stream, err := msFactory.NewMsgStream(ctx)
	if err != nil {
		return nil, err
	}
	stream.Start()
	stream.AsProducer(producerChannels)
	return stream, nil
}

func getReader(ctx context.Context, consumerChannels []string, subName string) (msgstream.MsgStream, error) {
	msFactory, err := genFactory()
	if err != nil {
		return nil, err
	}

	stream, err := msFactory.NewMsgStream(ctx)
	if err != nil {
		return nil, err
	}
	stream.Start()
	stream.AsReader(consumerChannels, subName)
	return stream, nil
}

func getInsertMsg(reqID int64) msgstream.TsMsg {
	hashValue := uint32(reqID)
	time := uint64(reqID)
	baseMsg := msgstream.BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{hashValue},
	}

	insertRequest := internalpb.InsertRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_Insert,
			MsgID:     reqID,
			Timestamp: time,
			SourceID:  reqID,
		},
		CollectionName: "Collection",
		PartitionName:  "Partition",
		SegmentID:      1,
		ShardName:      "0",
		Timestamps:     []Timestamp{time},
		RowIDs:         []int64{1},
		RowData:        []*commonpb.Blob{{}},
	}
	insertMsg := &msgstream.InsertMsg{
		BaseMsg:       baseMsg,
		InsertRequest: insertRequest,
	}
	return insertMsg
}

func prepareToRead(ctx context.Context) (*internalpb.MsgPosition, error) {
	msgPack := &msgstream.MsgPack{}
	inputStream, err := getProducer(ctx, []string{testChannelName})
	if err != nil {
		return nil, err
	}
	defer inputStream.Close()

	for i := 0; i < msgLength; i++ {
		insertMsg := getInsertMsg(int64(i))
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}

	err = inputStream.Produce(msgPack)
	if err != nil {
		return nil, err
	}

	readStream, err := getReader(ctx, []string{testChannelName}, "ut-sub-name-0")
	if err != nil {
		return nil, err
	}
	defer readStream.Close()
	var seekPosition *internalpb.MsgPosition
	for i := 0; i < msgLength; i++ {
		hasNext := readStream.HasNext(testChannelName)
		if !hasNext {
			return nil, errors.New("has next failed")
		}
		result, err := readStream.Next(ctx, testChannelName)
		if err != nil {
			return nil, err
		}
		if i == readPosition {
			seekPosition = result.Position()
		}
	}
	return seekPosition, nil
}

func TestReaderNode(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	position, err := prepareToRead(ctx)
	assert.NoError(t, err)

	readStream, err := getReader(ctx, []string{testChannelName}, "ut-sub-name-1")
	assert.NoError(t, err)
	defer readStream.Close()

	err = readStream.SeekReaders([]*internalpb.MsgPosition{position})
	assert.NoError(t, err)

	nodeName := "readerNode"
	readerNode := NewReaderNode(ctx, readStream, testChannelName, nodeName, 1024, 1024)
	assert.NotNil(t, readerNode)

	isInputNode := readerNode.IsInputNode()
	assert.True(t, isInputNode)

	name := readerNode.Name()
	assert.Equal(t, name, nodeName)

	res := readerNode.Operate([]Msg{})
	assert.Len(t, res, 1)

	resMsg, ok := res[0].(*MsgStreamMsg)
	assert.True(t, ok)
	assert.Len(t, resMsg.tsMessages, msgLength-readPosition)
}
