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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

func TestQueryNodeFlowGraph_consumerFlowGraph(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tSafe := newTSafeReplica()

	streamingReplica, err := genSimpleReplica()
	assert.NoError(t, err)

	fac, err := genFactory()
	assert.NoError(t, err)

	fg := newQueryNodeFlowGraph(ctx,
		loadTypeCollection,
		defaultCollectionID,
		defaultPartitionID,
		streamingReplica,
		tSafe,
		defaultVChannel,
		fac)

	err = fg.consumerFlowGraph(defaultVChannel, defaultSubName)
	assert.NoError(t, err)

	fg.close()
}

func TestQueryNodeFlowGraph_seekQueryNodeFlowGraph(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamingReplica, err := genSimpleReplica()
	assert.NoError(t, err)

	fac, err := genFactory()
	assert.NoError(t, err)

	tSafe := newTSafeReplica()

	fg := newQueryNodeFlowGraph(ctx,
		loadTypeCollection,
		defaultCollectionID,
		defaultPartitionID,
		streamingReplica,
		tSafe,
		defaultVChannel,
		fac)

	position := &internalpb.MsgPosition{
		ChannelName: defaultVChannel,
		MsgID:       []byte{},
		MsgGroup:    defaultSubName,
		Timestamp:   0,
	}
	err = fg.seekQueryNodeFlowGraph(position)
	assert.Error(t, err)

	fg.close()
}

func prepareToRead(ctx context.Context) (*internalpb.MsgPosition, error) {
	channel := defaultVChannel + funcutil.RandomString(8)
	msgPack := &msgstream.MsgPack{}
	inputStream, err := genMsgStreamProducer(ctx, []string{channel})
	if err != nil {
		return nil, err
	}
	defer inputStream.Close()

	for i := 0; i < defaultMsgLength; i++ {
		insertMsg, err := genSimpleInsertMsg()
		if err != nil {
			return nil, err
		}
		msgPack.Msgs = append(msgPack.Msgs, insertMsg)
	}

	err = inputStream.Produce(msgPack)
	if err != nil {
		return nil, err
	}

	readStream, err := getMsgStreamReader(ctx, []string{channel}, defaultSubName+"-0")
	if err != nil {
		return nil, err
	}
	defer readStream.Close()
	var seekPosition *internalpb.MsgPosition
	for i := 0; i < defaultMsgLength; i++ {
		hasNext := readStream.HasNext(channel)
		if !hasNext {
			return nil, errors.New("has next failed when read from msgStream")
		}
		result, err := readStream.Next(ctx, channel)
		if err != nil {
			return nil, err
		}
		if i == defaultMsgLength/2 {
			seekPosition = result.Position()
		}
	}
	return seekPosition, nil
}

func TestQueryNodeFlowGraph_readFlowGraph(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	position, err := prepareToRead(ctx)
	assert.NoError(t, err)

	streamingReplica, err := genSimpleReplica()
	assert.NoError(t, err)

	streamingReplica.addExcludedSegments(defaultCollectionID, nil)

	fac, err := genFactory()
	assert.NoError(t, err)

	err = readFlowGraph(ctx, defaultCollectionID, streamingReplica, position, fac)
	assert.NoError(t, err)

	seg, err := streamingReplica.getSegmentByID(defaultSegmentID)
	assert.NoError(t, err)

	rowCount := seg.getRowCount()
	expectedRowCount := defaultMsgLength*defaultMsgLength - defaultMsgLength/2 // totalInsert - readerPosition
	assert.Equal(t, expectedRowCount, int(rowCount))
}
