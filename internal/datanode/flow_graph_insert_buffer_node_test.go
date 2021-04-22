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

package datanode

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/msgstream"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/util/flowgraph"
	"github.com/stretchr/testify/assert"
)

func TestFlowGraphInsertBufferNode_Operate(t *testing.T) {
	const ctxTimeInMillisecond = 2000
	const closeWithDeadline = false
	var ctx context.Context

	if closeWithDeadline {
		var cancel context.CancelFunc
		d := time.Now().Add(ctxTimeInMillisecond * time.Millisecond)
		ctx, cancel = context.WithDeadline(context.Background(), d)
		defer cancel()
	} else {
		ctx = context.Background()
	}

	testPath := "/test/datanode/root/meta"
	err := clearEtcd(testPath)
	require.NoError(t, err)
	Params.MetaRootPath = testPath

	Factory := &MetaFactory{}
	collMeta := Factory.CollectionMetaFactory(UniqueID(0), "coll1")

	replica := newReplica()
	err = replica.addCollection(collMeta.ID, collMeta.Schema)
	require.NoError(t, err)
	err = replica.addSegment(1, collMeta.ID, 0, Params.InsertChannelNames[0])
	require.NoError(t, err)

	msFactory := msgstream.NewPmsFactory()
	m := map[string]interface{}{
		"receiveBufSize": 1024,
		"pulsarAddress":  Params.PulsarAddress,
		"pulsarBufSize":  1024}
	err = msFactory.SetParams(m)
	assert.Nil(t, err)

	iBNode := newInsertBufferNode(ctx, newBinlogMeta(), replica, msFactory)
	inMsg := genInsertMsg()
	var iMsg flowgraph.Msg = &inMsg
	iBNode.Operate([]flowgraph.Msg{iMsg})
}

func genInsertMsg() insertMsg {

	timeRange := TimeRange{
		timestampMin: 0,
		timestampMax: math.MaxUint64,
	}

	startPos := []*internalpb.MsgPosition{
		{
			ChannelName: Params.InsertChannelNames[0],
			MsgID:       make([]byte, 0),
			Timestamp:   0,
		},
	}

	var iMsg = &insertMsg{
		insertMessages: make([]*msgstream.InsertMsg, 0),
		flushMessages:  make([]*flushMsg, 0),
		timeRange: TimeRange{
			timestampMin: timeRange.timestampMin,
			timestampMax: timeRange.timestampMax,
		},
		startPositions: startPos,
		endPositions:   startPos,
	}

	dataFactory := NewDataFactory()
	iMsg.insertMessages = append(iMsg.insertMessages, dataFactory.GetMsgStreamInsertMsgs(2)...)

	fmsg := &flushMsg{
		msgID:        1,
		timestamp:    2000,
		segmentIDs:   []UniqueID{1},
		collectionID: UniqueID(1),
	}

	iMsg.flushMessages = append(iMsg.flushMessages, fmsg)
	return *iMsg

}
