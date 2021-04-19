package datanode

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/msgstream/pulsarms"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
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

	msFactory := pulsarms.NewFactory()
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
			ChannelName: "aaa",
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
