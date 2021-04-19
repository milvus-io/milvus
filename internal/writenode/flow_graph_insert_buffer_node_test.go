package writenode

import (
	"context"
	"encoding/binary"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"

	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/util/flowgraph"
)

func TestFlowGraphInputBufferNode_Operate(t *testing.T) {
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

	ddChan := make(chan *ddlFlushSyncMsg, 10)
	defer close(ddChan)
	insertChan := make(chan *insertFlushSyncMsg, 10)
	defer close(insertChan)

	testPath := "/test/writenode/root/meta"
	err := clearEtcd(testPath)
	require.NoError(t, err)
	Params.MetaRootPath = testPath
	fService := newFlushSyncService(ctx, ddChan, insertChan)
	assert.Equal(t, testPath, fService.metaTable.client.(*etcdkv.EtcdKV).GetPath("."))
	go fService.start()

	// Params.FlushInsertBufSize = 2
	iBNode := newInsertBufferNode(ctx, insertChan)

	newMeta()
	inMsg := genInsertMsg()
	var iMsg flowgraph.Msg = &inMsg
	iBNode.Operate([]*flowgraph.Msg{&iMsg})
}

func genInsertMsg() insertMsg {
	// test data generate
	// GOOSE TODO orgnize
	const DIM = 2
	const N = 1
	var rawData []byte

	// Float vector
	var fvector = [DIM]float32{1, 2}
	for _, ele := range fvector {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, math.Float32bits(ele))
		rawData = append(rawData, buf...)
	}

	// Binary vector
	// Dimension of binary vector is 32
	var bvector = [4]byte{255, 255, 255, 0}
	for _, ele := range bvector {
		bs := make([]byte, 4)
		binary.LittleEndian.PutUint32(bs, uint32(ele))
		rawData = append(rawData, bs...)
	}

	// Bool
	bb := make([]byte, 4)
	var fieldBool = true
	var fieldBoolInt uint32
	if fieldBool {
		fieldBoolInt = 1
	} else {
		fieldBoolInt = 0
	}

	binary.LittleEndian.PutUint32(bb, fieldBoolInt)
	rawData = append(rawData, bb...)

	// int8
	var dataInt8 int8 = 100
	bint8 := make([]byte, 4)
	binary.LittleEndian.PutUint32(bint8, uint32(dataInt8))
	rawData = append(rawData, bint8...)

	// int16
	var dataInt16 int16 = 200
	bint16 := make([]byte, 4)
	binary.LittleEndian.PutUint32(bint16, uint32(dataInt16))
	rawData = append(rawData, bint16...)

	// int32
	var dataInt32 int32 = 300
	bint32 := make([]byte, 4)
	binary.LittleEndian.PutUint32(bint32, uint32(dataInt32))
	rawData = append(rawData, bint32...)

	// int64
	var dataInt64 int64 = 300
	bint64 := make([]byte, 4)
	binary.LittleEndian.PutUint32(bint64, uint32(dataInt64))
	rawData = append(rawData, bint64...)

	// float32
	var datafloat float32 = 1.1
	bfloat32 := make([]byte, 4)
	binary.LittleEndian.PutUint32(bfloat32, math.Float32bits(datafloat))
	rawData = append(rawData, bfloat32...)

	// float64
	var datafloat64 float64 = 2.2
	bfloat64 := make([]byte, 8)
	binary.LittleEndian.PutUint64(bfloat64, math.Float64bits(datafloat64))
	rawData = append(rawData, bfloat64...)

	timeRange := TimeRange{
		timestampMin: 0,
		timestampMax: math.MaxUint64,
	}

	var iMsg = &insertMsg{
		insertMessages: make([]*msgstream.InsertMsg, 0),
		flushMessages:  make([]*msgstream.FlushMsg, 0),
		timeRange: TimeRange{
			timestampMin: timeRange.timestampMin,
			timestampMax: timeRange.timestampMax,
		},
	}

	// messages generate
	const MSGLENGTH = 1
	// insertMessages := make([]msgstream.TsMsg, 0)
	for i := 0; i < MSGLENGTH; i++ {
		var msg = &msgstream.InsertMsg{
			BaseMsg: msgstream.BaseMsg{
				HashValues: []uint32{
					uint32(i),
				},
			},
			InsertRequest: internalpb.InsertRequest{
				MsgType:        internalpb.MsgType_kInsert,
				ReqID:          UniqueID(0),
				CollectionName: "coll1",
				PartitionTag:   "default",
				SegmentID:      UniqueID(1),
				ChannelID:      UniqueID(0),
				ProxyID:        UniqueID(0),
				Timestamps:     []Timestamp{Timestamp(i + 1000)},
				RowIDs:         []UniqueID{UniqueID(i)},

				RowData: []*commonpb.Blob{
					{Value: rawData},
				},
			},
		}
		iMsg.insertMessages = append(iMsg.insertMessages, msg)
	}

	var fmsg msgstream.FlushMsg = msgstream.FlushMsg{
		BaseMsg: msgstream.BaseMsg{
			HashValues: []uint32{
				uint32(10),
			},
		},
		FlushMsg: internalpb.FlushMsg{
			MsgType:   internalpb.MsgType_kFlush,
			SegmentID: UniqueID(1),
			Timestamp: Timestamp(2000),
		},
	}
	iMsg.flushMessages = append(iMsg.flushMessages, &fmsg)
	return *iMsg

}
