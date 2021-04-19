package writenode

import (
	"bytes"
	"context"
	"encoding/binary"
	"log"
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
	log.Println(len(rawData))

	// Binary vector
	// Dimension of binary vector is 32
	// size := 4,  = 32 / 8
	var bvector = []byte{255, 255, 255, 0}
	rawData = append(rawData, bvector...)
	log.Println(len(rawData))

	// Bool
	var fieldBool = true
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.LittleEndian, fieldBool); err != nil {
		panic(err)
	}

	rawData = append(rawData, buf.Bytes()...)
	log.Println(len(rawData))

	// int8
	var dataInt8 int8 = 100
	bint8 := new(bytes.Buffer)
	if err := binary.Write(bint8, binary.LittleEndian, dataInt8); err != nil {
		panic(err)
	}
	rawData = append(rawData, bint8.Bytes()...)
	log.Println(len(rawData))

	// int16
	var dataInt16 int16 = 200
	bint16 := new(bytes.Buffer)
	if err := binary.Write(bint16, binary.LittleEndian, dataInt16); err != nil {
		panic(err)
	}
	rawData = append(rawData, bint16.Bytes()...)
	log.Println(len(rawData))

	// int32
	var dataInt32 int32 = 300
	bint32 := new(bytes.Buffer)
	if err := binary.Write(bint32, binary.LittleEndian, dataInt32); err != nil {
		panic(err)
	}
	rawData = append(rawData, bint32.Bytes()...)
	log.Println(len(rawData))

	// int64
	var dataInt64 int64 = 300
	bint64 := new(bytes.Buffer)
	if err := binary.Write(bint64, binary.LittleEndian, dataInt64); err != nil {
		panic(err)
	}
	rawData = append(rawData, bint64.Bytes()...)
	log.Println(len(rawData))

	// float32
	var datafloat float32 = 1.1
	bfloat32 := new(bytes.Buffer)
	if err := binary.Write(bfloat32, binary.LittleEndian, datafloat); err != nil {
		panic(err)
	}
	rawData = append(rawData, bfloat32.Bytes()...)
	log.Println(len(rawData))

	// float64
	var datafloat64 float64 = 2.2
	bfloat64 := new(bytes.Buffer)
	if err := binary.Write(bfloat64, binary.LittleEndian, datafloat64); err != nil {
		panic(err)
	}
	rawData = append(rawData, bfloat64.Bytes()...)
	log.Println(len(rawData))

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
