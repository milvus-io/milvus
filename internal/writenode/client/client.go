package writerclient

import (
	"strconv"

	"github.com/golang/protobuf/proto"
	"go.etcd.io/etcd/clientv3"

	"github.com/zilliztech/milvus-distributed/internal/kv"
	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
	"github.com/zilliztech/milvus-distributed/internal/msgstream"
	internalPb "github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	pb "github.com/zilliztech/milvus-distributed/internal/proto/writerpb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type UniqueID = typeutil.UniqueID

type Timestamp = typeutil.Timestamp

type Client struct {
	kvClient kv.TxnBase // client of a reliable kv service, i.e. etcd client
	kvPrefix string

	flushStream msgstream.MsgStream
}

func NewWriterClient(etcdAddress string, kvRootPath string, writeNodeKvSubPath string, flushStream msgstream.MsgStream) (*Client, error) {
	// init kv client
	etcdClient, err := clientv3.New(clientv3.Config{Endpoints: []string{etcdAddress}})
	if err != nil {
		return nil, err
	}
	kvClient := etcdkv.NewEtcdKV(etcdClient, kvRootPath)

	return &Client{
		kvClient:    kvClient,
		kvPrefix:    writeNodeKvSubPath,
		flushStream: flushStream,
	}, nil
}

type SegmentDescription struct {
	SegmentID UniqueID
	IsClosed  bool
	OpenTime  Timestamp
	CloseTime Timestamp
}

func (c *Client) FlushSegment(segmentID UniqueID) error {
	baseMsg := msgstream.BaseMsg{
		BeginTimestamp: 0,
		EndTimestamp:   0,
		HashValues:     []uint32{0},
	}

	flushMsg := internalPb.FlushMsg{
		MsgType:   internalPb.MsgType_kFlush,
		SegmentID: segmentID,
		Timestamp: Timestamp(0),
	}

	fMsg := &msgstream.FlushMsg{
		BaseMsg:  baseMsg,
		FlushMsg: flushMsg,
	}
	msgPack := msgstream.MsgPack{}
	msgPack.Msgs = append(msgPack.Msgs, fMsg)

	err := c.flushStream.Produce(&msgPack)
	return err
}

func (c *Client) DescribeSegment(segmentID UniqueID) (*SegmentDescription, error) {
	// query etcd
	ret := &SegmentDescription{
		SegmentID: segmentID,
		IsClosed:  false,
	}

	key := c.kvPrefix + strconv.FormatInt(segmentID, 10)
	value, err := c.kvClient.Load(key)
	if err != nil {
		return ret, err
	}

	flushMeta := pb.SegmentFlushMeta{}
	err = proto.UnmarshalText(value, &flushMeta)
	if err != nil {
		return ret, err
	}
	ret.IsClosed = flushMeta.IsClosed
	ret.OpenTime = flushMeta.OpenTime
	ret.CloseTime = flushMeta.CloseTime
	return ret, nil
}

func (c *Client) GetInsertBinlogPaths(segmentID UniqueID) (map[int64][]string, error) {
	key := c.kvPrefix + strconv.FormatInt(segmentID, 10)

	value, err := c.kvClient.Load(key)
	if err != nil {
		return nil, err
	}

	flushMeta := pb.SegmentFlushMeta{}
	err = proto.UnmarshalText(value, &flushMeta)
	if err != nil {
		return nil, err
	}
	ret := make(map[int64][]string)
	for _, field := range flushMeta.Fields {
		ret[field.FieldID] = field.BinlogPaths
	}
	return ret, nil
}
