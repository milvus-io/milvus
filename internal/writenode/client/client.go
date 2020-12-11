package writerclient

import "github.com/zilliztech/milvus-distributed/internal/util/typeutil"

type UniqueID = typeutil.UniqueID

type Timestamp = typeutil.Timestamp

type Client struct {
}

type SegmentDescription struct {
	SegmentID UniqueID
	IsClosed  bool
	OpenTime  Timestamp
	CloseTime Timestamp
}

func (c *Client) FlushSegment(semgentID UniqueID) error {
	// push msg to pulsar channel
	return nil
}

func (c *Client) DescribeSegment(semgentID UniqueID) (*SegmentDescription, error) {
	// query etcd
	return &SegmentDescription{}, nil
}

func (c *Client) GetInsertBinlogPaths(semgentID UniqueID) (map[int32]string, error) {
	// query etcd
	return nil, nil
}
