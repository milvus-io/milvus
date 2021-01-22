package datanode

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
)

type Interface interface {
	Init() error
	Start() error
	Stop() error

	WatchDmChannels(in *datapb.WatchDmChannelRequest) error
	FlushSegments(req *datapb.FlushSegRequest) error
}
