package masterservice

import (
	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

var Params ParamTable

type ParamTable struct {
	paramtable.BaseTable

	Address string
	Port    int
	NodeID  uint64

	PulsarAddress        string
	EtcdAddress          string
	MetaRootPath         string
	KvRootPath           string
	ProxyTimeTickChannel string
	MsgChannelSubName    string
	TimeTickChannel      string
	DdChannel            string
	StatisticsChannel    string

	MaxPartitionNum     int64
	DefaultPartitionTag string
}
