package proxynode

import (
	"bytes"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cast"

	"github.com/spf13/viper"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"

	"github.com/zilliztech/milvus-distributed/internal/util/paramtable"
)

const (
	StartParamsKey = "START_PARAMS"
)

type ParamTable struct {
	paramtable.BaseTable

	NetworkPort    int
	IP             string
	NetworkAddress string

	MasterAddress string
	PulsarAddress string

	QueryNodeNum                       int
	QueryNodeIDList                    []UniqueID
	ProxyID                            UniqueID
	TimeTickInterval                   time.Duration
	InsertChannelNames                 []string
	DeleteChannelNames                 []string
	K2SChannelNames                    []string
	SearchChannelNames                 []string
	SearchResultChannelNames           []string
	ProxySubName                       string
	ProxyTimeTickChannelNames          []string
	DataDefinitionChannelNames         []string
	MsgStreamInsertBufSize             int64
	MsgStreamSearchBufSize             int64
	MsgStreamSearchResultBufSize       int64
	MsgStreamSearchResultPulsarBufSize int64
	MsgStreamTimeTickBufSize           int64
	MaxNameLength                      int64
	MaxFieldNum                        int64
	MaxDimension                       int64
	DefaultPartitionTag                string
	DefaultIndexName                   string
}

var Params ParamTable
var once sync.Once

func (pt *ParamTable) LoadConfigFromInitParams(initParams *internalpb2.InitParams) error {
	pt.ProxyID = initParams.NodeID

	config := viper.New()
	config.SetConfigType("yaml")
	save := func() error {
		for _, key := range config.AllKeys() {
			val := config.Get(key)
			str, err := cast.ToStringE(val)
			if err != nil {
				switch val := val.(type) {
				case []interface{}:
					str = str[:0]
					for _, v := range val {
						ss, err := cast.ToStringE(v)
						if err != nil {
							log.Panic(err)
						}
						if len(str) == 0 {
							str = ss
						} else {
							str = str + "," + ss
						}
					}

				default:
					log.Panicf("undefine config type, key=%s", key)
				}
			}
			log.Println("key: ", key, ", value: ", str)
			err = pt.Save(key, str)
			if err != nil {
				panic(err)
			}
		}
		return nil
	}

	for _, pair := range initParams.StartParams {
		if strings.HasPrefix(pair.Key, StartParamsKey) {
			err := config.ReadConfig(bytes.NewBuffer([]byte(pair.Value)))
			if err != nil {
				return err
			}
			err = save()
			if err != nil {
				return err
			}
		}
	}

	pt.initParams()

	return nil
}

func (pt *ParamTable) Init() {
	once.Do(func() {
		pt.BaseTable.Init()
		// err := pt.LoadYaml("advanced/proxy_node.yaml")
		// if err != nil {
		// 	panic(err)
		// }
		// pt.initParams()
	})
}

func (pt *ParamTable) initParams() {
	pt.initPulsarAddress()
	pt.initQueryNodeIDList()
	pt.initQueryNodeNum()
	// pt.initProxyID()
	pt.initTimeTickInterval()
	pt.initInsertChannelNames()
	pt.initDeleteChannelNames()
	pt.initK2SChannelNames()
	pt.initSearchChannelNames()
	pt.initSearchResultChannelNames()
	pt.initProxySubName()
	pt.initProxyTimeTickChannelNames()
	pt.initDataDefinitionChannelNames()
	pt.initMsgStreamInsertBufSize()
	pt.initMsgStreamSearchBufSize()
	pt.initMsgStreamSearchResultBufSize()
	pt.initMsgStreamSearchResultPulsarBufSize()
	pt.initMsgStreamTimeTickBufSize()
	pt.initMaxNameLength()
	pt.initMaxFieldNum()
	pt.initMaxDimension()
	pt.initDefaultPartitionTag()
	pt.initDefaultIndexName()

}

func (pt *ParamTable) initPulsarAddress() {
	ret, err := pt.Load("_PulsarAddress")
	if err != nil {
		panic(err)
	}
	pt.PulsarAddress = ret
}

func (pt *ParamTable) initQueryNodeNum() {
	pt.QueryNodeNum = len(pt.QueryNodeIDList)
}

func (pt *ParamTable) initQueryNodeIDList() []UniqueID {
	queryNodeIDStr, err := pt.Load("nodeID.queryNodeIDList")
	if err != nil {
		panic(err)
	}
	var ret []UniqueID
	queryNodeIDs := strings.Split(queryNodeIDStr, ",")
	for _, i := range queryNodeIDs {
		v, err := strconv.Atoi(i)
		if err != nil {
			log.Panicf("load proxynode id list error, %s", err.Error())
		}
		ret = append(ret, UniqueID(v))
	}
	pt.QueryNodeIDList = ret
	return ret
}

func (pt *ParamTable) initProxyID() {
	proxyID, err := pt.Load("_proxyID")
	if err != nil {
		panic(err)
	}
	ID, err := strconv.Atoi(proxyID)
	if err != nil {
		panic(err)
	}
	pt.ProxyID = UniqueID(ID)
}

func (pt *ParamTable) initTimeTickInterval() {
	intervalStr, err := pt.Load("proxyNode.timeTickInterval")
	if err != nil {
		panic(err)
	}
	interval, err := strconv.Atoi(intervalStr)
	if err != nil {
		panic(err)
	}
	pt.TimeTickInterval = time.Duration(interval) * time.Millisecond
}

func (pt *ParamTable) initInsertChannelNames() {
	prefix, err := pt.Load("msgChannel.chanNamePrefix.insert")
	if err != nil {
		panic(err)
	}
	prefix += "-"
	iRangeStr, err := pt.Load("msgChannel.channelRange.insert")
	if err != nil {
		panic(err)
	}
	channelIDs := paramtable.ConvertRangeToIntSlice(iRangeStr, ",")
	var ret []string
	for _, ID := range channelIDs {
		ret = append(ret, prefix+strconv.Itoa(ID))
	}

	pt.InsertChannelNames = ret
}

func (pt *ParamTable) initDeleteChannelNames() {
	prefix, err := pt.Load("msgChannel.chanNamePrefix.delete")
	if err != nil {
		panic(err)
	}
	prefix += "-"
	dRangeStr, err := pt.Load("msgChannel.channelRange.delete")
	if err != nil {
		panic(err)
	}
	channelIDs := paramtable.ConvertRangeToIntSlice(dRangeStr, ",")
	var ret []string
	for _, ID := range channelIDs {
		ret = append(ret, prefix+strconv.Itoa(ID))
	}
	pt.DeleteChannelNames = ret
}

func (pt *ParamTable) initK2SChannelNames() {
	prefix, err := pt.Load("msgChannel.chanNamePrefix.k2s")
	if err != nil {
		panic(err)
	}
	prefix += "-"
	k2sRangeStr, err := pt.Load("msgChannel.channelRange.k2s")
	if err != nil {
		panic(err)
	}
	channelIDs := paramtable.ConvertRangeToIntSlice(k2sRangeStr, ",")
	var ret []string
	for _, ID := range channelIDs {
		ret = append(ret, prefix+strconv.Itoa(ID))
	}
	pt.K2SChannelNames = ret
}

func (pt *ParamTable) initSearchChannelNames() {
	prefix, err := pt.Load("msgChannel.chanNamePrefix.search")
	if err != nil {
		panic(err)
	}
	prefix += "-"
	sRangeStr, err := pt.Load("msgChannel.channelRange.search")
	if err != nil {
		panic(err)
	}
	channelIDs := paramtable.ConvertRangeToIntSlice(sRangeStr, ",")
	var ret []string
	for _, ID := range channelIDs {
		ret = append(ret, prefix+strconv.Itoa(ID))
	}
	pt.SearchChannelNames = ret
}

func (pt *ParamTable) initSearchResultChannelNames() {
	prefix, err := pt.Load("msgChannel.chanNamePrefix.searchResult")
	if err != nil {
		panic(err)
	}
	prefix += "-"
	sRangeStr, err := pt.Load("msgChannel.channelRange.searchResult")
	if err != nil {
		panic(err)
	}
	channelIDs := paramtable.ConvertRangeToIntSlice(sRangeStr, ",")
	var ret []string
	for _, ID := range channelIDs {
		ret = append(ret, prefix+strconv.Itoa(ID))
	}
	pt.SearchResultChannelNames = ret
}

func (pt *ParamTable) initProxySubName() {
	prefix, err := pt.Load("msgChannel.subNamePrefix.proxySubNamePrefix")
	if err != nil {
		panic(err)
	}
	pt.ProxySubName = prefix
	// proxyIDStr, err := pt.Load("_proxyID")
	// if err != nil {
	// 	panic(err)
	// }
	pt.ProxySubName = prefix + "-" + strconv.Itoa(int(pt.ProxyID))
}

func (pt *ParamTable) initProxyTimeTickChannelNames() {
	prefix, err := pt.Load("msgChannel.chanNamePrefix.proxyTimeTick")
	if err != nil {
		panic(err)
	}
	prefix += "-0"
	pt.ProxyTimeTickChannelNames = []string{prefix}
}

func (pt *ParamTable) initDataDefinitionChannelNames() {
	prefix, err := pt.Load("msgChannel.chanNamePrefix.dataDefinition")
	if err != nil {
		panic(err)
	}
	prefix += "-0"
	pt.DataDefinitionChannelNames = []string{prefix}
}

func (pt *ParamTable) initMsgStreamInsertBufSize() {
	pt.MsgStreamInsertBufSize = pt.ParseInt64("proxyNode.msgStream.insert.bufSize")
}

func (pt *ParamTable) initMsgStreamSearchBufSize() {
	pt.MsgStreamSearchBufSize = pt.ParseInt64("proxyNode.msgStream.search.bufSize")
}

func (pt *ParamTable) initMsgStreamSearchResultBufSize() {
	pt.MsgStreamSearchResultBufSize = pt.ParseInt64("proxyNode.msgStream.searchResult.recvBufSize")
}

func (pt *ParamTable) initMsgStreamSearchResultPulsarBufSize() {
	pt.MsgStreamSearchResultPulsarBufSize = pt.ParseInt64("proxyNode.msgStream.searchResult.pulsarBufSize")
}

func (pt *ParamTable) initMsgStreamTimeTickBufSize() {
	pt.MsgStreamTimeTickBufSize = pt.ParseInt64("proxyNode.msgStream.timeTick.bufSize")
}

func (pt *ParamTable) initMaxNameLength() {
	str, err := pt.Load("proxyNode.maxNameLength")
	if err != nil {
		panic(err)
	}
	maxNameLength, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		panic(err)
	}
	pt.MaxNameLength = maxNameLength
}

func (pt *ParamTable) initMaxFieldNum() {
	str, err := pt.Load("proxyNode.maxFieldNum")
	if err != nil {
		panic(err)
	}
	maxFieldNum, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		panic(err)
	}
	pt.MaxFieldNum = maxFieldNum
}

func (pt *ParamTable) initMaxDimension() {
	str, err := pt.Load("proxyNode.maxDimension")
	if err != nil {
		panic(err)
	}
	maxDimension, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		panic(err)
	}
	pt.MaxDimension = maxDimension
}

func (pt *ParamTable) initDefaultPartitionTag() {
	tag, err := pt.Load("common.defaultPartitionTag")
	if err != nil {
		panic(err)
	}
	pt.DefaultPartitionTag = tag
}

func (pt *ParamTable) initDefaultIndexName() {
	name, err := pt.Load("common.defaultIndexName")
	if err != nil {
		panic(err)
	}
	pt.DefaultIndexName = name
}
