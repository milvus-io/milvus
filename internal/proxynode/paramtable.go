package proxynode

import (
	"bytes"
	"log"
	"strconv"
	"strings"
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
}

var Params ParamTable

func (pt *ParamTable) LoadConfigFromInitParams(initParams *internalpb2.InitParams) error {
	pt.ProxyID = initParams.NodeID

	config := viper.New()
	config.SetConfigType("yaml")
	for _, pair := range initParams.StartParams {
		if pair.Key == StartParamsKey {
			err := config.ReadConfig(bytes.NewBuffer([]byte(pair.Value)))
			if err != nil {
				return err
			}
			break
		}
	}

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
		err = pt.Save(key, str)
		if err != nil {
			panic(err)
		}

	}

	pt.initParams()
	//
	//pulsarPort := config.GetString(PulsarPort)
	//pulsarHost := config.GetString(PulsarHost)
	//pt.PulsarAddress = pulsarHost + ":" + pulsarPort
	//
	//
	//queryNodeIDList := config.GetString(QueryNodeIDList)
	//pt.QueryNodeIDList = nil
	//queryNodeIDs := strings.Split(queryNodeIDList, ",")
	//for _, queryNodeID := range queryNodeIDs {
	//	v, err := strconv.Atoi(queryNodeID)
	//	if err != nil {
	//		return err
	//	}
	//	pt.QueryNodeIDList = append(pt.QueryNodeIDList, typeutil.UniqueID(v))
	//}
	//pt.QueryNodeNum = len(pt.QueryNodeIDList)
	//
	//timeTickInterval := config.GetString(TimeTickInterval)
	//interval, err := strconv.Atoi(timeTickInterval)
	//if err != nil {
	//	return err
	//}
	//pt.TimeTickInterval = time.Duration(interval) * time.Millisecond
	//
	//subName := config.GetString(SubName)
	//pt.ProxySubName = subName
	//
	//timeTickChannelNames := config.GetString(TimeTickChannelNames)
	//pt.ProxyTimeTickChannelNames = []string{timeTickChannelNames}
	//
	//msgStreamInsertBufSizeStr := config.GetString(MsgStreamInsertBufSize)
	//msgStreamInsertBufSize, err := strconv.Atoi(msgStreamInsertBufSizeStr)
	//if err != nil {
	//	return err
	//}
	//pt.MsgStreamInsertBufSize = int64(msgStreamInsertBufSize)
	//
	//msgStreamSearchBufSizeStr := config.GetString(MsgStreamSearchBufSize)
	//msgStreamSearchBufSize, err := strconv.Atoi(msgStreamSearchBufSizeStr)
	//if err != nil {
	//	return err
	//}
	//pt.MsgStreamSearchBufSize = int64(msgStreamSearchBufSize)
	//
	//msgStreamSearchResultBufSizeStr := config.GetString(MsgStreamSearchResultBufSize)
	//msgStreamSearchResultBufSize, err := strconv.Atoi(msgStreamSearchResultBufSizeStr)
	//if err != nil {
	//	return err
	//}
	//pt.MsgStreamSearchResultBufSize = int64(msgStreamSearchResultBufSize)
	//
	//msgStreamSearchResultPulsarBufSizeStr := config.GetString(MsgStreamSearchResultPulsarBufSize)
	//msgStreamSearchResultPulsarBufSize, err := strconv.Atoi(msgStreamSearchResultPulsarBufSizeStr)
	//if err != nil {
	//	return err
	//}
	//pt.MsgStreamSearchResultPulsarBufSize = int64(msgStreamSearchResultPulsarBufSize)
	//
	//msgStreamTimeTickBufSizeStr := config.GetString(MsgStreamTimeTickBufSize)
	//msgStreamTimeTickBufSize, err := strconv.Atoi(msgStreamTimeTickBufSizeStr)
	//if err != nil {
	//	return err
	//}
	//pt.MsgStreamTimeTickBufSize = int64(msgStreamTimeTickBufSize)
	//
	//maxNameLengthStr := config.GetString(MaxNameLength)
	//maxNameLength, err := strconv.Atoi(maxNameLengthStr)
	//if err != nil {
	//	return err
	//}
	//pt.MaxNameLength = int64(maxNameLength)
	//
	//maxFieldNumStr := config.GetString(MaxFieldNum)
	//maxFieldNum, err := strconv.Atoi(maxFieldNumStr)
	//if err != nil {
	//	return err
	//}
	//pt.MaxFieldNum = int64(maxFieldNum)
	//
	//maxDimensionStr := config.GetString(MaxDimension)
	//maxDimension, err := strconv.Atoi(maxDimensionStr)
	//if err != nil {
	//	return err
	//}
	//pt.MaxDimension = int64(maxDimension)
	//
	//defaultPartitionTag := config.GetString(DefaultPartitionTag)
	//pt.DefaultPartitionTag = defaultPartitionTag

	return nil
}

func (pt *ParamTable) Init() {
	pt.BaseTable.Init()
	pt.initParams()
}

func (pt *ParamTable) initParams() {
	pt.initPulsarAddress()
	pt.initQueryNodeIDList()
	pt.initQueryNodeNum()
	pt.initProxyID()
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
	internalStr, err := pt.Load("proxyNode.timeTickInterval")
	if err != nil {
		panic(err)
	}
	interval, err := strconv.Atoi(internalStr)
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
	proxyIDStr, err := pt.Load("_proxyID")
	if err != nil {
		panic(err)
	}
	pt.ProxySubName = prefix + "-" + proxyIDStr
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
