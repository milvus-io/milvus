package proxyservice

import (
	"testing"

	"github.com/zilliztech/milvus-distributed/internal/log"
	"go.uber.org/zap"
)

func TestParamTable_Init(t *testing.T) {
	Params.Init()

	log.Debug("TestParamTable_Init", zap.Any("PulsarAddress", Params.PulsarAddress))
	log.Debug("TestParamTable_Init", zap.Any("MasterAddress", Params.MasterAddress))
	log.Debug("TestParamTable_Init", zap.Any("NodeTimeTickChannel", Params.NodeTimeTickChannel))
	log.Debug("TestParamTable_Init", zap.Any("ServiceTimeTickChannel", Params.ServiceTimeTickChannel))
	log.Debug("TestParamTable_Init", zap.Any("DataServiceAddress", Params.DataServiceAddress))
	log.Debug("TestParamTable_Init", zap.Any("InsertChannelPrefixName", Params.InsertChannelPrefixName))
	log.Debug("TestParamTable_Init", zap.Any("InsertChannelNum", Params.InsertChannelNum))
}
