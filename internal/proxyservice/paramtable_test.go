// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package proxyservice

import (
	"testing"

	"github.com/milvus-io/milvus/internal/log"
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
