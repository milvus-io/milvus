// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package paramtable

import (
	"os"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/menv"
)

const (
	MilvusConfigRefreshIntervalEnvKey = "MILVUS_CONFIG_REFRESH_INTERVAL"
)

var (
	once         sync.Once
	params       ComponentParam
	hookParams   hookConfig
	cipherParams cipherConfig
)

func Init() {
	once.Do(func() {
		opts := []Option{}
		if refreshInterval := os.Getenv(MilvusConfigRefreshIntervalEnvKey); refreshInterval != "" {
			if duration, err := time.ParseDuration(refreshInterval); err == nil {
				log.Info("set config refresh interval", zap.Duration("duration", duration))
				opts = append(opts, Interval(duration))
			}
		}
		baseTable := NewBaseTable(opts...)
		params.Init(baseTable)
		hookBaseTable := NewBaseTableFromYamlOnly(hookYamlFile)
		hookParams.init(hookBaseTable)

		cipherBaseTable := NewBaseTableFromYamlOnly(cipherYamlFile)
		cipherParams.init(cipherBaseTable)
	})
}

func InitWithBaseTable(baseTable *BaseTable) {
	once.Do(func() {
		params.Init(baseTable)
		hookBaseTable := NewBaseTableFromYamlOnly(hookYamlFile)
		hookParams.init(hookBaseTable)
		cipherBaseTable := NewBaseTableFromYamlOnly(cipherYamlFile)
		cipherParams.init(cipherBaseTable)
	})
}

func Get() *ComponentParam {
	Init()
	return &params
}

func GetBaseTable() *BaseTable {
	return params.baseTable
}

func GetHookParams() *hookConfig {
	return &hookParams
}

func GetCipherParams() *cipherConfig {
	return &cipherParams
}

var (
	SetNodeID                = menv.SetNodeID
	GetNodeID                = menv.GetNodeID
	GetStringNodeID          = menv.GetStringNodeID
	SetRole                  = menv.SetRole
	GetRole                  = menv.GetRole
	SetCreateTime            = menv.SetCreateTime
	GetCreateTime            = menv.GetCreateTime
	SetUpdateTime            = menv.SetUpdateTime
	GetUpdateTime            = menv.GetUpdateTime
	SetLocalComponentEnabled = menv.SetLocalComponentEnabled
	IsLocalComponentEnabled  = menv.IsLocalComponentEnabled
)
