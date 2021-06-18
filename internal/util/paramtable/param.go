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

package paramtable

import (
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
)

var Params ParamTable
var once sync.Once

type ParamTable struct {
	BaseTable

	LogConfig *log.Config
}

func (p *ParamTable) Init() {
	once.Do(func() {
		p.BaseTable.Init()

		p.initLogCfg()
	})
}

func (p *ParamTable) initLogCfg() {
	p.LogConfig = &log.Config{}
	format, err := p.Load("log.format")
	if err != nil {
		panic(err)
	}
	p.LogConfig.Format = format
	level, err := p.Load("log.level")
	if err != nil {
		panic(err)
	}
	p.LogConfig.Level = level
	devStr, err := p.Load("log.dev")
	if err != nil {
		panic(err)
	}
	dev, err := strconv.ParseBool(devStr)
	if err != nil {
		panic(err)
	}
	p.LogConfig.Development = dev
	p.LogConfig.File.MaxSize = p.ParseInt("log.file.maxSize")
	p.LogConfig.File.MaxBackups = p.ParseInt("log.file.maxBackups")
	p.LogConfig.File.MaxDays = p.ParseInt("log.file.maxAge")
	p.LogConfig.File.RootPath, err = p.Load("log.file.rootPath")
	if err != nil {
		panic(err)
	}
}
