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

package paramtableutil

import (
	"path"
	"runtime"
	"strings"

	"github.com/spf13/viper"
	"github.com/zilliztech/milvus-distributed/internal/util/kvutil"
)

type GlobalParamsTable struct {
	params *kvutil.MemoryKV
}

func NewGlobalParamsTable() Base {
	return &GlobalParamsTable{params: kvutil.NewMemoryKV()}
}

var GParams = NewGlobalParamsTable()

func (gparams *GlobalParamsTable) Load(key string) (string, error) {
	return gparams.params.Load(strings.ToLower(key))
}

func (gparams *GlobalParamsTable) LoadRange(key, endKey string, limit int) ([]string, []string, error) {
	return gparams.params.LoadRange(strings.ToLower(key), strings.ToLower(endKey), limit)
}

func (gparams *GlobalParamsTable) LoadYaml(fileName string) error {
	config := viper.New()
	_, fpath, _, _ := runtime.Caller(0)
	configPath := path.Dir(fpath) + "/../../../configs/"
	config.SetConfigFile(configPath + fileName)
	if err := config.ReadInConfig(); err != nil {
		panic(err)
	}

	for _, key := range config.AllKeys() {
		err := gparams.params.Save(strings.ToLower(key), config.GetString(key))
		if err != nil {
			panic(err)
		}
	}

	return nil
}

func (gparams *GlobalParamsTable) Remove(key string) error {
	return gparams.params.Remove(strings.ToLower(key))
}

func (gparams *GlobalParamsTable) Save(key, value string) error {
	return gparams.params.Save(strings.ToLower(key), value)
}
