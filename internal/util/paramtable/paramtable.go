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
	"log"
	"os"
	"path"
	"runtime"
	"strings"

	"github.com/spf13/cast"
	"github.com/spf13/viper"
	"github.com/zilliztech/milvus-distributed/internal/kv"
)

type Base interface {
	Load(key string) (string, error)
	LoadRange(key, endKey string, limit int) ([]string, []string, error)
	LoadYaml(fileName string) error
	Remove(key string) error
	Save(key, value string) error
	Init()
}

type BaseTable struct {
	params *kv.MemoryKV
}

func (gp *BaseTable) Init() {
	gp.params = kv.NewMemoryKV()
	err := gp.LoadYaml("config.yaml")
	if err != nil {
		panic(err)
	}

	etcdAddress := os.Getenv("ETCD_ADDRESS")
	if etcdAddress == "" {
		etcdAddress = "localhost:2379"
	}
	err = gp.Save("_EtcdAddress", etcdAddress)
	if err != nil {
		panic(err)
	}

	pulsarAddress := os.Getenv("PULSAR_ADDRESS")
	if pulsarAddress == "" {
		pulsarAddress = "pulsar://localhost:6650"
	}
	err = gp.Save("_PulsarAddress", pulsarAddress)
	if err != nil {
		panic(err)
	}

	masterAddress := os.Getenv("MASTER_ADDRESS")
	if masterAddress == "" {
		masterAddress = "localhost:53100"
	}
	err = gp.Save("_MasterAddress", masterAddress)
	if err != nil {
		panic(err)
	}
}

func (gp *BaseTable) Load(key string) (string, error) {
	return gp.params.Load(strings.ToLower(key))
}

func (gp *BaseTable) LoadRange(key, endKey string, limit int) ([]string, []string, error) {
	return gp.params.LoadRange(strings.ToLower(key), strings.ToLower(endKey), limit)
}

func (gp *BaseTable) LoadYaml(fileName string) error {
	config := viper.New()
	_, fpath, _, _ := runtime.Caller(0)
	configFile := path.Dir(fpath) + "/../../../configs/" + fileName
	_, err := os.Stat(configFile)
	if os.IsNotExist(err) {
		runPath, err := os.Getwd()
		if err != nil {
			panic(err)
		}
		configFile = runPath + "/configs/" + fileName
	}

	config.SetConfigFile(configFile)
	if err := config.ReadInConfig(); err != nil {
		panic(err)
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
		err = gp.params.Save(strings.ToLower(key), str)
		if err != nil {
			panic(err)
		}

	}

	return nil
}

func (gp *BaseTable) Remove(key string) error {
	return gp.params.Remove(strings.ToLower(key))
}

func (gp *BaseTable) Save(key, value string) error {
	return gp.params.Save(strings.ToLower(key), value)
}
