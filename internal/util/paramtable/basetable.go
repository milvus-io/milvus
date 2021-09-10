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
	"os"
	"path"
	"runtime"
	"strconv"
	"strings"

	"go.uber.org/zap"

	memkv "github.com/milvus-io/milvus/internal/kv/mem"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/spf13/cast"
	"github.com/spf13/viper"
)

type UniqueID = typeutil.UniqueID

type Base interface {
	Load(key string) (string, error)
	LoadRange(key, endKey string, limit int) ([]string, []string, error)
	LoadYaml(fileName string) error
	Remove(key string) error
	Save(key, value string) error
	Init()
}

type BaseTable struct {
	params    *memkv.MemoryKV
	configDir string
}

func (gp *BaseTable) Init() {
	gp.params = memkv.NewMemoryKV()

	_, fpath, _, _ := runtime.Caller(0)
	configDir := path.Dir(fpath) + "/../../../configs/"
	if _, err := os.Stat(configDir); err != nil {
		log.Warn("cannot access config directory", zap.String("configDir", configDir), zap.Error(err))
		if runPath, err1 := os.Getwd(); err1 != nil {
			panic(err1.Error())
		} else {
			configDir = runPath + "/configs/"
		}
	}
	gp.configDir = configDir
	log.Debug("config directory", zap.String("configDir", gp.configDir))

	if err := gp.LoadYaml("milvus.yaml"); err != nil {
		panic(err)
	}
	if err := gp.LoadYaml("advanced/common.yaml"); err != nil {
		panic(err)
	}
	if err := gp.LoadYaml("advanced/channel.yaml"); err != nil {
		panic(err)
	}
	gp.tryloadFromEnv()
}

func (gp *BaseTable) GetConfigDir() string {
	return gp.configDir
}

func (gp *BaseTable) LoadFromKVPair(kvPairs []*commonpb.KeyValuePair) error {
	for _, pair := range kvPairs {
		err := gp.Save(pair.Key, pair.Value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (gp *BaseTable) tryloadFromEnv() {
	minioAddress := os.Getenv("MINIO_ADDRESS")
	if minioAddress == "" {
		minioHost, err := gp.Load("minio.address")
		if err != nil {
			panic(err)
		}
		port, err := gp.Load("minio.port")
		if err != nil {
			panic(err)
		}
		minioAddress = minioHost + ":" + port
	}
	err := gp.Save("_MinioAddress", minioAddress)
	if err != nil {
		panic(err)
	}

	etcdEndpoints := os.Getenv("ETCD_ENDPOINTS")
	if etcdEndpoints == "" {
		etcdEndpoints, err = gp.Load("etcd.endpoints")
		if err != nil {
			panic(err)
		}
	}
	err = gp.Save("_EtcdEndpoints", etcdEndpoints)
	if err != nil {
		panic(err)
	}

	pulsarAddress := os.Getenv("PULSAR_ADDRESS")
	if pulsarAddress == "" {
		pulsarHost, err := gp.Load("pulsar.address")
		if err != nil {
			panic(err)
		}
		port, err := gp.Load("pulsar.port")
		if err != nil {
			panic(err)
		}
		pulsarAddress = "pulsar://" + pulsarHost + ":" + port
	}
	err = gp.Save("_PulsarAddress", pulsarAddress)
	if err != nil {
		panic(err)
	}

	rocksmqPath := os.Getenv("ROCKSMQ_PATH")
	if rocksmqPath == "" {
		path, err := gp.Load("rocksmq.path")
		if err != nil {
			panic(err)
		}
		rocksmqPath = path
	}
	err = gp.Save("_RocksmqPath", rocksmqPath)
	if err != nil {
		panic(err)
	}

	rootCoordAddress := os.Getenv("ROOT_COORD_ADDRESS")
	if rootCoordAddress == "" {
		rootCoordHost, err := gp.Load("rootCoord.address")
		if err != nil {
			panic(err)
		}
		port, err := gp.Load("rootCoord.port")
		if err != nil {
			panic(err)
		}
		rootCoordAddress = rootCoordHost + ":" + port
	}
	err = gp.Save("_RootCoordAddress", rootCoordAddress)
	if err != nil {
		panic(err)
	}

	indexCoordAddress := os.Getenv("INDEX_COORD_ADDRESS")
	if indexCoordAddress == "" {
		indexCoordHost, err := gp.Load("indexCoord.address")
		if err != nil {
			panic(err)
		}
		port, err := gp.Load("indexCoord.port")
		if err != nil {
			panic(err)
		}
		indexCoordAddress = indexCoordHost + ":" + port
	}
	err = gp.Save("_IndexCoordAddress", indexCoordAddress)
	if err != nil {
		panic(err)
	}

	queryCoordAddress := os.Getenv("QUERY_COORD_ADDRESS")
	if queryCoordAddress == "" {
		serviceHost, err := gp.Load("queryCoord.address")
		if err != nil {
			panic(err)
		}
		port, err := gp.Load("queryCoord.port")
		if err != nil {
			panic(err)
		}
		queryCoordAddress = serviceHost + ":" + port
	}
	err = gp.Save("_QueryCoordAddress", queryCoordAddress)
	if err != nil {
		panic(err)
	}

	dataCoordAddress := os.Getenv("DATA_COORD_ADDRESS")
	if dataCoordAddress == "" {
		serviceHost, err := gp.Load("dataCoord.address")
		if err != nil {
			panic(err)
		}
		port, err := gp.Load("dataCoord.port")
		if err != nil {
			panic(err)
		}
		dataCoordAddress = serviceHost + ":" + port
	}
	err = gp.Save("_DataCoordAddress", dataCoordAddress)
	if err != nil {
		panic(err)
	}
}

func (gp *BaseTable) Load(key string) (string, error) {
	return gp.params.Load(strings.ToLower(key))
}

func (gp *BaseTable) LoadWithDefault(key string, defaultValue string) (string, error) {
	return gp.params.LoadWithDefault(strings.ToLower(key), defaultValue)
}

func (gp *BaseTable) LoadRange(key, endKey string, limit int) ([]string, []string, error) {
	return gp.params.LoadRange(strings.ToLower(key), strings.ToLower(endKey), limit)
}

func (gp *BaseTable) LoadYaml(fileName string) error {
	config := viper.New()
	configFile := gp.configDir + fileName
	if _, err := os.Stat(configFile); err != nil {
		panic("cannot access config file: " + configFile)
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
						panic(err)
					}
					if len(str) == 0 {
						str = ss
					} else {
						str = str + "," + ss
					}
				}

			default:
				panic("undefined config type, key=" + key)
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

func (gp *BaseTable) ParseBool(key string, defaultValue bool) bool {
	valueStr, err := gp.LoadWithDefault(key, strconv.FormatBool(defaultValue))
	if err != nil {
		panic(err)
	}
	value, err := strconv.ParseBool(valueStr)
	if err != nil {
		panic(err)
	}
	return value
}

func (gp *BaseTable) ParseFloat(key string) float64 {
	valueStr, err := gp.Load(key)
	if err != nil {
		panic(err)
	}
	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		panic(err)
	}
	return value
}

func (gp *BaseTable) ParseInt64(key string) int64 {
	valueStr, err := gp.Load(key)
	if err != nil {
		panic(err)
	}
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		panic(err)
	}
	return int64(value)
}

func (gp *BaseTable) ParseInt32(key string) int32 {
	valueStr, err := gp.Load(key)
	if err != nil {
		panic(err)
	}
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		panic(err)
	}
	return int32(value)
}

func (gp *BaseTable) ParseInt(key string) int {
	valueStr, err := gp.Load(key)
	if err != nil {
		panic(err)
	}
	value, err := strconv.Atoi(valueStr)
	if err != nil {
		panic(err)
	}
	return value
}

// package methods

func ConvertRangeToIntRange(rangeStr, sep string) []int {
	items := strings.Split(rangeStr, sep)
	if len(items) != 2 {
		panic("Illegal range ")
	}

	startStr := items[0]
	endStr := items[1]
	start, err := strconv.Atoi(startStr)
	if err != nil {
		panic(err)
	}
	end, err := strconv.Atoi(endStr)
	if err != nil {
		panic(err)
	}

	if start < 0 || end < 0 {
		panic("Illegal range value")
	}
	if start > end {
		panic("Illegal range value, start > end")
	}
	return []int{start, end}
}

func ConvertRangeToIntSlice(rangeStr, sep string) []int {
	rangeSlice := ConvertRangeToIntRange(rangeStr, sep)
	start, end := rangeSlice[0], rangeSlice[1]
	var ret []int
	for i := start; i < end; i++ {
		ret = append(ret, i)
	}
	return ret
}
