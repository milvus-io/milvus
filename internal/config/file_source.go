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

package config

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/spf13/cast"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type FileSource struct {
	sync.RWMutex
	file    string
	configs map[string]string
}

func NewFileSource(file string) *FileSource {
	fs := &FileSource{file: file, configs: make(map[string]string)}
	fs.loadFromFile()
	return fs
}

// GetConfigurationByKey implements ConfigSource
func (fs *FileSource) GetConfigurationByKey(key string) (string, error) {
	v, ok := fs.configs[key]
	if !ok {
		return "", fmt.Errorf("key not found: %s", key)
	}
	return v, nil
}

// GetConfigurations implements ConfigSource
func (fs *FileSource) GetConfigurations() (map[string]string, error) {
	configMap := make(map[string]string)
	fs.Lock()
	defer fs.Unlock()
	for k, v := range fs.configs {
		configMap[k] = v
	}
	return configMap, nil
}

// GetPriority implements ConfigSource
func (fs *FileSource) GetPriority() int {
	return LowPriority
}

// GetSourceName implements ConfigSource
func (fs *FileSource) GetSourceName() string {
	return "FileSource"
}

func (fs *FileSource) Close() {
}

func (fs *FileSource) loadFromFile() error {
	yamlReader := viper.New()
	configFile := fs.file
	if _, err := os.Stat(configFile); err != nil {
		return errors.New("cannot access config file: " + configFile)
	}

	yamlReader.SetConfigFile(configFile)
	if err := yamlReader.ReadInConfig(); err != nil {
		return err
	}

	for _, key := range yamlReader.AllKeys() {
		val := yamlReader.Get(key)
		str, err := cast.ToStringE(val)
		if err != nil {
			switch val := val.(type) {
			case []interface{}:
				str = str[:0]
				for _, v := range val {
					ss, err := cast.ToStringE(v)
					if err != nil {
						log.Warn("cast to string failed", zap.Any("value", v))
					}
					if str == "" {
						str = ss
					} else {
						str = str + "," + ss
					}
				}

			default:
				log.Warn("val is not a slice", zap.Any("value", val))
				continue
			}
		}
		fs.Lock()
		fs.configs[key] = str
		fs.configs[formatKey(key)] = str
		fs.Unlock()
	}

	return nil
}
