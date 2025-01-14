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
	"os"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/log"
)

type FileSource struct {
	sync.RWMutex
	files   []string
	configs map[string]string

	updateMu        sync.Mutex
	configRefresher *refresher
	manager         ConfigManager
}

func NewFileSource(fileInfo *FileInfo) *FileSource {
	fs := &FileSource{
		files:   fileInfo.Files,
		configs: make(map[string]string),
	}
	fs.configRefresher = newRefresher(fileInfo.RefreshInterval, fs.loadFromFile)
	return fs
}

// GetConfigurationByKey implements ConfigSource
func (fs *FileSource) GetConfigurationByKey(key string) (string, error) {
	fs.RLock()
	v, ok := fs.configs[key]
	fs.RUnlock()
	if !ok {
		return "", errors.Wrap(ErrKeyNotFound, key) // fmt.Errorf("key not found: %s", key)
	}
	return v, nil
}

// GetConfigurations implements ConfigSource
func (fs *FileSource) GetConfigurations() (map[string]string, error) {
	configMap := make(map[string]string)

	err := fs.loadFromFile()
	if err != nil {
		return nil, err
	}

	fs.configRefresher.start(fs.GetSourceName())

	fs.RLock()
	for k, v := range fs.configs {
		configMap[k] = v
	}
	fs.RUnlock()
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
	fs.configRefresher.stop()
}

func (fs *FileSource) SetManager(m ConfigManager) {
	fs.Lock()
	defer fs.Unlock()
	fs.manager = m
}

func (fs *FileSource) SetEventHandler(eh EventHandler) {
	fs.RWMutex.Lock()
	defer fs.RWMutex.Unlock()
	fs.configRefresher.SetEventHandler(eh)
}

func (fs *FileSource) UpdateOptions(opts Options) {
	if opts.FileInfo == nil {
		return
	}

	fs.Lock()
	defer fs.Unlock()
	fs.files = opts.FileInfo.Files
}

func (fs *FileSource) loadFromFile() error {
	yamlReader := viper.New()
	newConfig := make(map[string]string)
	var configFiles []string

	fs.RLock()
	configFiles = fs.files
	fs.RUnlock()

	notExistsNum := 0
	for _, configFile := range configFiles {
		if _, err := os.Stat(configFile); err != nil {
			if os.IsNotExist(err) {
				notExistsNum++
				continue
			}
			return err
		}

		yamlReader.SetConfigFile(configFile)
		if err := yamlReader.ReadInConfig(); err != nil {
			return errors.Wrap(err, "Read config failed: "+configFile)
		}

		for _, key := range yamlReader.AllKeys() {
			val := yamlReader.Get(key)
			str, err := cast.ToStringE(val)
			if err != nil {
				switch val := val.(type) {
				case []any:
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
			newConfig[key] = str
			newConfig[formatKey(key)] = str
		}
	}
	// not allow all config files missing, return error for this case
	if notExistsNum == len(configFiles) {
		return errors.Newf("all config files not exists, files: %v", configFiles)
	}

	return fs.update(newConfig)
}

// update souce config
// make sure only update changes configs
func (fs *FileSource) update(configs map[string]string) error {
	// make sure config not change when fire event
	fs.updateMu.Lock()
	defer fs.updateMu.Unlock()

	fs.Lock()
	events, err := PopulateEvents(fs.GetSourceName(), fs.configs, configs)
	if err != nil {
		fs.Unlock()
		log.Warn("generating event error", zap.Error(err))
		return err
	}
	fs.configs = configs
	fs.Unlock()
	if fs.manager != nil {
		fs.manager.EvictCacheValueByFormat(lo.Map(events, func(event *Event, _ int) string { return event.Key })...)
	}

	fs.configRefresher.fireEvents(events...)
	return nil
}
