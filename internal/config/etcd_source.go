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
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
)

const (
	ModeWatch = iota
	ModeInterval
)

type EtcdSource struct {
	sync.RWMutex
	etcdCli          *clientv3.Client
	ctx              context.Context
	currentConfig    map[string]string
	keyPrefix        string
	refreshMode      int
	refreshInterval  time.Duration
	intervalDone     chan bool
	intervalInitOnce sync.Once
	eh               EventHandler
}

func NewEtcdSource(remoteInfo *EtcdInfo) (*EtcdSource, error) {
	etcdCli, err := clientv3.New(clientv3.Config{
		Endpoints:   remoteInfo.Endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &EtcdSource{
		etcdCli:         etcdCli,
		ctx:             context.Background(),
		currentConfig:   make(map[string]string),
		keyPrefix:       remoteInfo.KeyPrefix,
		refreshMode:     remoteInfo.RefreshMode,
		refreshInterval: remoteInfo.RefreshInterval,
		intervalDone:    make(chan bool, 1),
	}, nil
}

// GetConfigurationByKey implements ConfigSource
func (es *EtcdSource) GetConfigurationByKey(key string) (string, error) {
	es.RLock()
	v, ok := es.currentConfig[key]
	es.RUnlock()
	if !ok {
		return "", fmt.Errorf("key not found: %s", key)
	}
	return v, nil
}

// GetConfigurations implements ConfigSource
func (es *EtcdSource) GetConfigurations() (map[string]string, error) {
	configMap := make(map[string]string)
	err := es.refreshConfigurations()
	if err != nil {
		return nil, err
	}
	if es.refreshMode == ModeInterval {
		es.intervalInitOnce.Do(func() {
			go es.refreshConfigurationsPeriodically()
		})
	}

	es.RLock()
	for key, value := range es.currentConfig {
		configMap[key] = value
	}
	es.RUnlock()

	return configMap, nil
}

// GetPriority implements ConfigSource
func (es *EtcdSource) GetPriority() int {
	return HighPriority
}

// GetSourceName implements ConfigSource
func (es *EtcdSource) GetSourceName() string {
	return "EtcdSource"
}

func (es *EtcdSource) Close() {
	es.intervalDone <- true
}

func (es *EtcdSource) refreshConfigurations() error {
	prefix := es.keyPrefix + "/config"
	response, err := es.etcdCli.Get(es.ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	newConfig := make(map[string]string, len(response.Kvs))
	for _, kv := range response.Kvs {
		key := string(kv.Key)
		key = strings.TrimPrefix(key, prefix+"/")
		newConfig[key] = string(kv.Value)
		newConfig[formatKey(key)] = string(kv.Value)
	}
	return es.updateConfigurationAndFireEvent(newConfig)
}

func (es *EtcdSource) refreshConfigurationsPeriodically() {
	ticker := time.NewTicker(es.refreshInterval)
	log.Info("start refreshing configurations")
	for {
		select {
		case <-ticker.C:
			err := es.refreshConfigurations()
			if err != nil {
				log.Error("can not pull configs", zap.Error(err))
				es.intervalDone <- true
			}
		case <-es.intervalDone:
			log.Info("stop refreshing configurations")
			return
		}
	}
}

func (es *EtcdSource) updateConfigurationAndFireEvent(config map[string]string) error {
	es.Lock()
	defer es.Unlock()
	//Populate the events based on the changed value between current config and newly received Config
	events, err := PopulateEvents(es.GetSourceName(), es.currentConfig, config)
	if err != nil {
		log.Warn("generating event error", zap.Error(err))
		return err
	}
	es.currentConfig = config
	//Generate OnEvent Callback based on the events created
	if es.eh != nil {
		for _, e := range events {
			es.eh.OnEvent(e)
		}
	}
	return nil
}
