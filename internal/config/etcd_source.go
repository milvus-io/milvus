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

	"github.com/milvus-io/milvus/internal/util/etcd"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	ReadConfigTimeout = 3 * time.Second
)

type EtcdSource struct {
	sync.RWMutex
	etcdCli       *clientv3.Client
	ctx           context.Context
	currentConfig map[string]string
	keyPrefix     string

	configRefresher refresher
	eh              EventHandler
}

func NewEtcdSource(etcdInfo *EtcdInfo) (*EtcdSource, error) {
	etcdCli, err := etcd.GetEtcdClient(
		etcdInfo.UseEmbed,
		etcdInfo.UseSSL,
		etcdInfo.Endpoints,
		etcdInfo.CertFile,
		etcdInfo.KeyFile,
		etcdInfo.CaCertFile,
		etcdInfo.MinVersion)
	if err != nil {
		return nil, err
	}
	es := &EtcdSource{
		etcdCli:       etcdCli,
		ctx:           context.Background(),
		currentConfig: make(map[string]string),
		keyPrefix:     etcdInfo.KeyPrefix,
	}
	es.configRefresher = newRefresher(etcdInfo.RefreshInterval, es.refreshConfigurations)
	return es, nil
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
	es.configRefresher.start(es.GetSourceName())
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
	es.etcdCli.Close()
	es.configRefresher.stop()
}

func (es *EtcdSource) SetEventHandler(eh EventHandler) {
	es.configRefresher.eh = eh
}

func (es *EtcdSource) refreshConfigurations() error {
	prefix := es.keyPrefix + "/config"
	ctx, cancel := context.WithTimeout(es.ctx, ReadConfigTimeout)
	defer cancel()
	response, err := es.etcdCli.Get(ctx, prefix, clientv3.WithPrefix())
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
	es.Lock()
	defer es.Unlock()
	err = es.configRefresher.fireEvents(es.GetSourceName(), es.currentConfig, newConfig)
	if err != nil {
		return err
	}
	es.currentConfig = newConfig
	return nil
}
