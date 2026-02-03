// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/cockroachdb/errors"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	TombValue     = "TOMB_VAULE"
	RuntimeSource = "RuntimeSource"
)

type Filter func(key string) (string, bool)

func WithSubstr(substring string) Filter {
	substring = strings.ToLower(substring)
	return func(key string) (string, bool) {
		return key, strings.Contains(key, substring)
	}
}

func WithPrefix(prefix string) Filter {
	prefix = strings.ToLower(prefix)
	return func(key string) (string, bool) {
		return key, strings.HasPrefix(key, prefix)
	}
}

func WithOneOfPrefixs(prefixs ...string) Filter {
	for id, prefix := range prefixs {
		prefixs[id] = strings.ToLower(prefix)
	}
	return func(key string) (string, bool) {
		for _, prefix := range prefixs {
			if strings.HasPrefix(key, prefix) {
				return key, true
			}
		}
		return key, false
	}
}

func RemovePrefix(prefix string) Filter {
	prefix = strings.ToLower(prefix)
	return func(key string) (string, bool) {
		return strings.Replace(key, prefix, "", 1), true
	}
}

func filterate(key string, filters ...Filter) (string, bool) {
	var ok bool
	for _, filter := range filters {
		key, ok = filter(key)
		if !ok {
			return key, ok
		}
	}
	return key, ok
}

type Manager struct {
	Dispatcher    *EventDispatcher
	sources       *typeutil.ConcurrentMap[string, Source]
	keySourceMap  *typeutil.ConcurrentMap[string, string] // store the key to config source, example: key is A.B.C and source is file which means the A.B.C's value is from file
	overlays      *typeutil.ConcurrentMap[string, string] // store the highest priority configs which modified at runtime
	forbiddenKeys *typeutil.ConcurrentSet[string]
	immutableKeys *typeutil.ConcurrentSet[string]

	cacheMutex  sync.RWMutex
	configCache map[string]any
	// configCache *typeutil.ConcurrentMap[string, interface{}]
}

func NewManager() *Manager {
	manager := &Manager{
		Dispatcher:    NewEventDispatcher(),
		sources:       typeutil.NewConcurrentMap[string, Source](),
		keySourceMap:  typeutil.NewConcurrentMap[string, string](),
		overlays:      typeutil.NewConcurrentMap[string, string](),
		forbiddenKeys: typeutil.NewConcurrentSet[string](),
		immutableKeys: typeutil.NewConcurrentSet[string](),
		configCache:   make(map[string]any),
	}
	resetConfigCacheFunc := NewHandler("reset.config.cache", func(event *Event) {
		keyToRemove := strings.NewReplacer("/", ".").Replace(event.Key)
		manager.EvictCachedValue(keyToRemove)
	})
	manager.Dispatcher.RegisterForKeyPrefix("", resetConfigCacheFunc)
	return manager
}

func (m *Manager) GetCachedValue(key string) (interface{}, bool) {
	m.cacheMutex.RLock()
	defer m.cacheMutex.RUnlock()
	value, ok := m.configCache[key]
	return value, ok
}

func (m *Manager) CASCachedValue(key string, origin string, value interface{}) bool {
	m.cacheMutex.Lock()
	defer m.cacheMutex.Unlock()
	_, current, err := m.GetConfig(key)
	if errors.Is(err, ErrKeyNotFound) {
		m.configCache[key] = value
		return true
	}
	if err != nil {
		return false
	}
	if current != origin {
		return false
	}
	m.configCache[key] = value
	return true
}

func (m *Manager) EvictCachedValue(key string) {
	m.cacheMutex.Lock()
	defer m.cacheMutex.Unlock()
	// cause param'value may rely on other params, so we need to evict all the cached value when config is changed
	clear(m.configCache)
}

func (m *Manager) EvictCacheValueByFormat(keys ...string) {
	m.cacheMutex.Lock()
	defer m.cacheMutex.Unlock()
	// cause param'value may rely on other params, so we need to evict all the cached value when config is changed
	clear(m.configCache)
}

func (m *Manager) GetConfig(key string) (string, string, error) {
	realKey := formatKey(key)
	v, ok := m.overlays.Get(realKey)
	if ok {
		if v == TombValue {
			return "", "", errors.Wrap(ErrKeyNotFound, key) // fmt.Errorf("key not found %s", key)
		}
		return RuntimeSource, v, nil
	}
	sourceName, ok := m.keySourceMap.Get(realKey)
	if !ok {
		return "", "", errors.Wrap(ErrKeyNotFound, key) // fmt.Errorf("key not found: %s", key)
	}
	v, err := m.getConfigValueBySource(realKey, sourceName)
	return sourceName, v, err
}

// GetConfigs returns all the key values
func (m *Manager) GetConfigs() map[string]string {
	config := make(map[string]string)

	m.keySourceMap.Range(func(key, value string) bool {
		_, sValue, err := m.GetConfig(key)
		if err != nil {
			return true
		}

		config[key] = sValue
		return true
	})

	m.overlays.Range(func(key, value string) bool {
		config[key] = value
		return true
	})

	return config
}

func (m *Manager) GetConfigsView() map[string]string {
	config := make(map[string]string)

	valueFmt := func(source, value string) string {
		return fmt.Sprintf("%s[%s]", value, source)
	}

	m.keySourceMap.Range(func(key, value string) bool {
		source, sValue, err := m.GetConfig(key)
		if err != nil {
			return true
		}

		config[key] = valueFmt(source, sValue)
		return true
	})

	m.overlays.Range(func(key, value string) bool {
		config[key] = valueFmt(RuntimeSource, value)
		return true
	})

	return config
}

func (m *Manager) GetBy(filters ...Filter) map[string]string {
	matchedConfig := make(map[string]string)

	m.keySourceMap.Range(func(key string, value string) bool {
		newkey, ok := filterate(key, filters...)
		if !ok {
			return true
		}
		_, sValue, err := m.GetConfig(key)
		if err != nil {
			return true
		}

		matchedConfig[newkey] = sValue
		return true
	})

	m.overlays.Range(func(key, value string) bool {
		newkey, ok := filterate(key, filters...)
		if !ok {
			return true
		}
		matchedConfig[newkey] = value
		return true
	})

	return matchedConfig
}

func (m *Manager) FileConfigs() map[string]string {
	config := make(map[string]string)
	m.sources.Range(func(key string, value Source) bool {
		if s, ok := value.(*FileSource); ok {
			config, _ = s.GetConfigurations()
			return false
		}
		return true
	})
	return config
}

func (m *Manager) Close() {
	m.sources.Range(func(key string, value Source) bool {
		value.Close()
		return true
	})
}

func (m *Manager) AddSource(source Source) error {
	sourceName := source.GetSourceName()
	_, ok := m.sources.Get(sourceName)
	if ok {
		err := errors.New("duplicate source supplied")
		return err
	}

	source.SetManager(m)
	m.sources.Insert(sourceName, source)

	err := m.pullSourceConfigs(sourceName)
	if err != nil {
		err = fmt.Errorf("failed to load %s cause: %x", sourceName, err)
		return err
	}

	source.SetEventHandler(m)

	return nil
}

// Update config at runtime, which can be called by others
// The most used scenario is UT
func (m *Manager) SetConfig(key, value string) {
	m.overlays.Insert(formatKey(key), value)
}

func (m *Manager) SetMapConfig(key, value string) {
	m.overlays.Insert(strings.ToLower(key), value)
}

// Delete config at runtime, which has the highest priority to override all other sources
func (m *Manager) DeleteConfig(key string) {
	m.overlays.Insert(formatKey(key), TombValue)
}

// Remove the config which set at runtime, use config from sources
func (m *Manager) ResetConfig(key string) {
	m.overlays.Remove(formatKey(key))
}

// Ignore any of update events, which means the config cannot auto refresh anymore
func (m *Manager) ForbidUpdate(key string) {
	m.forbiddenKeys.Insert(formatKey(key))
}

// It cannot be changed after the first startup, except for operation and maintenance
func (m *Manager) ImmutableUpdate(key string) {
	m.immutableKeys.Insert(formatKey(key))
}

// IsImmutable checks if a configuration key is marked as immutable
func (m *Manager) IsImmutable(key string) bool {
	return m.immutableKeys.Contain(formatKey(key))
}

func (m *Manager) UpdateSourceOptions(opts ...Option) {
	var options Options
	for _, opt := range opts {
		opt(&options)
	}

	m.sources.Range(func(key string, value Source) bool {
		value.UpdateOptions(options)
		return true
	})
}

// Do not use it directly, only used when add source and unittests.
func (m *Manager) pullSourceConfigs(source string) error {
	configSource, ok := m.sources.Get(source)
	if !ok {
		return errors.New("invalid source or source not added")
	}

	configs, err := configSource.GetConfigurations()
	if err != nil {
		log.Info("Get configuration by items failed", zap.Error(err))
		return err
	}

	sourcePriority := configSource.GetPriority()
	for key := range configs {
		sourceName, ok := m.keySourceMap.Get(key)
		if !ok { // if key do not exist then add source
			m.keySourceMap.Insert(key, source)
			continue
		}

		currentSource, ok := m.sources.Get(sourceName)
		if !ok {
			m.keySourceMap.Insert(key, source)
			continue
		}

		currentSrcPriority := currentSource.GetPriority()
		if currentSrcPriority > sourcePriority { // lesser value has high priority
			m.keySourceMap.Insert(key, source)
		}
	}

	return nil
}

func (m *Manager) getConfigValueBySource(configKey, sourceName string) (string, error) {
	source, ok := m.sources.Get(sourceName)
	if !ok {
		return "", ErrKeyNotFound
	}

	return source.GetConfigurationByKey(configKey)
}

func (m *Manager) updateEvent(e *Event) error {
	// refresh all configuration one by one
	if e.HasUpdated {
		return nil
	}
	switch e.EventType {
	case CreateType, UpdateType:
		sourceName, ok := m.keySourceMap.Get(e.Key)
		if !ok {
			m.keySourceMap.Insert(e.Key, e.EventSource)
			e.EventType = CreateType
		} else if sourceName == e.EventSource {
			e.EventType = UpdateType
		} else if sourceName != e.EventSource {
			prioritySrc := m.getHighPrioritySource(sourceName, e.EventSource)
			if prioritySrc != nil && prioritySrc.GetSourceName() == sourceName {
				// if event generated from less priority source then ignore
				log.Info(fmt.Sprintf("the event source %s's priority is less then %s's, ignore",
					e.EventSource, sourceName))
				return ErrIgnoreChange
			}
			m.keySourceMap.Insert(e.Key, e.EventSource)
			e.EventType = UpdateType
		}

	case DeleteType:
		sourceName, ok := m.keySourceMap.Get(e.Key)
		if !ok || sourceName != e.EventSource {
			// if delete event generated from source not maintained ignore it
			log.Info(fmt.Sprintf("the event source %s (expect %s) is not maintained, ignore",
				e.EventSource, sourceName))
			return ErrIgnoreChange
		} else if sourceName == e.EventSource {
			// find less priority source or delete key
			source := m.findNextBestSource(e.Key, sourceName)
			if source == nil {
				m.keySourceMap.Remove(e.Key)
			} else {
				m.keySourceMap.Insert(e.Key, source.GetSourceName())
			}
		}
	}

	e.HasUpdated = true
	log.Info("receive update event", zap.Any("event", e))
	return nil
}

// OnEvent Triggers actions when an event is generated
func (m *Manager) OnEvent(event *Event) {
	if m.forbiddenKeys.Contain(formatKey(event.Key)) {
		log.Info("ignore event for forbidden key", zap.String("key", event.Key))
		return
	}
	err := m.updateEvent(event)
	if err != nil {
		log.Warn("failed in updating event with error", zap.Error(err), zap.Any("event", event))
		return
	}

	m.Dispatcher.Dispatch(event)
}

func (m *Manager) GetIdentifier() string {
	return "Manager"
}

func (m *Manager) findNextBestSource(configKey string, sourceName string) Source {
	var rSource Source
	m.sources.Range(func(key string, value Source) bool {
		if value.GetSourceName() == sourceName {
			return true
		}
		_, err := value.GetConfigurationByKey(configKey)
		if err != nil {
			return true
		}
		if rSource == nil {
			rSource = value
			return true
		}
		if value.GetPriority() < rSource.GetPriority() { // less value has high priority
			rSource = value
		}
		return true
	})

	return rSource
}

func (m *Manager) getHighPrioritySource(srcNameA, srcNameB string) Source {
	sourceA, okA := m.sources.Get(srcNameA)
	sourceB, okB := m.sources.Get(srcNameB)

	if !okA && !okB {
		return nil
	} else if !okA {
		return sourceB
	} else if !okB {
		return sourceA
	}

	if sourceA.GetPriority() < sourceB.GetPriority() { // less value has high priority
		return sourceA
	}

	return sourceB
}

// GetEtcdSource returns the EtcdSource if available
func (m *Manager) GetEtcdSource() (*EtcdSource, bool) {
	etcdSource, ok := m.sources.Get("EtcdSource")
	if !ok {
		return nil, false
	}

	etcdSourceImpl, ok := etcdSource.(*EtcdSource)
	if !ok {
		return nil, false
	}
	return etcdSourceImpl, true
}

func (m *Manager) ProcessImmutableConfigs() error {
	etcdSourceImpl, ok := m.GetEtcdSource()
	if !ok {
		log.Info("etcd source not enable,skip processing immutable configs")
		return nil
	}

	var saveErrors []error
	var savedConfigs []string
	m.immutableKeys.Range(func(key string) bool {
		confgSourceName, configValue, getConfigErr := m.GetConfig(key)
		if getConfigErr != nil {
			log.Warn("failed to get config", zap.String("key", key), zap.Error(getConfigErr))
			return true
		}

		_, getFromEtcdErr := etcdSourceImpl.GetConfigurationByKey(key)
		if errors.Is(getFromEtcdErr, ErrKeyNotFound) {
			log.Info("immutable config not exist in etcd, saving to persistent storage",
				zap.String("fromSource", confgSourceName), zap.String("key", key), zap.String("value", configValue))
			if err := m.SaveConfigToEtcd(etcdSourceImpl, key, configValue); err != nil {
				log.Error("failed to save immutable config to etcd",
					zap.String("key", key), zap.String("value", configValue), zap.Error(err))
				saveErrors = append(saveErrors, err)
			} else {
				log.Info("successfully saved immutable config to etcd", zap.String("key", key), zap.String("value", configValue))
				savedConfigs = append(savedConfigs, key)
			}
		} else if getFromEtcdErr == nil {
			log.Info("immutable config already exists in etcd", zap.String("key", key), zap.String("value", configValue))
		} else {
			log.Warn("failed to check config in etcd", zap.String("key", key), zap.Error(getFromEtcdErr))
		}
		return true
	})

	if len(savedConfigs) > 0 {
		log.Info("triggering etcd source refresh after saving immutable configs", zap.Strings("savedConfigs", savedConfigs))
		if refreshErr := etcdSourceImpl.refreshConfigurations(); refreshErr != nil {
			log.Warn("failed to refresh etcd configurations after saving immutable configs", zap.Error(refreshErr))
		} else {
			log.Info("successfully refreshed etcd configurations after saving immutable configs")
		}
	}

	if len(saveErrors) > 0 {
		return fmt.Errorf("failed to save %d immutable configs to etcd", len(saveErrors))
	}
	return nil
}

func (m *Manager) SaveConfigToEtcd(etcdSource *EtcdSource, key, value string) error {
	if etcdSource == nil || etcdSource.etcdCli == nil {
		return errors.New("etcd client is not available")
	}
	etcdKey := fmt.Sprintf("%s/config/%s", etcdSource.keyPrefix, key)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := etcdSource.etcdCli.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(etcdKey), "=", 0)).
		Then(clientv3.OpPut(etcdKey, value)).
		Commit()
	if err != nil {
		return fmt.Errorf("failed to put config to etcd: %w", err)
	}
	if !resp.Succeeded {
		log.Info("config already exists in etcd, skip writing",
			zap.String("etcdKey", etcdKey), zap.String("configKey", key), zap.String("value", value))
		return nil
	}
	log.Info("config atomically saved to etcd",
		zap.String("etcdKey", etcdKey), zap.String("configKey", key), zap.String("value", value))

	return nil
}

// UpdateConfigInEtcd updates a configuration value in etcd.
// Unlike SaveConfigToEtcd, this function will update the config even if it already exists.
func (m *Manager) UpdateConfigInEtcd(etcdSource *EtcdSource, key, value string) error {
	return m.UpdateConfigsInEtcd(etcdSource, map[string]string{key: value})
}

// UpdateConfigsInEtcd atomically updates multiple configuration values in etcd.
// This function uses etcd transaction to ensure all configs are updated atomically.
func (m *Manager) UpdateConfigsInEtcd(etcdSource *EtcdSource, configs map[string]string) error {
	if etcdSource == nil || etcdSource.etcdCli == nil {
		return errors.New("etcd client is not available")
	}

	if len(configs) == 0 {
		return errors.New("no configs to update")
	}

	// Build transaction operations
	ops := make([]clientv3.Op, 0, len(configs))

	for key, value := range configs {
		fmtKey := formatKey(key)
		etcdKey := fmt.Sprintf("%s/config/%s", etcdSource.keyPrefix, fmtKey)
		ops = append(ops, clientv3.OpPut(etcdKey, value))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Execute all operations in a single transaction for atomicity
	resp, err := etcdSource.etcdCli.Txn(ctx).
		Then(ops...).
		Commit()
	if err != nil {
		return fmt.Errorf("failed to atomically update configs in etcd: %w", err)
	}

	if !resp.Succeeded {
		return errors.New("transaction failed to update configs in etcd")
	}

	log.Info("configs atomically updated in etcd", zap.Int("count", len(configs)), zap.Any("configs", configs))
	return nil
}
