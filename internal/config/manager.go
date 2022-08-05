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
	"strings"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

const (
	TombValue        = "TOMB_VALUE"
	CustomSourceName = "CustomSource"
)

type Manager struct {
	sync.RWMutex
	sources        map[string]Source
	keySourceMap   map[string]string
	overlayConfigs map[string]string // store the configs setted or deleted by user
}

func NewManager() *Manager {
	return &Manager{
		sources:        make(map[string]Source),
		keySourceMap:   make(map[string]string),
		overlayConfigs: make(map[string]string),
	}
}

func (m *Manager) GetConfig(key string) (string, error) {
	m.RLock()
	defer m.RUnlock()
	realKey := formatKey(key)
	v, ok := m.overlayConfigs[realKey]
	if ok {
		if v == TombValue {
			return "", fmt.Errorf("key not found: %s", key)
		}
		return v, nil
	}
	sourceName, ok := m.keySourceMap[realKey]
	if !ok {
		return "", fmt.Errorf("key not found: %s", key)
	}
	return m.getConfigValueBySource(realKey, sourceName)
}

//GetConfigsByPattern returns key values that matched pattern
func (m *Manager) GetConfigsByPattern(pattern string) map[string]string {
	m.RLock()
	defer m.RLock()
	matchedConfig := make(map[string]string)
	pattern = strings.ToLower(pattern)
	for key, value := range m.keySourceMap {
		result := strings.HasPrefix(key, pattern)

		if result {
			sValue, err := m.getConfigValueBySource(key, value)
			if err != nil {
				continue
			}
			matchedConfig[key] = sValue
		}
	}
	return matchedConfig
}

// Configs returns all the key values
func (m *Manager) Configs() map[string]string {
	m.RLock()
	defer m.RLock()
	config := make(map[string]string)

	for key, value := range m.keySourceMap {
		sValue, err := m.getConfigValueBySource(key, value)
		if err != nil {
			continue
		}
		config[key] = sValue
	}

	return config
}

// For compatible reason, only visiable for Test
func (m *Manager) SetConfig(key, value string) {
	m.Lock()
	defer m.Unlock()
	realKey := formatKey(key)
	m.overlayConfigs[realKey] = value
	m.updateEvent(newEvent(CustomSourceName, CreateType, realKey, value))
}

// For compatible reason, only visiable for Test
func (m *Manager) DeleteConfig(key string) {
	m.Lock()
	defer m.Unlock()
	realKey := formatKey(key)
	m.overlayConfigs[realKey] = TombValue
	m.updateEvent(newEvent(realKey, DeleteType, realKey, ""))
}

func (m *Manager) Close() {
	for _, s := range m.sources {
		s.Close()
	}
}

func (m *Manager) AddSource(source Source) error {
	m.Lock()
	defer m.Unlock()
	sourceName := source.GetSourceName()
	_, ok := m.sources[sourceName]
	if ok {
		err := errors.New("duplicate source supplied")
		return err
	}

	m.sources[sourceName] = source

	err := m.pullSourceConfigs(sourceName)
	if err != nil {
		err = fmt.Errorf("failed to load %s cause: %x", sourceName, err)
		return err
	}

	return nil
}

// Do not use it directly, only used when add source and unittests.
func (m *Manager) pullSourceConfigs(source string) error {
	configSource, ok := m.sources[source]
	if !ok {
		return errors.New("invalid source or source not added")
	}

	configs, err := configSource.GetConfigurations()
	if err != nil {
		log.Error("Get configuration by items failed", zap.Error(err))
		return err
	}

	sourcePriority := configSource.GetPriority()
	for key := range configs {
		sourceName, ok := m.keySourceMap[key]
		if !ok { // if key do not exist then add source
			m.keySourceMap[key] = source
			continue
		}

		currentSource, ok := m.sources[sourceName]
		if !ok {
			m.keySourceMap[key] = source
			continue
		}

		currentSrcPriority := currentSource.GetPriority()
		if currentSrcPriority > sourcePriority { // lesser value has high priority
			m.keySourceMap[key] = source
		}
	}

	return nil
}

func (m *Manager) getConfigValueBySource(configKey, sourceName string) (string, error) {
	source, ok := m.sources[sourceName]
	if !ok {
		return "", ErrKeyNotFound
	}

	return source.GetConfigurationByKey(configKey)
}

func (m *Manager) updateEvent(e *Event) error {
	// refresh all configuration one by one
	log.Debug("receive update event", zap.Any("event", e))
	if e.HasUpdated {
		return nil
	}
	switch e.EventType {
	case CreateType, UpdateType:
		sourceName, ok := m.keySourceMap[e.Key]
		if !ok {
			m.keySourceMap[e.Key] = e.EventSource
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
			m.keySourceMap[e.Key] = e.EventSource
			e.EventType = UpdateType
		}

	case DeleteType:
		sourceName, ok := m.keySourceMap[e.Key]
		if !ok || sourceName != e.EventSource {
			// if delete event generated from source not maintained ignore it
			log.Info(fmt.Sprintf("the event source %s (expect %s) is not maintained, ignore",
				e.EventSource, sourceName))
			return ErrIgnoreChange
		} else if sourceName == e.EventSource {
			// find less priority source or delete key
			source := m.findNextBestSource(e.Key, sourceName)
			if source == nil {
				delete(m.keySourceMap, e.Key)
			} else {
				m.keySourceMap[e.Key] = source.GetSourceName()
			}
		}

	}

	e.HasUpdated = true
	return nil
}

// OnEvent Triggers actions when an event is generated
func (m *Manager) OnEvent(event *Event) {
	m.Lock()
	defer m.Unlock()
	err := m.updateEvent(event)
	if err != nil {
		log.Warn("failed in updating event with error", zap.Error(err), zap.Any("event", event))
		return
	}

	// m.dispatcher.DispatchEvent(event)
}

func (m *Manager) findNextBestSource(key string, sourceName string) Source {
	var rSource Source
	for _, source := range m.sources {
		if source.GetSourceName() == sourceName {
			continue
		}
		_, err := source.GetConfigurationByKey(key)
		if err != nil {
			continue
		}
		if rSource == nil {
			rSource = source
			continue
		}
		if source.GetPriority() < rSource.GetPriority() { // less value has high priority
			rSource = source
		}
	}

	return rSource
}

func (m *Manager) getHighPrioritySource(srcNameA, srcNameB string) Source {
	sourceA, okA := m.sources[srcNameA]
	sourceB, okB := m.sources[srcNameB]

	if !okA && !okB {
		return nil
	} else if !okA {
		return sourceB
	} else if !okB {
		return sourceA
	}

	if sourceA.GetPriority() < sourceB.GetPriority() { //less value has high priority
		return sourceA
	}

	return sourceB
}
