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
	"github.com/samber/lo"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	TombValue     = "TOMB_VAULE"
	RuntimeSource = "RuntimeSource"
	// RedactedValue replaces values that are unsafe to expose in a configuration projection.
	RedactedValue = "*****"
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
	Dispatcher           *EventDispatcher
	sources              *typeutil.ConcurrentMap[string, Source]
	keySourceMap         *typeutil.ConcurrentMap[string, string] // store the key to config source, example: key is A.B.C and source is file which means the A.B.C's value is from file
	overlays             *typeutil.ConcurrentMap[string, string] // store the highest priority configs which modified at runtime
	forbiddenKeys        *typeutil.ConcurrentSet[string]
	immutableKeys        *typeutil.ConcurrentSet[string]
	sensitiveKeys        *typeutil.ConcurrentSet[string]
	sensitiveKeyPrefixes *typeutil.ConcurrentSet[string]
	// sensitiveKeyPrefixesCollapsed holds the same prefixes with separators
	// removed, so a key stored under the collapsed identity can be matched
	// without reformatting every prefix on every lookup.
	sensitiveKeyPrefixesCollapsed *typeutil.ConcurrentSet[string]
	nonSensitiveKeys              *typeutil.ConcurrentSet[string]
	nonSensitiveSuffixes          *typeutil.ConcurrentSet[sensitiveSuffixExemption]
	// declaredKeys maps a declared ParamItem's separator-free identity to its
	// dotted spelling, so the structure of the key survives a lookup made under
	// any of its aliases.
	declaredKeys *typeutil.ConcurrentMap[string, string]
	// dottedSpellings is the same map for keys nobody declared: a ParamGroup
	// member is named by whoever wrote it, so the core cannot enumerate the
	// members, but the sources still show both spellings of each one as they
	// load. Learning the pairing there is what lets a lookup made under the
	// collapsed identity be classified against the namespace it belongs to
	// rather than against a name with no structure left in it.
	dottedSpellings *typeutil.ConcurrentMap[string, string]
	// collidedSpellings remembers identities two different keys have been seen
	// under, so the refusal to learn one survives the events that arrive after
	// the initial load. Without it a later updateEvent re-learns whichever
	// spelling it happens to carry.
	collidedSpellings *typeutil.ConcurrentSet[string]
	// spellingMutex protects the cross-map invariant between dottedSpellings
	// and collidedSpellings. The maps are independently concurrent, but learning
	// a collision removes one entry and adds the other as one policy transition;
	// readers must not observe the state between those operations.
	spellingMutex sync.RWMutex
	// registeredKeyPrefixes maps a declared ParamGroup's prefix to the same
	// prefix with its separators removed. Both are needed on every lookup and
	// the collapsed one is derived, so it is derived once at registration
	// rather than #keys x #prefixes times per projection.
	registeredKeyPrefixes *typeutil.ConcurrentMap[string, string]

	cacheMutex  sync.RWMutex
	configCache map[string]any
	// configCache *typeutil.ConcurrentMap[string, interface{}]
}

func NewManager() *Manager {
	manager := &Manager{
		Dispatcher:                    NewEventDispatcher(),
		sources:                       typeutil.NewConcurrentMap[string, Source](),
		keySourceMap:                  typeutil.NewConcurrentMap[string, string](),
		overlays:                      typeutil.NewConcurrentMap[string, string](),
		forbiddenKeys:                 typeutil.NewConcurrentSet[string](),
		immutableKeys:                 typeutil.NewConcurrentSet[string](),
		sensitiveKeys:                 typeutil.NewConcurrentSet[string](),
		sensitiveKeyPrefixes:          typeutil.NewConcurrentSet[string](),
		sensitiveKeyPrefixesCollapsed: typeutil.NewConcurrentSet[string](),
		nonSensitiveKeys:              typeutil.NewConcurrentSet[string](),
		nonSensitiveSuffixes:          typeutil.NewConcurrentSet[sensitiveSuffixExemption](),
		declaredKeys:                  typeutil.NewConcurrentMap[string, string](),
		dottedSpellings:               typeutil.NewConcurrentMap[string, string](),
		collidedSpellings:             typeutil.NewConcurrentSet[string](),
		registeredKeyPrefixes:         typeutil.NewConcurrentMap[string, string](),
		configCache:                   make(map[string]any),
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
	if len(keys) == 0 {
		return
	}
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

// EtcdConfigKey returns the identity a configuration key is stored under in
// etcd. AlterConfigsInEtcd applies it on the way in, so callers that need to
// reason about collisions before writing must use the same function.
func EtcdConfigKey(key string) string {
	return formatKeyUncached(key)
}

// ResolveRegisteredConfigKey reports whether a caller-supplied key names
// declared configuration, and returns the identity to write it under.
// The identity returned is the dotted one: it is what every later predicate
// needs (a prefix cannot be matched against a separator-free key), and callers
// that write it out go through AlterConfigsInEtcd, which formats it anyway.
func (m *Manager) ResolveRegisteredConfigKey(key string) (string, RegisteredConfigKind) {
	resolved := m.resolveRegisteredKey(key)
	return resolved.dotted, resolved.kind
}

// GetRegisteredConfig reads a caller-supplied key, and is the only read API
// safe to expose to a management endpoint: it refuses keys that no ParamItem or
// ParamGroup declares, and refuses sensitive values. Callers distinguish the two
// with errors.Is against ErrKeyUnregistered and ErrKeySensitive.
func (m *Manager) GetRegisteredConfig(key string) (string, string, error) {
	resolved := m.resolveRegisteredKey(key)
	if resolved.kind == RegisteredConfigUnknown {
		return "", "", errors.Wrap(ErrKeyUnregistered, key)
	}
	if m.isSensitiveResolved(resolved) {
		return "", "", errors.Wrap(ErrKeySensitive, key)
	}
	return m.readResolved(resolved, key)
}

// readResolved reads a declared key under whichever of the two identities its
// value was stored as.
//
// The separator-free form comes first because it is the one every source agrees
// on: FileSource inserts both forms, EnvSource inserts the raw variable name
// and its formatted alias, and AlterConfigsInEtcd writes only the formatted
// one. Reading a ParamGroup member by its dotted key alone would find the file
// entry and report a stale value, with the wrong source, after an etcd
// override. The dotted form is still needed for runtime overlays written
// through SetMapConfig, which keeps the separators.
//
// Overlays are the other way round for a group member, and deliberately so:
// getBy runs its overlay pass last and lets the dotted spelling overwrite, so
// ParamGroup.GetValue reports that one when both are populated — which
// BaseTable.SaveGroup and BaseTable.Save between them can do. Reporting the
// other one here would name a value nothing is using.
func (m *Manager) readResolved(resolved resolvedKey, requestedKey string) (string, string, error) {
	// Only a ParamGroup member can legitimately live under the dotted spelling:
	// SetMapConfig writes it, and ParamGroup.GetValue reads it. A ParamItem is
	// resolved by ParamItem.get through Manager.GetConfig, which looks only
	// under the separator-free identity — so considering the dotted form for a
	// scalar would report a value nothing in the process actually uses.
	dottedApplies := resolved.kind == RegisteredConfigGroup && resolved.dotted != resolved.lookup

	// A runtime overlay outranks every source. Dotted first, because that is the
	// one ParamGroup.GetValue ends up with when both are set.
	overlayOrder := []string{resolved.lookup}
	if dottedApplies {
		overlayOrder = []string{resolved.dotted, resolved.lookup}
	}
	for _, candidate := range overlayOrder {
		if v, ok := m.overlays.Get(candidate); ok {
			if v == TombValue {
				return "", "", errors.Wrap(ErrKeyNotFound, requestedKey)
			}
			return RuntimeSource, v, nil
		}
	}

	sourceOrder := []string{resolved.lookup}
	if dottedApplies {
		sourceOrder = append(sourceOrder, resolved.dotted)
	}
	for _, candidate := range sourceOrder {
		if sourceName, ok := m.keySourceMap.Get(candidate); ok {
			v, err := m.getConfigValueBySource(candidate, sourceName)
			return sourceName, v, err
		}
	}
	return "", "", errors.Wrap(ErrKeyNotFound, requestedKey)
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
		return ErrSourceDuplicate
	}

	source.SetManager(m)
	m.sources.Insert(sourceName, source)

	err := m.pullSourceConfigs(sourceName)
	if err != nil {
		return errors.Wrapf(err, "failed to load source %s", sourceName)
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
	// Learn the pairing, for the same reason isStoredKey treats an overlay as
	// vouching for a segmentation: the two have to agree. While only isStoredKey
	// knew about overlays, a member written here was readable under the dotted
	// spelling and masked under the collapsed one — one identity, two verdicts,
	// which is the shape of every classification defect this file has had.
	//
	// RuntimeSource, not a config source: this is written through the package's
	// own setter by BaseTable.SaveGroup, so it is as trustworthy as a file.
	m.rememberSpelling(key, RuntimeSource)
	m.overlays.Insert(mapConfigKey(key), value)
}

// mapConfigKey is the identity SetMapConfig stores under, and therefore the one
// ResetConfig and DeleteConfig have to clear as well as the formatted one.
// Named and shared so the three cannot drift apart again: the removers used to
// clear only the formatted identity, so a group value written through
// BaseTable.SaveGroup survived its own deletion.
func mapConfigKey(key string) string {
	// lowerKey, not ToLower: FileSource stores keys below NotFormatPrefix with
	// their case intact, so folding it here would make SaveGroup add a second,
	// differently-cased member beside the file's rather than override it.
	return lowerKey(key)
}

// Delete config at runtime, which has the highest priority to override all other sources.
// Tombstones the identity SetMapConfig writes as well as the formatted one,
// because covering only the latter would leave a ParamGroup member in force
// after it was deleted. The two coincide for keys that have no separators left
// and for everything under NotFormatPrefix, in which case this writes one entry.
func (m *Manager) DeleteConfig(key string) {
	m.overlays.Insert(formatKey(key), TombValue)
	m.overlays.Insert(mapConfigKey(key), TombValue)
}

// Remove the config which set at runtime, use config from sources.
// Clears the identity SetMapConfig writes as well as the one SetConfig writes,
// because clearing only the latter would leave a group value set by
// BaseTable.SaveGroup in place forever. The two coincide for keys that have no
// separators left and for everything under NotFormatPrefix.
func (m *Manager) ResetConfig(key string) {
	m.overlays.Remove(formatKey(key))
	m.overlays.Remove(mapConfigKey(key))
}

// Ignore any of update events, which means the config cannot auto refresh anymore
func (m *Manager) ForbidUpdate(key string) {
	m.forbiddenKeys.Insert(formatKey(key))
}

// It cannot be changed after the first startup, except for operation and maintenance
func (m *Manager) ImmutableUpdate(key string) {
	m.immutableKeys.Insert(formatKey(key))
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
		return ErrSourceInvalid
	}

	configs, err := configSource.GetConfigurations()
	if err != nil {
		mlog.Info(context.TODO(), "Get configuration by items failed", mlog.Err(err))
		return err
	}

	sourcePriority := configSource.GetPriority()
	for key := range configs {
		m.rememberSpelling(key, source)
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
		m.rememberSpelling(e.Key, e.EventSource)
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
				mlog.Info(context.TODO(), fmt.Sprintf("the event source %s's priority is less then %s's, ignore",
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
			mlog.Info(context.TODO(), fmt.Sprintf("the event source %s (expect %s) is not maintained, ignore",
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
	mlog.Info(context.TODO(), "receive update event",
		mlog.String("eventSource", e.EventSource),
		mlog.String("eventType", e.EventType),
		mlog.String("key", e.Key),
		mlog.String("value", m.RedactValue(e.Key, e.Value)),
		mlog.Bool("hasUpdated", e.HasUpdated))
	return nil
}

// OnEvent Triggers actions when an event is generated
func (m *Manager) OnEvent(event *Event) {
	if m.forbiddenKeys.Contain(formatKey(event.Key)) {
		mlog.Info(context.TODO(), "ignore event for forbidden key", mlog.String("key", event.Key))
		return
	}
	err := m.updateEvent(event)
	if err != nil {
		mlog.Warn(context.TODO(), "failed in updating event with error",
			mlog.Err(err),
			mlog.String("eventSource", event.EventSource),
			mlog.String("eventType", event.EventType),
			mlog.String("key", event.Key),
			mlog.String("value", m.RedactValue(event.Key, event.Value)),
			mlog.Bool("hasUpdated", event.HasUpdated))
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

// ProcessImmutableConfigs persists immutable configs into etcd (create-if-absent).
// renderers optionally converts a placeholder raw value (e.g. mq.type's literal
// "default") into the concrete value to persist, keyed by config key. A renderer
// runs only when the key is not yet persisted in etcd; an existing etcd value is
// never overwritten or re-rendered.
func (m *Manager) ProcessImmutableConfigs(renderers map[string]func(raw string) string) error {
	etcdSourceImpl, ok := m.GetEtcdSource()
	if !ok {
		mlog.Info(context.TODO(), "etcd source not enable,skip processing immutable configs")
		return nil
	}

	normalizedRenderers := make(map[string]func(string) string, len(renderers))
	for key, render := range renderers {
		normalizedRenderers[formatKey(key)] = render
	}

	var saveErrors []error
	var savedConfigs []string
	m.immutableKeys.Range(func(key string) bool {
		render, hasRenderer := normalizedRenderers[key]
		confgSourceName, configValue, getConfigErr := m.GetConfig(key)
		if getConfigErr != nil {
			if !hasRenderer {
				mlog.Warn(context.TODO(), "failed to get config", mlog.String("key", key), mlog.Err(getConfigErr))
				return true
			}
			// the key exists in no source: the renderer alone decides the value to pin
			confgSourceName, configValue = "", ""
		}

		_, getFromEtcdErr := etcdSourceImpl.GetConfigurationByKey(key)
		if errors.Is(getFromEtcdErr, ErrKeyNotFound) {
			if hasRenderer {
				rendered := render(configValue)
				mlog.Info(context.TODO(), "rendered immutable config value before persisting",
					mlog.String("key", key), mlog.String("rawValue", m.RedactValue(key, configValue)),
					mlog.String("renderedValue", m.RedactValue(key, rendered)))
				configValue = rendered
			}
			mlog.Info(context.TODO(), "immutable config not exist in etcd, saving to persistent storage",
				mlog.String("fromSource", confgSourceName), mlog.String("key", key),
				mlog.String("value", m.RedactValue(key, configValue)))
			if err := m.SaveConfigToEtcd(etcdSourceImpl, key, configValue); err != nil {
				mlog.Error(context.TODO(), "failed to save immutable config to etcd",
					mlog.String("key", key), mlog.String("value", m.RedactValue(key, configValue)), mlog.Err(err))
				saveErrors = append(saveErrors, err)
			} else {
				mlog.Info(context.TODO(), "successfully saved immutable config to etcd",
					mlog.String("key", key), mlog.String("value", m.RedactValue(key, configValue)))
				savedConfigs = append(savedConfigs, key)
			}
		} else if getFromEtcdErr == nil {
			mlog.Info(context.TODO(), "immutable config already exists in etcd",
				mlog.String("key", key), mlog.String("value", m.RedactValue(key, configValue)))
		} else {
			mlog.Warn(context.TODO(), "failed to check config in etcd", mlog.String("key", key), mlog.Err(getFromEtcdErr))
		}
		return true
	})

	if len(savedConfigs) > 0 {
		mlog.Info(context.TODO(), "triggering etcd source refresh after saving immutable configs", mlog.Strings("savedConfigs", savedConfigs))
		if refreshErr := etcdSourceImpl.RefreshConfigurationsLinearizable(); refreshErr != nil {
			mlog.Warn(context.TODO(), "failed to refresh etcd configurations after saving immutable configs", mlog.Err(refreshErr))
		} else {
			mlog.Info(context.TODO(), "successfully refreshed etcd configurations after saving immutable configs")
		}
	}

	if len(saveErrors) > 0 {
		return errors.Wrapf(ErrImmutableConfigSaveFailed, "%d config(s) failed", len(saveErrors))
	}
	return nil
}

func (m *Manager) SaveConfigToEtcd(etcdSource *EtcdSource, key, value string) error {
	if etcdSource == nil || etcdSource.etcdCli == nil {
		return ErrEtcdClientUnavailable
	}
	etcdKey := fmt.Sprintf("%s/config/%s", etcdSource.keyPrefix, key)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := etcdSource.etcdCli.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(etcdKey), "=", 0)).
		Then(clientv3.OpPut(etcdKey, value)).
		Commit()
	if err != nil {
		return errors.Wrap(err, "failed to put config to etcd")
	}
	if !resp.Succeeded {
		mlog.Info(context.TODO(), "config already exists in etcd, skip writing",
			mlog.String("etcdKey", etcdKey), mlog.String("configKey", key),
			mlog.String("value", m.RedactValue(key, value)))
		return nil
	}
	mlog.Info(context.TODO(), "config atomically saved to etcd",
		mlog.String("etcdKey", etcdKey), mlog.String("configKey", key),
		mlog.String("value", m.RedactValue(key, value)))

	return nil
}

// UpdateConfigInEtcd updates a configuration value in etcd.
// Unlike SaveConfigToEtcd, this function will update the config even if it already exists.
func (m *Manager) UpdateConfigInEtcd(etcdSource *EtcdSource, key, value string) error {
	return m.AlterConfigsInEtcd(etcdSource, map[string]string{key: value}, nil)
}

// AlterConfigsInEtcd atomically updates and/or deletes configuration values in etcd.
// Both updates (put) and deletes are executed in a single etcd transaction.
func (m *Manager) AlterConfigsInEtcd(etcdSource *EtcdSource, updates map[string]string, deletes []string) error {
	if etcdSource == nil || etcdSource.etcdCli == nil {
		return ErrEtcdClientUnavailable
	}

	if len(updates) == 0 && len(deletes) == 0 {
		return ErrNoConfigsToAlter
	}

	// Build transaction operations
	ops := make([]clientv3.Op, 0, len(updates)+len(deletes))
	for key, value := range updates {
		fmtKey := EtcdConfigKey(key)
		etcdKey := fmt.Sprintf("%s/config/%s", etcdSource.keyPrefix, fmtKey)
		ops = append(ops, clientv3.OpPut(etcdKey, value))
	}
	for _, key := range deletes {
		fmtKey := EtcdConfigKey(key)
		etcdKey := fmt.Sprintf("%s/config/%s", etcdSource.keyPrefix, fmtKey)
		ops = append(ops, clientv3.OpDelete(etcdKey))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := etcdSource.etcdCli.Txn(ctx).
		Then(ops...).
		Commit()
	if err != nil {
		return errors.Wrap(err, "failed to atomically alter configs in etcd")
	}

	// Proactively refresh local EtcdSource so the write is immediately visible in this process,
	// rather than waiting for the async etcd-watch refresher. Linearizable read (no
	// WithSerializable) ensures the follower we read from has applied the txn we just
	// committed — the async refresher's serializable path would not provide that guarantee.
	if err := etcdSource.RefreshConfigurationsLinearizable(); err != nil {
		return err
	}

	// Keys only. Whether a value is safe to print depends on a classification
	// that can be wrong; the key names are enough to audit what was changed.
	mlog.Info(context.TODO(), "configs atomically altered in etcd",
		mlog.Int("updates", len(updates)),
		mlog.Int("deletes", len(deletes)),
		mlog.Strings("updatedKeys", lo.Keys(updates)),
		mlog.Strings("deleted", deletes))
	return nil
}
