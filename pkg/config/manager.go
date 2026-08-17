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

// RegisteredConfigKind identifies how a declared external configuration key
// must be resolved.
type RegisteredConfigKind int

const (
	RegisteredConfigUnknown RegisteredConfigKind = iota
	RegisteredConfigScalar
	RegisteredConfigGroup
)

// sensitiveKeyPatterns provides defense in depth for secret-like dynamic and
// source keys. Reviewed false positives use explicit NonSensitive metadata.
var sensitiveKeyPatterns = []string{
	"password",
	"secret",
	"token",
	"credential",
	"privatekey",
	"accesskey",
	"apikey",
}

// sensitivePatternReplacer normalizes a key before matching it against
// sensitiveKeyPatterns, so that api_key, api-key and apiKey all collapse to the
// same shape.
var sensitivePatternReplacer = strings.NewReplacer("-", "", "_", "", ".", "", "/", "")

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
	Dispatcher            *EventDispatcher
	sources               *typeutil.ConcurrentMap[string, Source]
	keySourceMap          *typeutil.ConcurrentMap[string, string] // store the key to config source, example: key is A.B.C and source is file which means the A.B.C's value is from file
	overlays              *typeutil.ConcurrentMap[string, string] // store the highest priority configs which modified at runtime
	forbiddenKeys         *typeutil.ConcurrentSet[string]
	immutableKeys         *typeutil.ConcurrentSet[string]
	sensitiveKeys         *typeutil.ConcurrentSet[string]
	sensitiveKeyPrefixes  *typeutil.ConcurrentSet[string]
	nonSensitiveKeys      *typeutil.ConcurrentSet[string]
	nonSensitiveSuffixes  *typeutil.ConcurrentSet[string]
	registeredKeys        *typeutil.ConcurrentSet[string]
	registeredKeyPrefixes *typeutil.ConcurrentSet[string]

	cacheMutex  sync.RWMutex
	configCache map[string]any
	// configCache *typeutil.ConcurrentMap[string, interface{}]
}

func NewManager() *Manager {
	manager := &Manager{
		Dispatcher:            NewEventDispatcher(),
		sources:               typeutil.NewConcurrentMap[string, Source](),
		keySourceMap:          typeutil.NewConcurrentMap[string, string](),
		overlays:              typeutil.NewConcurrentMap[string, string](),
		forbiddenKeys:         typeutil.NewConcurrentSet[string](),
		immutableKeys:         typeutil.NewConcurrentSet[string](),
		sensitiveKeys:         typeutil.NewConcurrentSet[string](),
		sensitiveKeyPrefixes:  typeutil.NewConcurrentSet[string](),
		nonSensitiveKeys:      typeutil.NewConcurrentSet[string](),
		nonSensitiveSuffixes:  typeutil.NewConcurrentSet[string](),
		registeredKeys:        typeutil.NewConcurrentSet[string](),
		registeredKeyPrefixes: typeutil.NewConcurrentSet[string](),
		configCache:           make(map[string]any),
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
	return m.getConfigByRealKey(formatKey(key), key)
}

func (m *Manager) getConfigByRealKey(realKey, requestedKey string) (string, string, error) {
	v, ok := m.overlays.Get(realKey)
	if ok {
		if v == TombValue {
			return "", "", errors.Wrap(ErrKeyNotFound, requestedKey) // fmt.Errorf("key not found %s", requestedKey)
		}
		return RuntimeSource, v, nil
	}
	sourceName, ok := m.keySourceMap.Get(realKey)
	if !ok {
		return "", "", errors.Wrap(ErrKeyNotFound, requestedKey) // fmt.Errorf("key not found: %s", requestedKey)
	}
	v, err := m.getConfigValueBySource(realKey, sourceName)
	return sourceName, v, err
}

// ResolveRegisteredConfigKey returns the key identity used by externally
// managed configuration. ParamItems use the historical separator-free lookup
// key, while ParamGroup members preserve their dotted suffix so an arbitrary
// environment alias cannot impersonate a dynamic group value.
func (m *Manager) ResolveRegisteredConfigKey(key string) (string, RegisteredConfigKind) {
	return m.resolveRegisteredKey(key)
}

// GetRegisteredConfig reads a caller-supplied key, and is the only read API
// safe to expose to a management endpoint: it refuses keys that no ParamItem or
// ParamGroup declares, and refuses credentials. Callers distinguish the two
// with errors.Is against ErrKeyUnregistered and ErrKeySensitive.
func (m *Manager) GetRegisteredConfig(key string) (string, string, error) {
	canonical, kind := m.resolveRegisteredKey(key)
	if kind == RegisteredConfigUnknown {
		return "", "", errors.Wrap(ErrKeyUnregistered, key)
	}
	if kind == RegisteredConfigGroup && m.groupHasEnvironmentSource(canonical) {
		// ParamGroup suffixes are open-ended. Unlike an explicitly registered
		// ParamItem, neither a dotted environment variable nor its
		// separator-free alias proves that the value is Milvus configuration;
		// fail closed rather than exposing an arbitrary process secret.
		return "", "", errors.Wrap(ErrKeyUnregistered, key)
	}
	if m.IsSensitive(canonical) {
		return "", "", errors.Wrap(ErrKeySensitive, key)
	}
	return m.getRegisteredValue(canonical, key)
}

// getRegisteredValue reads a declared key that may be stored under either of
// the two identities a ParamGroup member can take.
//
// The separator-free form is the one every source agrees on: FileSource and
// EnvSource insert both forms, and AlterConfigsInEtcd writes only this one — so
// resolving by the dotted key alone would find the file entry and report a
// stale value, with the wrong source, after an etcd override. The dotted form
// is still needed for runtime overlays written through SetMapConfig, which
// keeps the separators.
func (m *Manager) getRegisteredValue(canonical, requestedKey string) (string, string, error) {
	candidates := []string{formatKey(canonical)}
	if canonical != candidates[0] {
		candidates = append(candidates, canonical)
	}

	// A runtime overlay outranks every source, whichever identity it was
	// written under.
	for _, candidate := range candidates {
		if v, ok := m.overlays.Get(candidate); ok {
			if v == TombValue {
				return "", "", errors.Wrap(ErrKeyNotFound, requestedKey)
			}
			return RuntimeSource, v, nil
		}
	}
	for _, candidate := range candidates {
		if sourceName, ok := m.keySourceMap.Get(candidate); ok {
			v, err := m.getConfigValueBySource(candidate, sourceName)
			return sourceName, v, err
		}
	}
	return "", "", errors.Wrap(ErrKeyNotFound, requestedKey)
}

func (m *Manager) groupHasEnvironmentSource(canonical string) bool {
	if source, ok := m.keySourceMap.Get(canonical); ok && source == environmentSourceName {
		return true
	}
	if formatted := formatKey(canonical); formatted != canonical {
		if source, ok := m.keySourceMap.Get(formatted); ok && source == environmentSourceName {
			return true
		}
	}
	return false
}

// GetConfigs returns a safe projection of all key values: credentials are
// replaced by RedactedValue, and keys that no ParamItem or ParamGroup declares
// are omitted entirely. Internal code that requires the original values must
// call GetConfigsRaw explicitly.
func (m *Manager) GetConfigs() map[string]string {
	return m.getConfigs(true)
}

// GetConfigsRaw returns all original key values without redaction.
func (m *Manager) GetConfigsRaw() map[string]string {
	return m.getConfigs(false)
}

func (m *Manager) getConfigs(redact bool) map[string]string {
	config := make(map[string]string)

	m.keySourceMap.Range(func(key, value string) bool {
		_, sValue, err := m.GetConfig(key)
		if err != nil {
			return true
		}

		if projected, include := m.projectValue(key, sValue, redact); include {
			config[key] = projected
		}
		return true
	})

	m.overlays.Range(func(key, value string) bool {
		if projected, include := m.projectValue(key, value, redact); include {
			config[key] = projected
		}
		return true
	})

	return config
}

// GetConfigsView returns a safe projection of all key values annotated with
// the source that supplied them.
func (m *Manager) GetConfigsView() map[string]string {
	config := make(map[string]string)

	annotate := func(key, value, source string) {
		switch m.classify(key) {
		case projectionOmit:
		case projectionRedact:
			config[key] = RedactedValue
		default:
			config[key] = fmt.Sprintf("%s[%s]", value, source)
		}
	}

	m.keySourceMap.Range(func(key, value string) bool {
		source, sValue, err := m.GetConfig(key)
		if err != nil {
			return true
		}
		annotate(key, sValue, source)
		return true
	})

	m.overlays.Range(func(key, value string) bool {
		annotate(key, value, RuntimeSource)
		return true
	})

	return config
}

func (m *Manager) GetBy(filters ...Filter) map[string]string {
	return m.getBy(true, filters...)
}

// GetByRaw returns matching original values without redaction.
func (m *Manager) GetByRaw(filters ...Filter) map[string]string {
	return m.getBy(false, filters...)
}

func (m *Manager) getBy(redact bool, filters ...Filter) map[string]string {
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

		if projected, include := m.projectValue(key, sValue, redact); include {
			matchedConfig[newkey] = projected
		}
		return true
	})

	m.overlays.Range(func(key, value string) bool {
		newkey, ok := filterate(key, filters...)
		if !ok {
			return true
		}
		if projected, include := m.projectValue(key, value, redact); include {
			matchedConfig[newkey] = projected
		}
		return true
	})

	return matchedConfig
}

// FileConfigs returns a safe projection of the file-source values.
func (m *Manager) FileConfigs() map[string]string {
	config := make(map[string]string)
	m.sources.Range(func(key string, value Source) bool {
		if s, ok := value.(*FileSource); ok {
			config, _ = s.GetConfigurations()
			return false
		}
		return true
	})
	for key, value := range config {
		if projected, include := m.projectValue(key, value, true); include {
			config[key] = projected
		} else {
			delete(config, key)
		}
	}
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

// RegisterConfigKey records a declared configuration key. Config sources may
// contain arbitrary values, including every process environment variable, so
// safe projections must distinguish declared Milvus configuration from source
// implementation details.
func (m *Manager) RegisterConfigKey(key string) {
	formattedKey := formatKey(key)
	if formattedKey != "" {
		m.registeredKeys.Insert(formattedKey)
	}
}

// RegisterConfigPrefix records a declared dynamic configuration prefix.
//
// An empty prefix declares every key of this manager to be Milvus
// configuration. That is correct only for a manager whose sources are all
// operator-authored — hook.yaml is the one such case. Declaring it on a
// manager that carries an EnvSource would hand the whole process environment
// to configuration projections, so ComponentParam is asserted to have no
// empty-prefix ParamGroup (see TestNoEmptyPrefixParamGroup).
func (m *Manager) RegisterConfigPrefix(prefix string) {
	m.registeredKeyPrefixes.Insert(strings.ToLower(prefix))
}

// RegisterSensitiveKey marks a declared configuration key as sensitive.
func (m *Manager) RegisterSensitiveKey(key string) {
	formattedKey := formatKey(key)
	if formattedKey != "" {
		m.sensitiveKeys.Insert(formattedKey)
	}
}

// RegisterNonSensitiveKey exempts a reviewed, declared key from the
// secret-name fallback. Use it only for names such as password length or token
// count whose values are not credentials.
func (m *Manager) RegisterNonSensitiveKey(key string) {
	formattedKey := formatKey(key)
	if formattedKey != "" {
		m.nonSensitiveKeys.Insert(formattedKey)
	}
}

// RegisterSensitivePrefix marks every configuration below a dynamic prefix as
// sensitive.
func (m *Manager) RegisterSensitivePrefix(prefix string) {
	canonicalPrefix := strings.ToLower(prefix)
	if canonicalPrefix != "" {
		m.sensitiveKeyPrefixes.Insert(canonicalPrefix)
	}
}

// RegisterNonSensitiveSuffix exempts one leaf name below a sensitive prefix.
//
// A ParamGroup is marked sensitive when its members are provider- or
// plugin-defined and the core cannot enumerate which of them carry credentials.
// That default is right for the unknown ones, but wrong for the leaves the
// group itself defines: an enable flag and an endpoint URL are the same class
// of infrastructure detail as minio.address, and hiding them costs operators
// the ability to see whether a provider is even switched on. Everything below
// the prefix that is not exempted here still fails closed.
func (m *Manager) RegisterNonSensitiveSuffix(prefix, suffix string) {
	canonicalPrefix := strings.ToLower(prefix)
	canonicalSuffix := strings.ToLower(suffix)
	if canonicalPrefix == "" || canonicalSuffix == "" {
		return
	}
	m.nonSensitiveSuffixes.Insert(suffixExemption(canonicalPrefix, canonicalSuffix))
}

// suffixExemption joins a prefix and a leaf name into one set entry. The NUL
// separator cannot occur in a configuration key, so the two halves can never be
// confused with a key that happens to concatenate to the same string.
func suffixExemption(prefix, suffix string) string {
	return prefix + "\x00" + suffix
}

// leafName returns the last dot-separated segment of a canonical key.
func leafName(canonicalKey string) string {
	if idx := strings.LastIndex(canonicalKey, "."); idx >= 0 {
		return canonicalKey[idx+1:]
	}
	return canonicalKey
}

func (m *Manager) resolveRegisteredKey(key string) (canonical string, kind RegisteredConfigKind) {
	formattedKey := formatKey(key)
	if m.registeredKeys.Contain(formattedKey) {
		return formattedKey, RegisteredConfigScalar
	}

	// Prefix registration intentionally uses the canonical, separator-preserving
	// key. EnvSource also stores separator-free aliases for every process
	// environment variable, so accepting such aliases here would let an
	// unrelated key such as PROXY_ACCESSLOG_FORMATTERS_DATABASE_URL masquerade
	// as a member of proxy.accessLog.formatters.
	canonicalKey := lowerKey(strings.ReplaceAll(key, "/", "."))
	m.registeredKeyPrefixes.Range(func(prefix string) bool {
		if strings.HasPrefix(canonicalKey, prefix) && len(canonicalKey) > len(prefix) {
			kind = RegisteredConfigGroup
			return false
		}
		return true
	})
	return canonicalKey, kind
}

// IsSensitive reports whether key is a credential.
//
// Precedence is explicit-before-inferred, and it matters in both directions: a
// declared NonSensitive ParamItem that happens to sit below a sensitive
// ParamGroup prefix (kafka.producer.message.max.bytes) must stay readable, and
// a declared Sensitive key must stay hidden whatever its name looks like.
func (m *Manager) IsSensitive(key string) bool {
	formattedKey := formatKey(key)
	if m.sensitiveKeys.Contain(formattedKey) {
		return true
	}
	if m.nonSensitiveKeys.Contain(formattedKey) {
		return false
	}

	canonicalKey := strings.ToLower(strings.ReplaceAll(key, "/", "."))
	if m.matchesSensitivePrefix(canonicalKey) {
		return true
	}

	patternKey := sensitivePatternReplacer.Replace(strings.ToLower(key))
	for _, pattern := range sensitiveKeyPatterns {
		if strings.Contains(patternKey, pattern) {
			return true
		}
	}
	return false
}

func (m *Manager) matchesSensitivePrefix(canonicalKey string) bool {
	leaf := leafName(canonicalKey)
	matched := false
	m.sensitiveKeyPrefixes.Range(func(prefix string) bool {
		if !strings.HasPrefix(canonicalKey, prefix) {
			return true
		}
		if m.nonSensitiveSuffixes.Contain(suffixExemption(prefix, leaf)) {
			// Exempted for this prefix, but another sensitive prefix may still
			// cover the key, so keep scanning.
			return true
		}
		matched = true
		return false
	})
	return matched
}

// projectionKind is what a configuration projection does with one key.
type projectionKind int

const (
	// projectionKeep emits the original value.
	projectionKeep projectionKind = iota
	// projectionRedact emits RedactedValue: the key is declared Milvus
	// configuration, so naming it is fine, but its value is a credential.
	projectionRedact
	// projectionOmit drops the entry entirely. The key is not declared
	// configuration at all — EnvSource imports the whole process environment —
	// so the name is itself source implementation detail, and publishing a list
	// of every environment variable in the pod is a disclosure of its own.
	projectionOmit
)

func (m *Manager) classify(key string) projectionKind {
	canonical, kind := m.resolveRegisteredKey(key)
	if kind == RegisteredConfigUnknown {
		return projectionOmit
	}
	if kind == RegisteredConfigGroup && m.groupHasEnvironmentSource(canonical) {
		return projectionOmit
	}
	if m.IsSensitive(canonical) {
		return projectionRedact
	}
	return projectionKeep
}

// ShouldRedact reports whether a key's value is unsafe to write out verbatim.
// Undeclared keys fail closed. Use it for logs, where the key is already known
// to whoever reads the line; configuration projections use classify instead,
// because there the set of key names is itself part of what leaks.
func (m *Manager) ShouldRedact(key string) bool {
	return m.classify(key) != projectionKeep
}

// RedactValue returns a log-safe value for key.
func (m *Manager) RedactValue(key, value string) string {
	if m.ShouldRedact(key) {
		return RedactedValue
	}
	return value
}

// RedactValues returns a log-safe copy without mutating values.
func (m *Manager) RedactValues(values map[string]string) map[string]string {
	redacted := make(map[string]string, len(values))
	for key, value := range values {
		redacted[key] = m.RedactValue(key, value)
	}
	return redacted
}

// projectValue returns the value to publish for key and whether to publish the
// entry at all.
func (m *Manager) projectValue(key, value string, redact bool) (string, bool) {
	if !redact {
		return value, true
	}
	switch m.classify(key) {
	case projectionOmit:
		return "", false
	case projectionRedact:
		return RedactedValue, true
	default:
		return value, true
	}
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
		fmtKey := formatKey(key)
		etcdKey := fmt.Sprintf("%s/config/%s", etcdSource.keyPrefix, fmtKey)
		ops = append(ops, clientv3.OpPut(etcdKey, value))
	}
	for _, key := range deletes {
		fmtKey := formatKey(key)
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
