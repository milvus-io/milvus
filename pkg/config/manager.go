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

// sensitiveKeyPatterns is the last-resort classifier: it decides any key that
// no explicit rule claims, whether or not something declares that key.
//
// That "whether or not" is easy to misread and matters. A declared ParamItem
// with a credential-shaped name is caught by this list even though nothing
// marked it Sensitive, which is why proxy.maxPasswordLength and
// proxy.minPasswordLength carry NonSensitive: true — they are length bounds,
// and without the flag they would read as passwords. Declaring a key is not by
// itself a statement that its value is safe.
//
// The list must not be narrower than the audit tripwire in paramtable. A
// manager whose ParamGroup declares the empty prefix (hook.yaml, whose keys are
// plugin-defined and cannot be enumerated by the core) has nothing else to go
// on at all.
var sensitiveKeyPatterns = []string{
	"password",
	"secret",
	"token",
	"credential",
	"privatekey",
	"accesskey",
	"apikey",
	"authparams",
	"saslusername",
	"licensekey",
}

// SensitiveKeyPatterns returns the substrings the last-resort classifier matches
// on. Exported so the paramtable audit can build its own, wider tripwire from
// this list rather than a copy that silently drifts below it.
func SensitiveKeyPatterns() []string {
	return append([]string(nil), sensitiveKeyPatterns...)
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
	nonSensitiveSuffixes          *typeutil.ConcurrentSet[string]
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
	// spellingMutex serialises the check-then-act in rememberSpelling. Without
	// it two source refreshers can each pass the collision check and then
	// re-teach the identity one of them was supposed to have retired.
	spellingMutex sync.Mutex
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
		nonSensitiveSuffixes:          typeutil.NewConcurrentSet[string](),
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
// ParamGroup declares, and refuses credentials. Callers distinguish the two
// with errors.Is against ErrKeyUnregistered and ErrKeySensitive.
func (m *Manager) GetRegisteredConfig(key string) (string, string, error) {
	resolved := m.resolveRegisteredKey(key)
	if resolved.kind == RegisteredConfigUnknown {
		return "", "", errors.Wrap(ErrKeyUnregistered, key)
	}
	if m.isSensitiveResolved(resolved.lookup, resolved.dotted, resolved.segmented) {
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

// isStoredKey reports whether some source other than the environment, or a
// runtime overlay, holds a value under exactly this spelling.
//
// This is what vouches for a segmentation nothing declared — see
// resolvedKey.segmented — so it excludes the environment for the same reason
// rememberSpelling and groupMemberIsEnvironmentOnly do: environment variable
// names are not a namespace, they are whatever the pod happens to carry, and a
// name that arrives with dots in it ("env 'a.b=c'") would otherwise vouch for
// its own segmentation. groupMemberIsEnvironmentOnly refuses such a key before
// it reaches any exemption today; agreeing with it here means that stays true
// if the two are ever reached in the other order.
func (m *Manager) isStoredKey(dotted string) bool {
	for _, candidate := range [2]string{dotted, strings.ReplaceAll(dotted, ".", "/")} {
		if value, ok := m.overlays.Get(candidate); ok && value != TombValue {
			return true
		}
		if source, ok := m.keySourceMap.Get(candidate); ok && source != environmentSourceName {
			return true
		}
	}
	return false
}

// groupMemberIsEnvironmentOnly reports whether the only thing standing behind a
// prefix-matching key is a process environment variable that happens to
// collapse into the group's separator-free namespace.
//
// A prefix match alone is not enough to trust a caller-supplied key. EnvSource
// stores every variable in the pod under KeyFormatter(name), which is the same
// namespace ParamGroup members are stored in, so a caller could otherwise reach
// an arbitrary variable by re-spelling it with dots until it matched a
// registered prefix — PROXY_ACCESSLOG_FORMATTERS_DATABASE_URL asked for as
// proxy.accessLog.formatters.DATABASE_URL.
//
// A key counts as configuration when some source other than the environment
// supplied it, under either the group's dotted namespace or the separator-free
// identity: an etcd entry written by /management/config/alter is legitimate,
// and a key that exists nowhere yet is a member waiting to be created. Only the
// environment is ambiguous, and only the environment is refused.
//
// The source has to be checked, not merely the presence of the key. EnvSource
// stores each variable under its RAW name as well as the formatted alias
// (env_source.go), and a raw name is frequently already in canonical form —
// PATH, HOSTNAME, http_proxy. Treating "the dotted spelling exists" as proof of
// configuration would hand every such variable to a group whose prefix they
// happen to match, which for the empty prefix is all of them.
func (m *Manager) groupMemberIsEnvironmentOnly(dotted, lookup string) bool {
	// Every spelling the same key can be stored under. EtcdSource keeps whatever
	// separator the stored key used, hence the slash form.
	candidates := []string{
		dotted,
		strings.ReplaceAll(dotted, ".", "/"),
		lookup,
	}
	// The one identity formatKey refuses to produce. It differs from lookup only
	// for knowhere.*, which formatKey exempts and the EnvSource key formatter
	// does not — see strippedKey. Appending it unconditionally would repeat the
	// lookup above for every other key in the table.
	if stripped := strippedKey(dotted); stripped != lookup {
		candidates = append(candidates, stripped)
	}
	sawEnvironment := false
	for _, candidate := range candidates {
		// Overlays are written only through this package's own setters, never
		// from a config source, so a live one is trusted evidence on its own. A
		// tombstone is not: it records that the key was deleted.
		if value, ok := m.overlays.Get(candidate); ok && value != TombValue {
			return false
		}
		source, ok := m.keySourceMap.Get(candidate)
		if !ok {
			continue
		}
		if source != environmentSourceName {
			return false
		}
		sawEnvironment = true
	}
	return sawEnvironment
}

// GetConfigs returns a safe projection of all key values: credentials are
// replaced by RedactedValue, and keys that no ParamItem or ParamGroup declares
// are omitted entirely. Internal code that requires the original values must
// call GetConfigsRaw explicitly.
//
// The projection is only as complete as the declarations made so far. A Manager
// whose ParamItems have not been initialised yet declares nothing, so this
// returns an empty map rather than an error — call it after the owning
// ParamTable is built, or use GetConfigsRaw if you are not serving the result
// to anyone.
func (m *Manager) GetConfigs() map[string]string {
	return m.getConfigs(true)
}

// GetConfigsRaw returns all original key values without redaction.
func (m *Manager) GetConfigsRaw() map[string]string {
	return m.getConfigs(false)
}

// everyKey is the accept function for the projections that take no filters. It
// is not filterate with an empty list: filterate reports false when it is handed
// no filters, which is what makes GetBy() with no arguments return nothing.
func everyKey(key string) (string, bool) { return key, true }

func (m *Manager) getConfigs(redact bool) map[string]string {
	config := make(map[string]string)
	m.walkProjection(!redact, everyKey, func(key, storedKey, value, _ string) {
		if projected, include := m.projectValue(storedKey, value, redact); include {
			config[key] = projected
		}
	})
	return config
}

// walkProjection visits every configuration entry once and hands it to emit,
// with the key both as the caller's filters rewrote it and as it is stored:
// classification is keyed by the stored spelling, the projection by the
// rewritten one.
//
// The three projections differ in what they build out of an entry, not in which
// entries there are. That a tombstone records a deletion rather than a value,
// and that only one of an overlay's two spellings is the one its consumer
// reads, are properties of the configuration and not of any one caller, so they
// are decided here instead of three times over.
//
// includeInertOverlays is for the Raw variants: an overlay written under the
// spelling nothing reads is not part of the configuration in force, but it is
// part of what the manager holds, and an internal caller asking for originals
// is asking for the latter.
func (m *Manager) walkProjection(includeInertOverlays bool, accept func(string) (string, bool), emit func(key, storedKey, value, source string)) {
	m.keySourceMap.Range(func(storedKey, _ string) bool {
		key, ok := accept(storedKey)
		if !ok {
			return true
		}
		source, value, err := m.GetConfig(storedKey)
		if err != nil {
			return true
		}
		emit(key, storedKey, value, source)
		return true
	})

	// Last, so that a runtime overlay overwrites the source value for the same
	// key rather than the other way round.
	m.overlays.Range(func(storedKey, value string) bool {
		key, ok := accept(storedKey)
		if !ok {
			return true
		}
		if value == TombValue {
			// Deleted at runtime; it is not a key whose value is the literal
			// "TOMB_VAULE", which is what publishing it would claim.
			return true
		}
		if !includeInertOverlays && !m.overlayIsAuthoritative(storedKey) {
			return true
		}
		emit(key, storedKey, value, RuntimeSource)
		return true
	})
}

// GetConfigsView returns a safe projection of all key values annotated with
// the source that supplied them.
func (m *Manager) GetConfigsView() map[string]string {
	config := make(map[string]string)
	m.walkProjection(false, everyKey, func(key, storedKey, value, source string) {
		switch m.classify(storedKey) {
		case projectionOmit:
		case projectionRedact:
			// Keep the annotation: which source supplies a credential is not
			// secret, it is the most useful thing left to say about it, and
			// dropping it would break the value[source] shape for exactly the
			// entries an operator is most likely to be chasing.
			config[key] = fmt.Sprintf("%s[%s]", RedactedValue, source)
		default:
			config[key] = fmt.Sprintf("%s[%s]", value, source)
		}
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
	// filterate, not everyKey: GetBy() with no filters has always matched
	// nothing, and this is an exported API.
	accept := func(key string) (string, bool) { return filterate(key, filters...) }
	m.walkProjection(!redact, accept, func(key, storedKey, value, _ string) {
		if projected, include := m.projectValue(storedKey, value, redact); include {
			matchedConfig[key] = projected
		}
	})
	return matchedConfig
}

// FileConfigs returns a safe projection of the file-source values.
func (m *Manager) FileConfigs() map[string]string {
	var fileConfigs map[string]string
	m.sources.Range(func(key string, value Source) bool {
		if s, ok := value.(*FileSource); ok {
			fileConfigs, _ = s.GetConfigurations()
			return false
		}
		return true
	})
	// Project into a fresh map rather than editing fileConfigs in place: it is
	// the FileSource's answer, and today it happens to be a copy.
	projected := make(map[string]string, len(fileConfigs))
	for key, value := range fileConfigs {
		if value, include := m.projectValue(key, value, true); include {
			projected[key] = value
		}
	}
	return projected
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

// IsImmutable checks if a configuration key is marked as immutable.
// Uncached for the same reason as resolveRegisteredKey: the management endpoint
// hands this caller-supplied keys.
func (m *Manager) IsImmutable(key string) bool {
	return m.immutableKeys.Contain(formatKeyUncached(key))
}

// RegisterConfigKey records a declared configuration key. Config sources may
// contain arbitrary values, including every process environment variable, so
// safe projections must distinguish declared Milvus configuration from source
// implementation details.
func (m *Manager) RegisterConfigKey(key string) {
	formattedKey := formatKey(key)
	if formattedKey != "" {
		m.declaredKeys.Insert(formattedKey, lowerKey(strings.ReplaceAll(key, "/", ".")))
	}
}

// RegisterConfigPrefix records a declared dynamic configuration prefix.
//
// An empty prefix declares every key of this manager to be Milvus
// configuration, which is correct only for a table whose sources are all
// operator-authored — hook.yaml is the one such case. It is accepted rather
// than refused because the refusal could not be made reliable: prefixes and
// sources are registered in either order, so a check here would fire or not
// depending on which came first. What actually contains the hazard is
// groupMemberIsEnvironmentOnly, which refuses a key whose only backing is the
// environment whatever prefix matched it —
// TestEmptyPrefixNeverPublishesTheEnvironment is the guarantee.
func (m *Manager) RegisterConfigPrefix(prefix string) {
	canonicalPrefix := strings.ToLower(prefix)
	m.registeredKeyPrefixes.Insert(canonicalPrefix, formatKeyUncached(canonicalPrefix))
}

// RegisteredConfigPrefixes returns the dynamic namespaces declared so far, in
// no particular order. A manager whose ParamGroup declares no KeyPrefix — the
// hook table — reports the empty prefix, so a caller appending a leaf to each
// entry must expect a bare key back.
//
// Exported for the paramtable audits: they used to enumerate namespaces by
// reflecting over ParamGroup fields, which made a prefix registered any other
// way — grpc_param.go registers the two CDC namespaces directly, since nothing
// reads them as a group — invisible to the very tests that check namespaces are
// classified consistently.
func (m *Manager) RegisteredConfigPrefixes() []string {
	prefixes := make([]string, 0, m.registeredKeyPrefixes.Len())
	m.registeredKeyPrefixes.Range(func(prefix, _ string) bool {
		prefixes = append(prefixes, prefix)
		return true
	})
	return prefixes
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
// sensitive. The empty prefix is accepted and means every key of this manager,
// which is what Sensitive on a group with no KeyPrefix declares; dropping it as
// a no-op would be the one registration path in this file that fails open.
func (m *Manager) RegisterSensitivePrefix(prefix string) {
	canonicalPrefix := strings.ToLower(prefix)
	m.sensitiveKeyPrefixes.Insert(canonicalPrefix)
	m.sensitiveKeyPrefixesCollapsed.Insert(formatKeyUncached(canonicalPrefix))
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

// resolvedKey is one configuration key seen under both identities it can take.
//
// lookup is the separator-free form every source agrees on, and is what values,
// sensitivity marks and immutability are keyed by. dotted is the form that
// carries the key's structure, and is the only one a prefix can be matched
// against — "kafkaproducercompression" cannot be tested against the prefix
// "kafka.producer.", which is why the two must travel together rather than be
// re-derived from whichever spelling a caller happened to use.
type resolvedKey struct {
	lookup string
	dotted string
	kind   RegisteredConfigKind
	// segmented records that dotted came from a declaration or from a source,
	// rather than from whoever asked. Only one rule in this file widens a
	// verdict — a NonSensitiveSuffixes exemption, which is granted to a leaf
	// name — and a leaf name is a property of the segmentation. So that rule
	// may only be applied to a segmentation nothing outside the process chose.
	segmented bool
}

// rememberSpelling records that one configuration key can be addressed under
// two identities, so a later lookup made under the collapsed one can be
// classified against the namespace it belongs to.
//
// Every source inserts a key twice — once as written, once collapsed — so the
// pairing is observable here without any source having to report it. Only
// ParamGroup members need this: a ParamItem records its own spelling in
// declaredKeys, which is consulted first and always wins.
//
// The environment is excluded on purpose. Its variable names are not a
// namespace, they are whatever the pod happens to carry, and letting one of
// them define the structure of a collapsed identity is exactly the
// impersonation groupMemberIsEnvironmentOnly exists to refuse.
func (m *Manager) rememberSpelling(key, sourceName string) {
	if sourceName == environmentSourceName {
		return
	}
	// "/" is what EtcdSource keeps when a key was stored with slashes; "." is
	// every other source. A key with neither is already the collapsed identity
	// and has nothing to teach.
	if !strings.ContainsAny(key, "./") {
		return
	}
	dotted := lowerKey(strings.ReplaceAll(key, "/", "."))
	collapsed := formatKey(key)
	if collapsed == dotted {
		// knowhere.*, which formatKey exempts from collapsing.
		return
	}
	// Two different keys can collapse to one identity ("a.bc" and "ab.c"), and
	// the order sources are walked in is a Go map iteration order. Letting the
	// last writer decide would make the classification of both of them depend on
	// it. Learn nothing instead, and keep learning nothing: with no spelling
	// recovered, the collapsed identity falls back to the collapsed prefixes,
	// which claim rather than exempt. TestDeclaredKeysDoNotCollide rules this
	// out among declared keys; group members are named by whoever wrote them,
	// so it cannot.
	m.spellingMutex.Lock()
	defer m.spellingMutex.Unlock()
	// Deliberately never unlearned, not even when the key is deleted: a learned
	// spelling only ever endorses a segmentation, and endorsing one for an
	// identity that no longer holds a value costs nothing, while dropping it
	// would mean tracking which of the key's two entries went away.
	//
	// A collision leaves that identity classified by its collapsed spelling
	// while the dotted one it was taught first may still be exempted — the very
	// state TestOneIdentityHasOneVerdict forbids, which that test cannot reach
	// because it never writes two keys that collide. It errs closed, and
	// producing one takes writing both spellings of a member yourself.
	if m.collidedSpellings.Contain(collapsed) {
		return
	}
	if previous, loaded := m.dottedSpellings.GetOrInsert(collapsed, dotted); loaded && previous != dotted {
		m.collidedSpellings.Insert(collapsed)
		m.dottedSpellings.Remove(collapsed)
	}
}

func (m *Manager) resolveRegisteredKey(key string) resolvedKey {
	dotted := lowerKey(strings.ReplaceAll(key, "/", "."))
	// Format the dotted form, not the caller's spelling: formatKey leaves keys
	// below NotFormatPrefix untouched, so "knowhere.X/y" would otherwise keep
	// its slashes and match nothing. Uncached, because this runs on
	// caller-supplied keys from an endpoint anyone can reach, and formatKey's
	// cache is a process-global map that never evicts.
	formattedKey := formatKeyUncached(dotted)
	// A declared ParamItem remembers its own dotted spelling, so it resolves the
	// same way whether the caller used the declared key, its environment alias,
	// or the separator-free identity the value is stored under.
	if declared, ok := m.declaredKeys.Get(formattedKey); ok {
		return resolvedKey{lookup: formattedKey, dotted: declared, kind: RegisteredConfigScalar, segmented: true}
	}

	// Nobody declared this key, so use the spelling the sources showed us for
	// this identity, whatever spelling the caller reached it by.
	//
	// Unconditionally, and that is the whole point. A key's identity is the
	// collapsed form — it is what values are stored under and what an etcd write
	// addresses — so where the separators fall within it is not the caller's to
	// decide. Believing a caller who supplies dots means believing their
	// segmentation, and every rule below reads the segments: a namespace prefix,
	// and the leaf name a NonSensitiveSuffixes exemption is granted to. So
	// "…providers.myprov.credential_url", a credential, could be asked for as
	// "…providers.myprovcredential.url", which is the same stored entry with its
	// leaf renamed to one the group declared safe, and it came back in the clear.
	//
	// Recovery cannot widen anything, because it only fires when the two
	// spellings collapse to the same identity — which is to say, when they are
	// the same entry. It can only put the segments back where the source had
	// them.
	//
	// When nothing was learned — a member that exists only in etcd, which is
	// what an alter-endpoint write leaves behind, or an identity two keys
	// collide on — the caller's segmentation is all there is, and it is then
	// marked unendorsed rather than trusted. See resolvedKey.segmented: an
	// unendorsed segmentation may still name a namespace, because naming one
	// only ever narrows, but it may not claim a suffix exemption.
	segmented := false
	if learned, ok := m.dottedSpellings.Get(formattedKey); ok {
		dotted = learned
		segmented = true
	} else if m.isStoredKey(dotted) {
		// No pairing learned, but a source stored the key under exactly this
		// spelling, so the segmentation is still not the caller's invention.
		segmented = true
	}

	resolved := resolvedKey{lookup: formattedKey, dotted: dotted, segmented: segmented}
	var environmentOnly, environmentBacked *bool
	lazily := func(cached **bool, compute func() bool) bool {
		if *cached == nil {
			verdict := compute()
			*cached = &verdict
		}
		return **cached
	}
	m.registeredKeyPrefixes.Range(func(prefix, collapsedPrefix string) bool {
		switch {
		case hasNamespacePrefix(dotted, prefix):
			// The key names the namespace explicitly, separators and all. Only
			// the environment can produce such a name without meaning it — see
			// groupMemberIsEnvironmentOnly.
			if lazily(&environmentOnly, func() bool {
				return m.groupMemberIsEnvironmentOnly(dotted, formattedKey)
			}) {
				return true
			}
		case !strings.Contains(dotted, ".") && hasNamespacePrefix(formattedKey, collapsedPrefix):
			// Separators already collapsed, so the namespace can only be matched
			// fuzzily — "tlsclustersprodcapempath" against "tlsclusters". That is
			// how AlterConfigsInEtcd stores a member, and how FileSource and
			// EnvSource store their aliases, so the identity has to be accepted
			// or a key written through the alter endpoint becomes invisible to
			// the very projections this file exists to make truthful.
			//
			// Only for a key that has no dots left. A half-collapsed spelling
			// — "kafkaconsumerssl.key.pem", which matches the collapsed prefix
			// while matching no dotted one — names no namespace anybody
			// declared, and the caller who wrote it is the only reason it looks
			// like it does. Such a key is refused rather than admitted as a
			// member of a namespace it only resembles.
			//
			// Sensitivity does not rest on this: matchSensitivePrefix consults
			// the collapsed prefixes unconditionally, so the same spelling
			// classifies sensitive whether or not it is admitted here. Two
			// independent reasons, which is the point — this predicate is about
			// what may be named, not about what may be read.
			//
			// Fuzzy matching is only safe on sources that hold nothing but
			// Milvus configuration. The environment holds everything in the pod,
			// and a collapsed name cannot be checked against the namespace's
			// structure, so an environment-backed identity is not accepted here.
			if lazily(&environmentBacked, func() bool {
				source, ok := m.keySourceMap.Get(formattedKey)
				return ok && source == environmentSourceName
			}) {
				return true
			}
		default:
			return true
		}
		resolved.kind = RegisteredConfigGroup
		return false
	})
	return resolved
}

// hasNamespacePrefix reports whether key names something strictly below prefix.
// An empty prefix covers every non-empty key, which is what a ParamGroup with
// no KeyPrefix declares.
func hasNamespacePrefix(key, prefix string) bool {
	return strings.HasPrefix(key, prefix) && len(key) > len(prefix)
}

// IsSensitive reports whether key is a credential. It accepts any spelling of
// the key — declared, environment alias, or separator-free — because it
// resolves the key first rather than matching prefixes against whatever form
// the caller passed.
func (m *Manager) IsSensitive(key string) bool {
	resolved := m.resolveRegisteredKey(key)
	return m.isSensitiveResolved(resolved.lookup, resolved.dotted, resolved.segmented)
}

// isSensitiveResolved decides sensitivity from both identities of one key.
//
// Precedence is explicit-before-inferred, and it matters in both directions: a
// declared NonSensitive ParamItem that happens to sit below a sensitive
// ParamGroup prefix (kafka.producer.message.max.bytes) must stay readable, and
// a declared Sensitive key must stay hidden whatever its name looks like.
func (m *Manager) isSensitiveResolved(lookup, dotted string, segmented bool) bool {
	if m.sensitiveKeys.Contain(lookup) {
		return true
	}
	if m.nonSensitiveKeys.Contain(lookup) {
		return false
	}

	// Lower-cased here rather than upstream: lowerKey deliberately leaves
	// NotFormatPrefix keys alone so knowhere index parameters keep the case
	// their engine needs. Every rule below matches against lower-case literals,
	// so without this "knowhere.apiKey" would classify differently from
	// "knowhere.apikey" — the exact four-spellings-of-one-key problem this file
	// exists to remove, reintroduced in the one namespace that legitimately
	// uses mixed case.
	dotted = strings.ToLower(dotted)

	switch m.matchSensitivePrefix(dotted, lookup, segmented) {
	case prefixSensitive:
		return true
	case prefixExempted:
		// A declared suffix exemption is explicit metadata, so it wins over the
		// name-pattern guess below. This is load-bearing today, not a provision
		// for later: the patterns match anywhere in the key, so a provider whose
		// own name contains one — "…providers.mycredential.url" — would be
		// classified sensitive by the fallback despite ending in a leaf the
		// group declared safe. TestNonSensitiveSuffixExemption pins it.
		return false
	}

	patternKey := sensitivePatternReplacer.Replace(dotted)
	for _, pattern := range sensitiveKeyPatterns {
		if strings.Contains(patternKey, pattern) {
			return true
		}
	}
	return false
}

// prefixVerdict is what the registered sensitive prefixes say about one key.
type prefixVerdict int

const (
	// prefixNoMatch: no sensitive prefix covers the key.
	prefixNoMatch prefixVerdict = iota
	// prefixSensitive: a sensitive prefix covers it and nothing exempts it.
	prefixSensitive
	// prefixExempted: every sensitive prefix that covers it declared this leaf
	// safe.
	prefixExempted
)

// matchSensitivePrefix classifies a key against the registered sensitive
// prefixes, using the dotted identity whenever there is one.
//
// resolveRegisteredKey recovers the dotted spelling for a collapsed key
// whenever some source has shown the manager both, so the common case —
// "kafkaproducersslkeypem", the alias FileSource inserts beside
// "kafka.producer.ssl.key.pem" — arrives here already carrying its structure,
// and gets the same verdict, exemptions included, as the spelling it is an
// alias of.
//
// What is left is a key whose structure nothing in the process knows: a member
// created through /management/config/alter, under a namespace whose members are
// open-ended by definition. There the group's Sensitive default has to stand,
// and it stands two ways. A name with no separators left in it is matched
// against the collapsed prefixes. A name the caller supplied separators for is
// matched against the dotted prefixes as usual, but arrives unendorsed, so the
// exemption below is withheld: "…zilliz.secret_enable" and
// "…zilliz.secret.enable" are one identity, and letting the caller pick which
// of them to send would let them pick the leaf name the exemption is granted
// to, and with it the verdict.
func (m *Manager) matchSensitivePrefix(dotted, lookup string, segmented bool) prefixVerdict {
	verdict := prefixNoMatch
	m.sensitiveKeyPrefixes.Range(func(prefix string) bool {
		if !strings.HasPrefix(dotted, prefix) {
			return true
		}
		if segmented && m.suffixExempted(prefix, dotted[len(prefix):]) {
			// Exempted for this prefix, but a longer sensitive prefix may still
			// cover the key without exempting it, so keep scanning.
			verdict = prefixExempted
			return true
		}
		verdict = prefixSensitive
		return false
	})
	if verdict != prefixNoMatch {
		return verdict
	}

	// Nothing claimed the dotted identity. Try the collapsed one unconditionally
	// rather than only when the key looks structureless: this is the last thing
	// standing between a namespace declared sensitive and a spelling nobody
	// anticipated, so it errs towards claiming too much. Over-claiming here
	// costs an operator a value they can read under the key's real spelling;
	// under-claiming costs them a private key.
	m.sensitiveKeyPrefixesCollapsed.Range(func(prefix string) bool {
		if strings.HasPrefix(lookup, prefix) {
			verdict = prefixSensitive
			return false
		}
		return true
	})
	return verdict
}

// suffixExempted reports whether the part of a key below a sensitive prefix is
// one of the leaves the group declared safe. Callers must have established that
// the segmentation is endorsed — see resolvedKey.segmented — because this is the
// one rule in this file that turns a sensitive verdict into a readable one.
//
// The exemption is deliberately shallow: it covers "<provider>.enable" and the
// group's own "enable", but not "<provider>.secret.enable". A group is marked
// sensitive precisely because its member names are open-ended, so an exemption
// that matched the last segment at any depth would let an arbitrary subtree out
// by ending in a safe-looking word.
func (m *Manager) suffixExempted(prefix, remainder string) bool {
	if strings.Count(remainder, ".") > 1 {
		return false
	}
	return m.nonSensitiveSuffixes.Contain(suffixExemption(prefix, leafName(remainder)))
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

// overlayIsAuthoritative reports whether a runtime overlay stored under this
// exact spelling is the one its consumer reads. A ParamItem is resolved only
// through Manager.GetConfig, which looks under the separator-free identity, so
// an overlay written under the dotted spelling is read by nothing and a
// projection that named it would be advertising a value nothing uses.
func (m *Manager) overlayIsAuthoritative(storedKey string) bool {
	resolved := m.resolveRegisteredKey(storedKey)
	switch resolved.kind {
	case RegisteredConfigScalar:
		return storedKey == resolved.lookup
	case RegisteredConfigGroup:
		// Either spelling may be the live one: a ParamGroup aggregate reads the
		// dotted prefix, while a caller that builds the key itself — the
		// per-cluster CDC settings in grpc_param.go — goes through
		// Manager.GetConfig and reads the separator-free identity. Which
		// consumer a dynamic namespace has is not something this package knows.
		return true
	default:
		return false
	}
}

func (m *Manager) classify(key string) projectionKind {
	resolved := m.resolveRegisteredKey(key)
	switch {
	case resolved.kind == RegisteredConfigUnknown:
		return projectionOmit
	case m.isSensitiveResolved(resolved.lookup, resolved.dotted, resolved.segmented):
		return projectionRedact
	default:
		return projectionKeep
	}
}

// RedactValue returns a log-safe value for key. Undeclared keys fail closed:
// a log line names its own key, so masking a value costs nothing there, whereas
// a configuration projection omits the entry instead — see projectionOmit.
func (m *Manager) RedactValue(key, value string) string {
	if m.classify(key) != projectionKeep {
		return RedactedValue
	}
	return value
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
