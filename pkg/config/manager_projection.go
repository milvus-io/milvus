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

import "fmt"

// projectionKind is what a configuration projection does with one key.
type projectionKind int

const (
	// projectionKeep emits the original value.
	projectionKeep projectionKind = iota
	// projectionRedact emits RedactedValue: the key is declared Milvus
	// configuration, so naming it is fine, but its value is sensitive.
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
	case m.isSensitiveResolved(resolved):
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

// GetConfigs returns the original, unredacted values held by the manager. This
// preserves the historical exported API, including its runtime tombstones, for
// internal and out-of-tree consumers. Code that exposes configuration outside
// the process must call ProjectConfigs instead.
func (m *Manager) GetConfigs() map[string]string {
	return m.getConfigs(false)
}

// ProjectConfigs returns a safe projection of all key values: sensitive values
// are replaced by RedactedValue, and keys that no ParamItem or ParamGroup
// declares are omitted entirely.
//
// The projection is only as complete as the declarations made so far. A Manager
// whose ParamItems have not been initialized yet declares nothing, so this
// returns an empty map rather than an error. Call it after the owning ParamTable
// is built.
func (m *Manager) ProjectConfigs() map[string]string {
	return m.getConfigs(true)
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
// includeInertOverlays is for the raw getters: an overlay written under the
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
			if includeInertOverlays {
				// Raw getters historically expose the manager's tombstone. Safe
				// projections omit it because it records deletion, not a literal
				// configuration value.
				emit(key, storedKey, value, RuntimeSource)
			}
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
			// Keep the annotation: which source supplies a sensitive value is not
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

// GetBy returns matching original, unredacted values held by the manager,
// preserving its historical exported API, including runtime tombstones. Code
// that exposes the result outside the process must call ProjectBy instead.
func (m *Manager) GetBy(filters ...Filter) map[string]string {
	return m.getBy(false, true, filters...)
}

// GetEffectiveBy returns matching original, unredacted values that are in
// force. Unlike GetBy it omits tombstones and overlay spellings no config
// consumer reads. Internal consumers such as ParamGroup use this view: a
// tombstone is the manager's representation of deletion, never a config value.
func (m *Manager) GetEffectiveBy(filters ...Filter) map[string]string {
	return m.getBy(false, false, filters...)
}

// ProjectBy returns a safe projection of the matching values.
func (m *Manager) ProjectBy(filters ...Filter) map[string]string {
	return m.getBy(true, false, filters...)
}

func (m *Manager) getBy(redact, includeInertOverlays bool, filters ...Filter) map[string]string {
	matchedConfig := make(map[string]string)
	// filterate, not everyKey: GetBy() with no filters has always matched
	// nothing, and this is an exported API.
	accept := func(key string) (string, bool) { return filterate(key, filters...) }
	m.walkProjection(includeInertOverlays, accept, func(key, storedKey, value, _ string) {
		if projected, include := m.projectValue(storedKey, value, redact); include {
			matchedConfig[key] = projected
		}
	})
	return matchedConfig
}

// FileConfigs returns the original file-source values, preserving its
// historical exported API contract.
func (m *Manager) FileConfigs() map[string]string {
	return m.fileConfigs(false)
}

// ProjectFileConfigs returns a safe projection of the file-source values.
func (m *Manager) ProjectFileConfigs() map[string]string {
	return m.fileConfigs(true)
}

func (m *Manager) fileConfigs(redact bool) map[string]string {
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
		if value, include := m.projectValue(key, value, redact); include {
			projected[key] = value
		}
	}
	return projected
}
