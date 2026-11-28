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
	"encoding/json"
	"os"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/spf13/cast"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var (
	ErrNotInitial   = errors.New("config is not initialized")
	ErrIgnoreChange = errors.New("ignore change")
	ErrKeyNotFound  = errors.New("key not found")

	// ErrKeyUnregistered marks a key that no ParamItem or ParamGroup declares.
	// Config sources carry more than Milvus configuration — EnvSource imports
	// the whole process environment — so an undeclared key is not something a
	// caller-supplied lookup may reach.
	ErrKeyUnregistered = errors.New("unregistered config key")
	// ErrKeySensitive marks a declared key whose value carries a credential or
	// protected infrastructure topology.
	ErrKeySensitive = errors.New("sensitive config key")

	// config source management
	ErrSourceDuplicate = errors.New("duplicate config source")
	ErrSourceInvalid   = errors.New("invalid config source or source not added")

	// etcd config read/write
	ErrEtcdClientUnavailable     = errors.New("etcd client is not available")
	ErrImmutableConfigSaveFailed = errors.New("failed to save immutable configs to etcd")
	ErrNoConfigsToAlter          = errors.New("no configs to alter")

	// config file parsing
	ErrUnsupportedConfigType  = errors.New("unsupported config file type")
	ErrAllConfigFilesNotExist = errors.New("all config files not exist")
)

const (
	NotFormatPrefix = "knowhere."
)

func Init(opts ...Option) (*Manager, error) {
	o := &Options{}
	for _, opt := range opts {
		opt(o)
	}
	sourceManager := NewManager()
	if o.FileInfo != nil {
		s := NewFileSource(o.FileInfo)
		err := sourceManager.AddSource(s)
		if err != nil {
			// Parser errors can quote configuration values. Keep the original
			// process-exit behavior independently of the logger's fatal hook.
			mlog.Error(context.TODO(), "failed to add FileSource config", mlog.String("error", RedactedValue))
			os.Exit(1)
		}
	}
	if o.EnvKeyFormatter != nil {
		sourceManager.AddSource(NewEnvSource(o.EnvKeyFormatter))
	}
	if o.EtcdInfo != nil {
		etcdCli, err := newEtcdClient(o.EtcdInfo)
		if err != nil {
			return nil, err
		}
		s, err := NewEtcdSource(etcdCli, o.EtcdInfo)
		if err != nil {
			return nil, err
		}
		sourceManager.AddSource(s)
	}
	return sourceManager, nil
}

var (
	formattedKeys   = typeutil.NewConcurrentMap[string, string]()
	formattedKeysMu sync.Mutex
)

// Four spellings of one configuration key travel through this package, and
// picking the wrong one is how a check ends up guarding a name nothing uses:
//
//	lowerKey          "Kafka.SSL.tlsKey" -> "kafka.ssl.tlskey"   (case only)
//	formatKey         "Kafka.SSL.tlsKey" -> "kafkassltlskey"     (memoised; internal keys only)
//	formatKeyUncached same as formatKey, no memo                 (caller-supplied keys)
//	strippedKey       same, without the NotFormatPrefix guard    (what EnvSource produces)
//
// lowerKey and formatKey both leave NotFormatPrefix ("knowhere.") keys exactly
// as they are, because the index engine needs the case and the dots; strippedKey
// is the one that does not, which is why the two disagree there and only there.
// Values are stored under formatKey's identity, so that is what a lookup must
// use; prefixes are declared with dots, so that is what a namespace test must
// use.
func lowerKey(key string) string {
	if strings.HasPrefix(key, NotFormatPrefix) {
		return key
	}
	return strings.ToLower(key)
}

var keyFormatReplacer = strings.NewReplacer("/", "", "_", "", ".", "")

// FormatKey is the identity a config key is stored and looked up under.
// Callers that guard a specific key must compare against this rather than a
// hand-rolled normalization: separators are stripped, not translated, so
// "a.b.c", "a_b_c", "a/b/c" and "abc" are all the same key.
func FormatKey(key string) string { return formatKey(key) }

// maxFormattedKeys bounds the normalization memo. The cache exists for the
// fixed config vocabulary, which is a few hundred keys and is resolved on every
// ParamItem read -- but /management/config/get and /management/config/alter
// normalize caller-supplied keys, and both answer anonymously while
// common.security.adminAuthEnabled is off. An unbounded memo therefore lets
// anyone who can reach the metrics port grow it without limit. Past the bound
// the result is still correct, it is just recomputed. Bound both stored strings
// to 1 KiB as well: a count limit alone still permits huge request keys to pin
// gigabytes, and Unicode lowercasing can grow a normalized string.
const (
	maxFormattedKeys     = 4096
	maxFormattedKeyBytes = 1024
)

func formatKey(key string) string {
	if strings.HasPrefix(key, NotFormatPrefix) {
		return key
	}
	if len(key) > maxFormattedKeyBytes {
		return normalizeKey(key)
	}
	cached, ok := formattedKeys.Get(key)
	if ok {
		return cached
	}
	result := normalizeKey(key)
	if len(result) > maxFormattedKeyBytes {
		return result
	}
	formattedKeysMu.Lock()
	defer formattedKeysMu.Unlock()
	// A concurrent miss may have populated this key while this goroutine waited.
	if cached, ok := formattedKeys.Get(key); ok {
		return cached
	}
	if formattedKeys.Len() < maxFormattedKeys {
		// A short query key can be a substring of a much larger HTTP request.
		// Own the cached bytes so the byte limits also bound retained memory.
		formattedKeys.Insert(strings.Clone(key), strings.Clone(result))
	}
	return result
}

// normalizeKey is the normalization itself, split out so the memoized and
// unmemoized paths cannot drift.
func normalizeKey(key string) string {
	return keyFormatReplacer.Replace(strings.ToLower(key))
}

// formatKeyUncached is formatKey without the memo. Use it for caller-supplied
// projection keys so diagnostic reads do not populate the bounded cache used
// by runtime lookups.
func formatKeyUncached(key string) string {
	if strings.HasPrefix(key, NotFormatPrefix) {
		return key
	}
	return normalizeKey(key)
}

// strippedKey collapses a key with no NotFormatPrefix exemption at all.
//
// formatKey deliberately leaves knowhere.* alone, but the EnvSource key
// formatter that BaseTable installs does not — it strips every separator
// unconditionally. So the two disagree exactly on knowhere.*, and any check
// that asks "did the environment supply this key?" has to look under this
// spelling too, or an environment variable named KNOWHERE.SOMETHING is invisible
// to it.
func strippedKey(key string) string {
	return normalizeKey(key)
}

func flattenAndMergeMap(prefix string, m map[string]interface{}, result map[string]string, seen typeutil.Set[string]) error {
	for k, v := range m {
		fullKey := k
		if prefix != "" {
			fullKey = prefix + "." + k
		}

		if child, ok := v.(map[interface{}]interface{}); ok {
			v = cast.ToStringMap(child)
		}
		if child, ok := v.(map[string]any); ok {
			if err := flattenAndMergeMap(fullKey, child, result, seen); err != nil {
				return err
			}
			continue
		}
		str, err := configValueString(v, seen != nil)
		if err != nil {
			if seen != nil {
				return merr.WrapErrParameterInvalidMsg("cannot convert value for configuration key %q", fullKey)
			}
			mlog.Warn(context.TODO(), "cannot convert configuration value", mlog.String("error", RedactedValue))
			continue
		}
		spelling, identity := lowerKey(fullKey), formatKey(fullKey)
		if seen != nil {
			if seen.Contain(spelling) || seen.Contain(identity) {
				return merr.WrapErrParameterInvalidMsg("conflicting configuration key %q", fullKey)
			}
			seen.Insert(spelling, identity)
		}
		result[spelling] = str
		result[identity] = str
	}
	return nil
}

// Lists of objects are represented as JSON. Other lists use comma-separated scalars.
func configValueString(value any, strict bool) (string, error) {
	items, ok := value.([]any)
	if !ok {
		return cast.ToStringE(value)
	}
	for _, item := range items {
		switch item.(type) {
		case map[string]any, map[any]any:
			objects, err := convertToJSONCompatible(items, strict)
			if err != nil {
				return "", err
			}
			data, err := json.Marshal(objects)
			return string(data), err
		}
	}
	var result strings.Builder
	for i, item := range items {
		str, err := cast.ToStringE(item)
		if err != nil {
			if strict {
				return "", err
			}
			continue
		}
		if i > 0 {
			result.WriteByte(',')
		}
		result.WriteString(str)
	}
	return result.String(), nil
}

// Normalize keys in the decoded YAML tree before flattening or JSON encoding.
func convertToJSONCompatible(v any, strict bool) (any, error) {
	switch val := v.(type) {
	case map[any]any:
		result := make(map[string]any, len(val))
		for k, item := range val {
			key, err := cast.ToStringE(k)
			if err != nil {
				if strict {
					return nil, merr.WrapErrParameterInvalidMsg("cannot convert YAML key to string")
				}
				continue
			}
			// Merges may bring differently typed keys into the same mapping.
			if _, exists := result[key]; strict && exists {
				return nil, merr.WrapErrParameterInvalidMsg("multiple YAML keys resolve to %q", key)
			}
			result[key], err = convertToJSONCompatible(item, strict)
			if err != nil {
				return nil, err
			}
		}
		return result, nil
	case map[string]any:
		result := make(map[string]any, len(val))
		for k, item := range val {
			converted, err := convertToJSONCompatible(item, strict)
			if err != nil {
				return nil, err
			}
			result[k] = converted
		}
		return result, nil
	case []any:
		result := make([]any, len(val))
		for i, item := range val {
			converted, err := convertToJSONCompatible(item, strict)
			if err != nil {
				return nil, err
			}
			result[i] = converted
		}
		return result, nil
	}
	return v, nil
}
