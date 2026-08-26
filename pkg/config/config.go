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
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/spf13/cast"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
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
			log.Fatal("failed to add FileSource config", mlog.Err(err))
		}
	}
	if o.EnvKeyFormatter != nil {
		sourceManager.AddSource(NewEnvSource(o.EnvKeyFormatter))
	}
	if o.EtcdInfo != nil {
		s, err := NewEtcdSource(o.EtcdInfo)
		if err != nil {
			return nil, err
		}
		sourceManager.AddSource(s)
	}
	return sourceManager, nil
}

var formattedKeys = typeutil.NewConcurrentMap[string, string]()

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

func formatKey(key string) string {
	if strings.HasPrefix(key, NotFormatPrefix) {
		return key
	}
	cached, ok := formattedKeys.Get(key)
	if ok {
		return cached
	}
	result := keyFormatReplacer.Replace(strings.ToLower(key))
	formattedKeys.Insert(key, result)
	return result
}

// formatKeyUncached is formatKey without the memo. Use it for keys that arrive
// from outside the process: formattedKeys is global and unbounded, so caching
// arbitrary caller input would let a request grow it without limit.
func formatKeyUncached(key string) string {
	if strings.HasPrefix(key, NotFormatPrefix) {
		return key
	}
	return keyFormatReplacer.Replace(strings.ToLower(key))
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
	return keyFormatReplacer.Replace(strings.ToLower(key))
}

func flattenAndMergeMap(prefix string, m map[string]interface{}, result map[string]string) {
	for k, v := range m {
		fullKey := k
		if prefix != "" {
			fullKey = prefix + "." + k
		}

		switch val := v.(type) {
		case map[string]interface{}:
			flattenAndMergeMap(fullKey, val, result)
		case map[interface{}]interface{}:
			flattenAndMergeMap(fullKey, cast.ToStringMap(val), result)
		case []interface{}:
			// Check if array contains complex types (maps/structs)
			isComplexArray := false
			for _, item := range val {
				switch item.(type) {
				case map[string]interface{}, map[interface{}]interface{}:
					isComplexArray = true
				}
				if isComplexArray {
					break
				}
			}

			var str string
			if isComplexArray {
				// For complex arrays (containing objects), convert to JSON-compatible format and serialize
				jsonCompatible := convertToJSONCompatible(val)
				jsonBytes, err := json.Marshal(jsonCompatible)
				if err != nil {
					fmt.Printf("marshal to json failed %s, error = %s\n", fullKey, err.Error())
					continue
				}
				str = string(jsonBytes)
			} else {
				// For simple arrays, use comma-separated values
				for i, item := range val {
					itemStr, err := cast.ToStringE(item)
					if err != nil {
						continue
					}
					if i == 0 {
						str = itemStr
					} else {
						str = str + "," + itemStr
					}
				}
			}
			result[lowerKey(fullKey)] = str
			result[formatKey(fullKey)] = str
		default:
			str, err := cast.ToStringE(val)
			if err != nil {
				fmt.Printf("cast to string failed %s, error = %s\n", fullKey, err.Error())
				continue
			}
			result[lowerKey(fullKey)] = str
			result[formatKey(fullKey)] = str
		}
	}
}

// convertToJSONCompatible converts map[interface{}]interface{} to map[string]interface{}
// recursively to make it compatible with JSON marshaling
func convertToJSONCompatible(v interface{}) interface{} {
	switch val := v.(type) {
	case map[interface{}]interface{}:
		result := make(map[string]interface{})
		for k, v := range val {
			keyStr, err := cast.ToStringE(k)
			if err != nil {
				continue
			}
			result[keyStr] = convertToJSONCompatible(v)
		}
		return result
	case map[string]interface{}:
		result := make(map[string]interface{})
		for k, v := range val {
			result[k] = convertToJSONCompatible(v)
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(val))
		for i, item := range val {
			result[i] = convertToJSONCompatible(item)
		}
		return result
	default:
		return v
	}
}
