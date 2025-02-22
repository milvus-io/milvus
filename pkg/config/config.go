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
	"fmt"
	"log"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/spf13/cast"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var (
	ErrNotInitial   = errors.New("config is not initialized")
	ErrIgnoreChange = errors.New("ignore change")
	ErrKeyNotFound  = errors.New("key not found")
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
			log.Fatal("failed to add FileSource config", zap.Error(err))
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

func lowerKey(key string) string {
	if strings.HasPrefix(key, NotFormatPrefix) {
		return key
	}
	return strings.ToLower(key)
}

func formatKey(key string) string {
	if strings.HasPrefix(key, NotFormatPrefix) {
		return key
	}
	cached, ok := formattedKeys.Get(key)
	if ok {
		return cached
	}
	result := strings.NewReplacer("/", "", "_", "", ".", "").Replace(strings.ToLower(key))
	formattedKeys.Insert(key, result)
	return result
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
			str := ""
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
