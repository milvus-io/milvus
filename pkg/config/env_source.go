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
	"os"
	"strings"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type EnvSource struct {
	configs      *typeutil.ConcurrentMap[string, string]
	KeyFormatter func(string) string
}

func NewEnvSource(KeyFormatter func(string) string) EnvSource {
	es := EnvSource{
		configs:      typeutil.NewConcurrentMap[string, string](),
		KeyFormatter: KeyFormatter,
	}

	for _, value := range os.Environ() {
		rs := []rune(value)
		in := strings.Index(value, "=")
		key := string(rs[0:in])
		value := string(rs[in+1:])
		envKey := KeyFormatter(key)
		es.configs.Insert(key, value)
		es.configs.Insert(envKey, value)
	}
	return es
}

// GetConfigurationByKey implements ConfigSource
func (es EnvSource) GetConfigurationByKey(key string) (string, error) {
	value, ok := es.configs.Get(key)

	if !ok {
		return "", errors.Wrap(ErrKeyNotFound, key) // fmt.Errorf("key not found: %s", key)
	}

	return value, nil
}

// GetConfigurations implements ConfigSource
func (es EnvSource) GetConfigurations() (map[string]string, error) {
	configMap := make(map[string]string)
	es.configs.Range(func(k, v string) bool {
		configMap[k] = v
		return true
	})

	return configMap, nil
}

// GetPriority implements ConfigSource
func (es EnvSource) GetPriority() int {
	return NormalPriority
}

// GetSourceName implements ConfigSource
func (es EnvSource) GetSourceName() string {
	return "EnvironmentSource"
}

func (es EnvSource) SetManager(m ConfigManager) {
}

func (es EnvSource) SetEventHandler(eh EventHandler) {
}

func (es EnvSource) UpdateOptions(opts Options) {
}

func (es EnvSource) Close() {
}
