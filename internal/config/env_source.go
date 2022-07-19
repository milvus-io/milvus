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
	"os"
	"strings"
	"sync"
)

type EnvSource struct {
	configs      sync.Map
	KeyFormatter func(string) string
}

func NewEnvSource(KeyFormatter func(string) string) EnvSource {
	es := EnvSource{
		configs:      sync.Map{},
		KeyFormatter: KeyFormatter,
	}
	for _, value := range os.Environ() {
		rs := []rune(value)
		in := strings.Index(value, "=")
		key := string(rs[0:in])
		value := string(rs[in+1:])
		envKey := KeyFormatter(key)
		es.configs.Store(key, value)
		es.configs.Store(envKey, value)

	}
	return es
}

// GetConfigurationByKey implements ConfigSource
func (es EnvSource) GetConfigurationByKey(key string) (string, error) {
	value, ok := es.configs.Load(key)
	if !ok {
		return "", fmt.Errorf("key not found: %s", key)
	}

	return value.(string), nil
}

// GetConfigurations implements ConfigSource
func (es EnvSource) GetConfigurations() (map[string]string, error) {
	configMap := make(map[string]string)
	es.configs.Range(func(k, v interface{}) bool {
		configMap[k.(string)] = v.(string)
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

func (es EnvSource) Close() {

}
