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
	"strings"

	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v3"

	"github.com/milvus-io/milvus/pkg/util/typeutil"
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
		sourceManager.AddSource(s)
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

func flattenNode(node *yaml.Node, parentKey string, result map[string]string) {
	// The content of the node should contain key-value pairs in a MappingNode
	if node.Kind == yaml.MappingNode {
		for i := 0; i < len(node.Content); i += 2 {
			keyNode := node.Content[i]
			valueNode := node.Content[i+1]

			key := keyNode.Value
			// Construct the full key with parent hierarchy
			fullKey := key
			if parentKey != "" {
				fullKey = parentKey + "." + key
			}

			switch valueNode.Kind {
			case yaml.ScalarNode:
				// Scalar value, store it as a string
				result[lowerKey(fullKey)] = valueNode.Value
				result[formatKey(fullKey)] = valueNode.Value
			case yaml.MappingNode:
				// Nested map, process recursively
				flattenNode(valueNode, fullKey, result)
			case yaml.SequenceNode:
				// List (sequence), process elements
				var listStr string
				for j, item := range valueNode.Content {
					if j > 0 {
						listStr += ","
					}
					if item.Kind == yaml.ScalarNode {
						listStr += item.Value
					}
				}
				result[lowerKey(fullKey)] = listStr
				result[formatKey(fullKey)] = listStr
			}
		}
	}
}
