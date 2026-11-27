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
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"github.com/spf13/cast"
	"gopkg.in/yaml.v2"
	yamlv3 "gopkg.in/yaml.v3"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// TestConfigFile is the shared CI overlay. Duplicate keys in this file would
// silently discard test settings, so decode it strictly on every load.
const TestConfigFile = "_test.yaml"

type FileSource struct {
	sync.RWMutex
	files   []string
	configs map[string]string

	updateMu        sync.Mutex
	configRefresher *refresher
	manager         ConfigManager
}

func NewFileSource(fileInfo *FileInfo) *FileSource {
	fs := &FileSource{
		files:   fileInfo.Files,
		configs: make(map[string]string),
	}
	fs.configRefresher = newRefresher(fileInfo.RefreshInterval, fs.loadFromFile)
	return fs
}

// GetConfigurationByKey implements ConfigSource
func (fs *FileSource) GetConfigurationByKey(key string) (string, error) {
	fs.RLock()
	v, ok := fs.configs[key]
	fs.RUnlock()
	if !ok {
		return "", errors.Wrap(ErrKeyNotFound, key) // fmt.Errorf("key not found: %s", key)
	}
	return v, nil
}

// GetConfigurations implements ConfigSource
func (fs *FileSource) GetConfigurations() (map[string]string, error) {
	configMap := make(map[string]string)

	err := fs.loadFromFile()
	if err != nil {
		return nil, err
	}

	fs.configRefresher.start(fs.GetSourceName())

	fs.RLock()
	for k, v := range fs.configs {
		configMap[k] = v
	}
	fs.RUnlock()
	return configMap, nil
}

// GetPriority implements ConfigSource
func (fs *FileSource) GetPriority() int {
	return LowPriority
}

// GetSourceName implements ConfigSource
func (fs *FileSource) GetSourceName() string {
	return "FileSource"
}

func (fs *FileSource) Close() {
	fs.configRefresher.stop()
}

func (fs *FileSource) SetManager(m ConfigManager) {
	fs.Lock()
	defer fs.Unlock()
	fs.manager = m
}

func (fs *FileSource) SetEventHandler(eh EventHandler) {
	fs.Lock()
	defer fs.Unlock()
	fs.configRefresher.SetEventHandler(eh)
}

func (fs *FileSource) UpdateOptions(opts Options) {
	if opts.FileInfo == nil {
		return
	}

	fs.Lock()
	defer fs.Unlock()
	fs.files = opts.FileInfo.Files
}

func (fs *FileSource) loadFromFile() error {
	newConfig := make(map[string]string)
	var configFiles []string

	fs.RLock()
	configFiles = fs.files
	fs.RUnlock()

	notExistsNum := 0
	for _, configFile := range configFiles {
		if _, err := os.Stat(configFile); err != nil {
			if os.IsNotExist(err) {
				notExistsNum++
				continue
			}
			return err
		}

		ext := filepath.Ext(configFile)
		if len(ext) == 0 || (ext[1:] != "yaml" && ext[1:] != "yml") {
			return errors.Wrapf(ErrUnsupportedConfigType, "%s", ext)
		}

		data, err := os.ReadFile(configFile)
		if err != nil {
			return errors.Wrapf(err, "Read config failed: %s", configFile)
		}

		if err := mergeYAML(configFile, data, newConfig); err != nil {
			return merr.Wrapf(err, "load YAML file %s", configFile)
		}
	}
	// not allow all config files missing, return error for this case
	if notExistsNum == len(configFiles) {
		return errors.Wrapf(ErrAllConfigFilesNotExist, "files: %v", configFiles)
	}

	return fs.update(newConfig)
}

func mergeYAML(file string, data []byte, result map[string]string) error {
	var values map[string]any
	if filepath.Base(file) != TestConfigFile {
		if err := yaml.Unmarshal(data, &values); err != nil {
			return err
		}
		return flattenAndMergeMap("", values, result, nil)
	}

	values, err := DecodeTestConfig(data)
	if err != nil {
		return err
	}
	return flattenAndMergeMap("", values, result, typeutil.NewSet[string]())
}

// DecodeTestConfig reads the shared file, preserving its YAML types for the
// repository test that checks the feature overrides against Milvus parameters.
func DecodeTestConfig(data []byte) (map[string]any, error) {
	decoder := yamlv3.NewDecoder(bytes.NewReader(data))
	var doc, trailing yamlv3.Node
	if err := decoder.Decode(&doc); err != nil {
		if err == io.EOF { // Empty or comment-only test file.
			return nil, nil
		}
		return nil, err
	}
	switch err := decoder.Decode(&trailing); err {
	case nil:
		return nil, merr.WrapErrParameterInvalidMsg("expected a single YAML document")
	case io.EOF:
	default:
		return nil, err
	}
	root := doc.Content[0]
	// A document marker with no content produces an implicit empty null scalar.
	if root.Kind == yamlv3.ScalarNode && root.Tag == "!!null" && root.Value == "" && root.Style == 0 && root.Anchor == "" {
		return nil, nil
	}
	if root.Kind != yamlv3.MappingNode {
		return nil, merr.WrapErrParameterInvalidMsg("expected a configuration mapping at line %d", root.Line)
	}
	if err := validateTestNode(root, true); err != nil {
		return nil, err
	}

	// Use the deployment decoder for the values, including merge precedence.
	// The syntax tree above is only for checks that decoding into a map loses.
	var values map[string]any
	if err := yaml.Unmarshal(data, &values); err != nil {
		return nil, err
	}
	for key, value := range values {
		converted, err := convertToJSONCompatible(value, true)
		if err != nil {
			return nil, err
		}
		values[key] = converted
	}
	return values, nil
}

func validateTestNode(node *yamlv3.Node, topLevel bool) error {
	if node.Style&yamlv3.TaggedStyle != 0 && strings.HasPrefix(node.Tag, "!!") {
		kind := yamlv3.ScalarNode
		switch node.Tag {
		case "!!map", "!!set":
			kind = yamlv3.MappingNode
		case "!!seq", "!!omap", "!!pairs":
			kind = yamlv3.SequenceNode
		}
		if node.Kind != kind {
			return merr.WrapErrParameterInvalidMsg("YAML tag %s does not match the node kind at line %d", node.Tag, node.Line)
		}
	}

	var keys typeutil.Set[string]
	if node.Kind == yamlv3.MappingNode {
		keys = typeutil.NewSet[string]()
	}
	for i, child := range node.Content {
		// Walk definitions once. The deployment decoder validates merge sources
		// and resolves aliases when it reads the original document.
		if err := validateTestNode(child, false); err != nil {
			return err
		}
		if keys == nil || i%2 != 0 {
			continue
		}
		key, err := testConfigKey(child, topLevel)
		if err != nil {
			return err
		}
		if keys.Contain(key) {
			return merr.WrapErrParameterInvalidMsg("duplicate configuration key at line %d", child.Line)
		}
		keys.Insert(key)
	}
	return nil
}

func testConfigKey(node *yamlv3.Node, topLevel bool) (string, error) {
	key := node
	if key.Kind == yamlv3.AliasNode {
		key = key.Alias
	}
	if key.Kind != yamlv3.ScalarNode {
		return "", merr.WrapErrParameterInvalidMsg("expected a scalar configuration key at line %d", node.Line)
	}
	if key.Tag == "!!str" && (topLevel || key.Style != 0) {
		return key.Value, nil
	}

	// A top-level map has string keys. Nested maps resolve scalar types before
	// flattening. Use that decoder to check the spelling of keys such as on and 01.
	encoded, err := yamlv3.Marshal(key)
	if err != nil {
		return "", err
	}
	var value any
	if err := yaml.Unmarshal(encoded, &value); err != nil {
		return "", err
	}
	name := key.Value
	switch decoded := value.(type) {
	case string:
		name = decoded // Includes decoded binary strings.
	case nil:
		name = ""
	}
	if !topLevel && (value == nil || cast.ToString(value) != name) {
		return "", merr.WrapErrParameterInvalidMsg("quote the configuration key at line %d to preserve its spelling", node.Line)
	}
	return name, nil
}

// update souce config
// make sure only update changes configs
func (fs *FileSource) update(configs map[string]string) error {
	// make sure config not change when fire event
	fs.updateMu.Lock()
	defer fs.updateMu.Unlock()

	fs.RLock()
	manager := fs.manager
	fs.RUnlock()
	var events []*Event
	err := publishSourceSnapshot(manager, fs.GetSourceName(), configs, func() error {
		fs.Lock()
		defer fs.Unlock()
		var err error
		events, err = PopulateEvents(fs.GetSourceName(), fs.configs, configs)
		if err != nil {
			return err
		}
		fs.configs = configs
		return nil
	})
	if err != nil {
		mlog.Warn(context.TODO(), "generating event error", mlog.Err(err))
		return err
	}
	// Cache eviction and callbacks may read configuration; keep them outside
	// both the source lock and the manager snapshot publication section.
	if manager != nil {
		manager.EvictCacheValueByFormat(lo.Map(events, func(event *Event, _ int) string { return event.Key })...)
	}

	fs.configRefresher.fireEvents(events...)
	return nil
}
