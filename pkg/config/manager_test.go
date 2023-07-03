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
	"os"
	"path"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

func TestAllConfigFromManager(t *testing.T) {
	mgr, _ := Init()
	all := mgr.GetConfigs()
	assert.Equal(t, 0, len(all))

	mgr, _ = Init(WithEnvSource(formatKey))
	all = mgr.GetConfigs()
	assert.Less(t, 0, len(all))
}

func TestConfigChangeEvent(t *testing.T) {
	dir, _ := os.MkdirTemp("", "milvus")
	os.WriteFile(path.Join(dir, "milvus.yaml"), []byte("a.b: 1\nc.d: 2"), 0600)
	os.WriteFile(path.Join(dir, "user.yaml"), []byte("a.b: 3"), 0600)

	fs := NewFileSource(&FileInfo{[]string{path.Join(dir, "milvus.yaml"), path.Join(dir, "user.yaml")}, 1})
	mgr, _ := Init()
	err := mgr.AddSource(fs)
	assert.NoError(t, err)
	res, err := mgr.GetConfig("a.b")
	assert.NoError(t, err)
	assert.Equal(t, res, "3")
	os.WriteFile(path.Join(dir, "user.yaml"), []byte("a.b: 6"), 0600)
	time.Sleep(3 * time.Second)
	res, err = mgr.GetConfig("a.b")
	assert.NoError(t, err)
	assert.Equal(t, res, "6")
}

func TestAllDupliateSource(t *testing.T) {
	mgr, _ := Init()
	err := mgr.AddSource(NewEnvSource(formatKey))
	assert.NoError(t, err)
	err = mgr.AddSource(NewEnvSource(formatKey))
	assert.Error(t, err)

	err = mgr.AddSource(ErrSource{})
	assert.Error(t, err, "error")

	err = mgr.pullSourceConfigs("ErrSource")
	assert.Error(t, err, "invalid source or source not added")
}

type ErrSource struct {
}

func (e ErrSource) Close() {
}

func (e ErrSource) GetConfigurationByKey(string) (string, error) {
	return "", errors.New("error")
}

// GetConfigurations implements Source
func (ErrSource) GetConfigurations() (map[string]string, error) {
	return nil, errors.New("error")
}

// GetPriority implements Source
func (ErrSource) GetPriority() int {
	return 2
}

// GetSourceName implements Source
func (ErrSource) GetSourceName() string {
	return "ErrSource"
}

func (e ErrSource) SetEventHandler(eh EventHandler) {

}

func (e ErrSource) UpdateOptions(opt Options) {
}
