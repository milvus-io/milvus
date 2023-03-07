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

	"github.com/stretchr/testify/assert"
)

func TestLoadFromFileSource(t *testing.T) {
	t.Run("file not exist", func(t *testing.T) {
		fs := NewFileSource(&FileInfo{[]string{"file_not_exist.yaml"}, -1})
		_ = fs.loadFromFile()
		assert.Zero(t, len(fs.configs))
	})

	t.Run("file type not support", func(t *testing.T) {
		fs := NewFileSource(&FileInfo{[]string{"../../go.mod"}, -1})
		err := fs.loadFromFile()
		assert.Error(t, err)
	})

	t.Run("multiple files", func(t *testing.T) {
		dir, _ := os.MkdirTemp("", "milvus")
		os.WriteFile(path.Join(dir, "milvus.yaml"), []byte("a.b: 1\nc.d: 2"), 0600)
		os.WriteFile(path.Join(dir, "user.yaml"), []byte("a.b: 3"), 0600)

		fs := NewFileSource(&FileInfo{[]string{path.Join(dir, "milvus.yaml"), path.Join(dir, "user.yaml")}, -1})
		fs.loadFromFile()
		v1, _ := fs.GetConfigurationByKey("a.b")
		assert.Equal(t, "3", v1)
		v2, _ := fs.GetConfigurationByKey("c.d")
		assert.Equal(t, "2", v2)
	})
}
