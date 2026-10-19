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

package storagev2

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	sharedstorage "github.com/milvus-io/milvus/pkg/v3/storagev2"
)

// Milvus keys already carry the storage prefix, so the loon filesystem must be
// rooted at the namespace root: "/" for local, and the configured root path
// (which milvus-storage does not apply for object storage) for remote.
func TestLoonFSRootPath(t *testing.T) {
	assert.Equal(t, "/", sharedstorage.LoonFSRootPath(&indexpb.StorageConfig{StorageType: "local", RootPath: "/var/lib/milvus/data"}))
	assert.Equal(t, "/", sharedstorage.LoonFSRootPath(&indexpb.StorageConfig{StorageType: "local", RootPath: "data"}))
	assert.Equal(t, "/", sharedstorage.LoonFSRootPath(&indexpb.StorageConfig{StorageType: "local"}))
	assert.Equal(t, "files", sharedstorage.LoonFSRootPath(&indexpb.StorageConfig{StorageType: "remote", RootPath: "files"}))
	assert.Equal(t, "files", sharedstorage.LoonFSRootPath(&indexpb.StorageConfig{StorageType: "minio", RootPath: "files"}))
	assert.Equal(t, "", sharedstorage.LoonFSRootPath(&indexpb.StorageConfig{StorageType: "remote"}))
	assert.Equal(t, "", sharedstorage.LoonFSRootPath(nil))
}
