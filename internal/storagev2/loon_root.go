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

import "github.com/milvus-io/milvus/pkg/v3/proto/indexpb"

// LoonLocalFSRootPath is the fs.root_path handed to loon for storageType=local.
//
// Milvus keys already carry the storage prefix: localStorage.path for local
// (/var/lib/milvus/data/insert_log/...) and minio.rootPath for remote
// (files/insert_log/...). The loon filesystem therefore has to be rooted at the
// namespace root, which is the bucket for remote (milvus-storage never applies
// root_path there) and "/" for local. Rooting the local SubTreeFileSystem at
// localStorage.path would join the prefix twice (milvus-storage #351, #53051)
// and make complete keys differ from their physical locations. Every Go site
// that turns a StorageConfig into loon properties must go through
// LoonFSRootPath; the C++ counterpart is
// LoonFSRootPath in storage/loon_ffi/util.h.
const LoonLocalFSRootPath = "/"

// LoonFSRootPath returns the fs.root_path for storageConfig: "/" for local,
// the configured root path otherwise.
func LoonFSRootPath(storageConfig *indexpb.StorageConfig) string {
	if storageConfig.GetStorageType() == "local" {
		return LoonLocalFSRootPath
	}
	return storageConfig.GetRootPath()
}
