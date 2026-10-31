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

package pkindex

import (
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/common"
)

// The functions below build and parse the object keys of the primary key
// index. All keys are complete object keys under the root path of the chunk
// manager (rootPath):
//
//	<rootPath>/pk_index/<collectionID>/<vchannel>/
//	├── sst/                        SSTBasePath
//	│   └── <sstID>.sst             SSTKey, ParseSSTKey
//	└── manifest/                   ManifestDir
//	    └── <version>-<nonce>.pb    ManifestBodyKey, ParseManifestBodyKey
//
// For example: files/pk_index/449/by-dev-rootcoord-dml_0_449v0/sst/450123.sst
//
// A streaming node handover and a data node compaction write SSTs. DataCoord
// writes the manifest bodies. The DataCoord GC deletes both.

// vchannelDir returns the complete object key prefix of all objects of a vchannel.
func vchannelDir(rootPath string, collectionID int64, vchannel string) string {
	return path.Join(rootPath, common.PKIndexPath, strconv.FormatInt(collectionID, 10), vchannel)
}

// SSTBasePath returns the complete object key prefix of the SSTs of a vchannel.
func SSTBasePath(rootPath string, collectionID int64, vchannel string) string {
	return vchannelDir(rootPath, collectionID, vchannel) + "/sst"
}

// SSTKey returns the object key of an SST from the SST base path of a manifest.
func SSTKey(basePath string, id int64) string {
	return basePath + "/" + strconv.FormatInt(id, 10) + ".sst"
}

// ParseSSTKey returns the SST ID of an object key directly under basePath.
func ParseSSTKey(basePath, key string) (int64, bool) {
	name, ok := strings.CutPrefix(key, basePath+"/")
	if !ok || strings.Contains(name, "/") {
		return 0, false
	}
	idStr, ok := strings.CutSuffix(name, ".sst")
	if !ok {
		return 0, false
	}
	id, err := strconv.ParseInt(idStr, 10, 64)
	return id, err == nil
}

// ManifestDir returns the complete object key prefix of the manifest bodies of a vchannel.
func ManifestDir(rootPath string, collectionID int64, vchannel string) string {
	return vchannelDir(rootPath, collectionID, vchannel) + "/manifest"
}

// ManifestBodyKey returns the object key of one manifest body under dir. The
// nonce makes each body key unique.
func ManifestBodyKey(dir string, version int64, nonce string) string {
	return fmt.Sprintf("%s/%d-%s.pb", dir, version, nonce)
}

// ParseManifestBodyKey returns the version of a manifest body key directly under dir.
func ParseManifestBodyKey(dir, key string) (int64, bool) {
	name, ok := strings.CutPrefix(key, dir+"/")
	if !ok || strings.Contains(name, "/") {
		return 0, false
	}
	stem, ok := strings.CutSuffix(name, ".pb")
	if !ok {
		return 0, false
	}
	versionStr, nonce, ok := strings.Cut(stem, "-")
	if !ok || nonce == "" {
		return 0, false
	}
	v, err := strconv.ParseInt(versionStr, 10, 64)
	return v, err == nil
}
