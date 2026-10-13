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

// Package manifest holds the durable state of the primary key index of one
// vchannel: the manifest edits, the object key layout, and the object store
// interface.
package manifest

import (
	"fmt"
	"path"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
)

const (
	// PointerPrefix is the etcd key prefix of all manifest pointers.
	PointerPrefix = "pkindex-meta/manifest/"
	// ClaimPrefix is the etcd key prefix of all pchannel claim records.
	ClaimPrefix = "pkindex-meta/claim/"
)

// Layout builds the object keys of one cluster from the root path of its
// object storage.
type Layout struct {
	RootPath string
}

// PointerKey returns the etcd key of the manifest pointer of a vchannel.
func PointerKey(collectionID int64, vchannel string) string {
	return PointerPrefix + strconv.FormatInt(collectionID, 10) + "/" + vchannel
}

// ClaimKey returns the etcd key of the claim record of a pchannel.
func ClaimKey(pchannel string) string {
	return ClaimPrefix + pchannel
}

// ParsePointerKey returns the collection ID and the vchannel of a pointer key.
func ParsePointerKey(key string) (int64, string, bool) {
	rest, ok := strings.CutPrefix(key, PointerPrefix)
	if !ok {
		return 0, "", false
	}
	coll, vchannel, ok := strings.Cut(rest, "/")
	if !ok || vchannel == "" || strings.Contains(vchannel, "/") {
		return 0, "", false
	}
	id, err := strconv.ParseInt(coll, 10, 64)
	if err != nil {
		return 0, "", false
	}
	return id, vchannel, true
}

func (l Layout) vchannelDir(collectionID int64, vchannel string) string {
	return path.Join(l.RootPath, common.PKIndexPath, strconv.FormatInt(collectionID, 10), vchannel)
}

// SSTBasePath returns the complete object key prefix of the SSTs of a vchannel.
func (l Layout) SSTBasePath(collectionID int64, vchannel string) string {
	return metautil.BuildPKIndexSSTBasePath(l.RootPath, collectionID, vchannel)
}

// ManifestDir returns the complete object key prefix of the manifest bodies of a vchannel.
func (l Layout) ManifestDir(collectionID int64, vchannel string) string {
	return l.vchannelDir(collectionID, vchannel) + "/manifest"
}

// ManifestBodyKey returns the object key of one manifest body. The nonce makes
// each body key unique.
func (l Layout) ManifestBodyKey(collectionID int64, vchannel string, version int64, nonce string) string {
	return fmt.Sprintf("%s/%d-%s.pb", l.ManifestDir(collectionID, vchannel), version, nonce)
}

// SSTKey returns the object key of an SST from the sst_base_path of a manifest.
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
