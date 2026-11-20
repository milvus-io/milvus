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

package hookutil

import (
	"encoding/base64"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// ImportCipherContexts keeps the two roles distinct even when they have the same zone ID.
type ImportCipherContexts struct {
	Source *indexcgopb.StoragePluginContext
	Target *indexcgopb.StoragePluginContext
}

// PrepareImportCipherContexts validates all entries before registering any read keys.
// The backup and target schema determine which roles must appear, in wire order.
func PrepareImportCipherContexts(artifact string, properties, pluginContext []*commonpb.KeyValuePair, collectionID int64) (ImportCipherContexts, error) {
	var result ImportCipherContexts
	next := func() *commonpb.KeyValuePair {
		for len(pluginContext) > 0 {
			entry := pluginContext[0]
			pluginContext = pluginContext[1:]
			if entry.GetKey() == CipherConfigUnsafeEZK {
				return entry
			}
		}
		return nil
	}
	if artifact != "" {
		id, err := GetEzIDByImportEzk(artifact)
		if err != nil {
			return ImportCipherContexts{}, err
		}
		// The wire context contains the source zone, but no source collection ID.
		result.Source, err = importCipherEntry(next(), "source", id, 0)
		if err != nil {
			return ImportCipherContexts{}, err
		}
	}
	targetID, hasTarget, err := importTargetEZ(properties)
	if err != nil {
		return ImportCipherContexts{}, err
	}
	if hasTarget {
		result.Target, err = importCipherEntry(next(), "target", targetID, collectionID)
		if err != nil {
			return ImportCipherContexts{}, err
		}
	}
	if next() != nil {
		return ImportCipherContexts{}, merr.WrapErrImportFailed("unexpected import cipher context")
	}
	if (result.Source != nil || result.Target != nil) && !IsClusterEncryptionEnabled() {
		return ImportCipherContexts{}, errCipherPluginMissing
	}
	if source := result.Source; source != nil {
		err = GetCipher().Init(map[string]string{CipherConfigCreateEZ: strconv.FormatInt(source.GetEncryptionZoneId(), 10), CipherConfigUnsafeEZK: source.GetEncryptionKey(), CipherConfigReadOnly: "true"})
		if err != nil {
			return ImportCipherContexts{}, err
		}
	}
	if target := result.Target; target != nil {
		if err := CreateLocalEZ(target.GetEncryptionZoneId(), target.GetEncryptionKey()); err != nil {
			return ImportCipherContexts{}, err
		}
	}
	return result, nil
}

func importCipherEntry(entry *commonpb.KeyValuePair, role string, expectedID, collectionID int64) (*indexcgopb.StoragePluginContext, error) {
	if entry == nil {
		return nil, merr.WrapErrImportFailedMsg("missing %s import cipher context", role)
	}
	id, key, err := decodeEZContext(entry.GetValue())
	if err != nil || id != expectedID {
		return nil, merr.WrapErrImportFailedMsg("invalid %s import cipher context for zone %d", role, expectedID)
	}
	if raw, err := base64.StdEncoding.DecodeString(key); err != nil || len(raw) == 0 {
		return nil, merr.WrapErrImportFailedMsg("invalid %s import keyset for zone %d", role, expectedID)
	}
	return &indexcgopb.StoragePluginContext{EncryptionZoneId: id, CollectionId: collectionID, EncryptionKey: key}, nil
}
