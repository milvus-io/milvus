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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// ParseImportCipherContexts reads the ordered source and target snapshots without
// consulting or updating the worker's keys. A plaintext side has no wire entry.
func ParseImportCipherContexts(artifact string, properties, pluginContext []*commonpb.KeyValuePair, collectionID int64) (source, target *indexcgopb.StoragePluginContext, err error) {
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
			return nil, nil, err
		}
		// The wire context contains the source zone, but no source collection ID.
		source, err = importCipherEntry(next(), "source", id, 0)
		if err != nil {
			return nil, nil, err
		}
	}
	targetID, hasTarget, err := parseTargetEzID(properties)
	if err != nil {
		return nil, nil, err
	}
	if hasTarget {
		target, err = importCipherEntry(next(), "target", targetID, collectionID)
		if err != nil {
			return nil, nil, err
		}
	}
	if next() != nil {
		return nil, nil, merr.WrapErrImportFailed("unexpected import cipher context")
	}
	return source, target, nil
}

func RegisterImportCipherContexts(
	artifact string,
	properties, pluginContext []*commonpb.KeyValuePair,
	collectionID int64,
	registerNativeRead func(ezID int64, key string) error,
) error {
	source, target, err := ParseImportCipherContexts(artifact, properties, pluginContext, collectionID)
	if err != nil {
		return err
	}
	if (source != nil || target != nil) && !IsClusterEncryptionEnabled() {
		return errCipherPluginMissing
	}
	if source != nil {
		if err := RegisterLocalImportEZ(source.GetEncryptionZoneId(), source.GetEncryptionKey()); err != nil {
			return err
		}
		if registerNativeRead != nil {
			if err := registerNativeRead(source.GetEncryptionZoneId(), source.GetEncryptionKey()); err != nil {
				return err
			}
		}
	}
	if target != nil {
		return CreateLocalEZ(target.GetEncryptionZoneId(), target.GetEncryptionKey())
	}
	return nil
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
