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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ImportCipherContext holds immutable snapshots for one task attempt.
// The source may share its zone ID with the target after key rotation.
type ImportCipherContext struct {
	Source *indexcgopb.StoragePluginContext
	Target *indexcgopb.StoragePluginContext
}

func registerTaskKey(context *indexcgopb.StoragePluginContext, readOnly bool) error {
	if context == nil {
		return nil
	}
	cipher := GetCipher()
	if cipher == nil {
		return merr.Wrap(merr.ErrServiceUnavailable, "cipher plugin is unavailable for encrypted import")
	}
	config := map[string]string{
		CipherConfigCreateEZ:  strconv.FormatInt(context.GetEncryptionZoneId(), 10),
		CipherConfigUnsafeEZK: context.GetEncryptionKey(),
	}
	if readOnly {
		config[CipherConfigReadOnly] = "true"
	}
	err := cipher.Init(config)
	if err != nil && !merr.IsRetryableErr(err) && !merr.IsCanceledOrTimeout(err) {
		err = merr.Combine(err, merr.ErrImportSysFailed)
	}
	return merr.Wrap(err, "register import encryption context")
}

// PrepareImportCipher validates the entire ordered wire context before making
// any keys visible. Source files carry their own collection ID in the EDEK
// metadata. The destination collection ID belongs only to the writer.
func PrepareImportCipher(artifact string, properties, entries []*commonpb.KeyValuePair, collectionID int64) (ImportCipherContext, error) {
	var sourceEZ int64
	if artifact != "" {
		var err error
		sourceEZ, err = GetEzIDByImportEzk(artifact)
		if err != nil {
			return ImportCipherContext{}, merr.Combine(err, merr.ErrImportSysFailed)
		}
		if sourceEZ <= 0 {
			return ImportCipherContext{}, merr.WrapErrImportSysFailedMsg("invalid backup encryption zone")
		}
	}
	return PrepareImportCipherForZone(sourceEZ, properties, entries, collectionID)
}

func PrepareImportCipherForZone(sourceEZ int64, properties, entries []*commonpb.KeyValuePair, collectionID int64) (ImportCipherContext, error) {
	var cipherEntries []*commonpb.KeyValuePair
	for _, entry := range entries {
		if entry.GetKey() == CipherConfigUnsafeEZK {
			cipherEntries = append(cipherEntries, entry)
		}
	}

	take := func(ezID, collID int64) (*indexcgopb.StoragePluginContext, error) {
		if len(cipherEntries) == 0 {
			return nil, merr.WrapErrImportSysFailedMsg("import task is missing a keyset for EZ %d", ezID)
		}
		zone, key, err := decodeEZContext(cipherEntries[0].GetValue())
		cipherEntries = cipherEntries[1:]
		if err != nil || ezID <= 0 || zone != ezID {
			return nil, merr.WrapErrImportSysFailedMsg("import task has an invalid key context for EZ %d", ezID)
		}
		decoded, err := base64.StdEncoding.DecodeString(key)
		if err != nil || len(decoded) == 0 {
			return nil, merr.WrapErrImportSysFailedMsg("import task has an invalid keyset for EZ %d", ezID)
		}
		return &indexcgopb.StoragePluginContext{EncryptionZoneId: zone, CollectionId: collID, EncryptionKey: key}, nil
	}

	var result ImportCipherContext
	if sourceEZ != 0 {
		var err error
		result.Source, err = take(sourceEZ, 0)
		if err != nil {
			return ImportCipherContext{}, err
		}
	}
	if target := GetEzByCollProperties(properties, collectionID); target != nil {
		var err error
		result.Target, err = take(target.EzID, collectionID)
		if err != nil {
			return ImportCipherContext{}, err
		}
	}
	if len(cipherEntries) != 0 {
		return ImportCipherContext{}, merr.WrapErrImportSysFailedMsg("import task has unexpected cipher.ezk entries")
	}
	if err := registerTaskKey(result.Source, true); err != nil {
		return ImportCipherContext{}, err
	}
	if err := registerTaskKey(result.Target, false); err != nil {
		return ImportCipherContext{}, err
	}
	return result, nil
}
