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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

const (
	// Used in Plugins
	CipherConfigImportEZ       = "cipher.ez.import"
	CipherConfigCreateEZ       = "cipher.ez.create"
	CipherConfigRemoveEZ       = "cipher.ez.remove"
	CipherConfigMilvusRoleName = "cipher.milvusRoleName"
	CipherConfigKeyKmsKeyArn   = "cipher.kmsKeyArn"
	CipherConfigUnsafeEZK      = "cipher.ezk"
)

func encodeEZContext(ezID int64, key []byte) *commonpb.KeyValuePair {
	return &commonpb.KeyValuePair{
		Key:   CipherConfigUnsafeEZK,
		Value: fmt.Sprintf("%d:%s", ezID, base64.StdEncoding.EncodeToString(key)),
	}
}

func decodeEZContext(encoded string) (ezID int64, key string, err error) {
	parts := strings.Split(encoded, ":")
	if len(parts) != 2 {
		return 0, "", merr.WrapErrParameterInvalidMsg("invalid EZ context format")
	}

	ezID, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, "", merr.WrapErrParameterInvalidMsg("invalid ezID in context")
	}

	return ezID, parts[1], nil
}

func ParseEzIDFromProperties(properties []*commonpb.KeyValuePair) (int64, bool) {
	for _, property := range properties {
		if property.GetKey() == common.EncryptionEzIDKey {
			ezID, err := strconv.ParseInt(property.GetValue(), 10, 64)
			if err != nil {
				return 0, false
			}
			return ezID, true
		}
	}
	return 0, false
}

// parseTargetEzID requires at most one positive encryption zone ID on the target schema.
func parseTargetEzID(properties []*commonpb.KeyValuePair) (int64, bool, error) {
	var id int64
	var found bool
	for _, property := range properties {
		if property.GetKey() != common.EncryptionEzIDKey {
			continue
		}
		if found {
			return 0, false, merr.WrapErrParameterInvalidMsg("duplicate target %s", common.EncryptionEzIDKey)
		}
		parsed, err := strconv.ParseInt(property.GetValue(), 10, 64)
		if err != nil || parsed <= 0 {
			return 0, false, merr.WrapErrParameterInvalidMsg("invalid target %s", common.EncryptionEzIDKey)
		}
		id = parsed
		found = true
	}
	return id, found, nil
}

func CreateEZ(ezID int64, kmsKeyArn string) error {
	cipher := GetCipher()
	if cipher == nil {
		return nil
	}

	config := map[string]string{
		CipherConfigCreateEZ:     strconv.FormatInt(ezID, 10),
		CipherConfigKeyKmsKeyArn: kmsKeyArn,
	}

	return cipher.Init(config)
}

func CreateLocalEZ(ezID int64, encryptionKey string) error {
	return registerLocalEZ(CipherConfigCreateEZ, ezID, encryptionKey)
}

func RegisterLocalImportEZ(ezID int64, encryptionKey string) error {
	return registerLocalEZ(CipherConfigImportEZ, ezID, encryptionKey)
}

func registerLocalEZ(configKey string, ezID int64, encryptionKey string) error {
	cipher := GetCipher()
	if cipher == nil {
		return nil
	}

	config := map[string]string{
		configKey:             strconv.FormatInt(ezID, 10),
		CipherConfigUnsafeEZK: encryptionKey,
	}

	return cipher.Init(config)
}

func RemoveEZ(ezID int64) error {
	cipher := GetCipher()
	if cipher == nil {
		return nil
	}

	config := map[string]string{
		CipherConfigRemoveEZ: strconv.FormatInt(ezID, 10),
	}

	return cipher.Init(config)
}

func BackupEZ(ezID int64) (string, error) {
	cipher := GetCipher()
	if cipher == nil {
		return "", errCipherPluginMissing
	}

	backupProvider, ok := cipher.(EZBackupCipher)
	if !ok {
		return "", merr.WrapErrServiceInternalMsg("cipher plugin does not support backup operation")
	}

	return backupProvider.Backup(ezID)
}

func GetPluginContext(ezID int64, collectionID int64) ([]*commonpb.KeyValuePair, error) {
	cipher := GetCipher()
	if cipher == nil {
		return nil, errCipherPluginMissing
	}

	key := cipher.GetUnsafeKey(ezID, collectionID)
	if len(key) == 0 {
		return nil, merr.WrapErrServiceUnavailable(fmt.Sprintf("cannot get ez key for ezID=%d, collectionID=%d", ezID, collectionID))
	}

	return []*commonpb.KeyValuePair{encodeEZContext(ezID, key)}, nil
}

func ResolveImportEZ(artifact string) ([]*commonpb.KeyValuePair, error) {
	if artifact == "" {
		return nil, nil
	}
	sourceID, err := GetEzIDByImportEzk(artifact)
	if err != nil {
		return nil, err
	}
	cipher := GetCipher()
	if cipher == nil {
		return nil, errCipherPluginMissing
	}
	resolver, ok := cipher.(EZBackupCipher)
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("cipher plugin does not support ResolveImportEZ")
	}
	ezID, key, err := resolver.ResolveImportEZ(artifact)
	if err != nil {
		return nil, merr.Wrap(err, "failed to resolve source EZK")
	}
	if ezID <= 0 || len(key) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("cipher plugin returned an invalid source EZK")
	}
	if ezID != sourceID {
		return nil, merr.WrapErrServiceInternalMsg("cipher plugin resolved source ezID %d, expected %d", ezID, sourceID)
	}
	return []*commonpb.KeyValuePair{encodeEZContext(ezID, key)}, nil
}

func GetEzIDByImportEzk(importEzk string) (int64, error) {
	ezkBytes, err := base64.StdEncoding.DecodeString(importEzk)
	if err != nil {
		return 0, merr.Wrap(err, "failed to decode EZK")
	}

	type EZKData struct {
		EzID int64 `json:"ez_id"`
	}
	var ezkData EZKData
	if err := json.Unmarshal(ezkBytes, &ezkData); err != nil {
		return 0, merr.WrapErrParameterInvalidMsg("failed to unmarshal backup EZK")
	}
	if ezkData.EzID <= 0 {
		return 0, merr.WrapErrParameterInvalidMsg("backup EZK must include a positive ez_id")
	}
	return ezkData.EzID, nil
}

// GetImportSourcePluginContext extracts the backup source cipher context from the
// request's plugin context. When present, the source entry always precedes the target.
func GetImportSourcePluginContext(artifact string, pluginContext []*commonpb.KeyValuePair) (*indexcgopb.StoragePluginContext, error) {
	if artifact == "" {
		return nil, nil
	}
	id, err := GetEzIDByImportEzk(artifact)
	if err != nil {
		return nil, merr.WrapErrImportFailed(err.Error())
	}
	for _, entry := range pluginContext {
		if entry.GetKey() == CipherConfigUnsafeEZK {
			return importCipherEntry(entry, "source", id, 0)
		}
	}
	return nil, merr.WrapErrImportFailed("missing source import cipher context")
}

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
			return nil, nil, merr.WrapErrImportFailed(err.Error())
		}
		// The wire context contains the source zone, but no source collection ID.
		source, err = importCipherEntry(next(), "source", id, 0)
		if err != nil {
			return nil, nil, err
		}
	}
	targetID, hasTarget, err := parseTargetEzID(properties)
	if err != nil {
		return nil, nil, merr.WrapErrImportFailed(err.Error())
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
) error {
	source, target, err := ParseImportCipherContexts(artifact, properties, pluginContext, collectionID)
	if err != nil {
		return err
	}
	if (source != nil || target != nil) && !IsClusterEncryptionEnabled() {
		return merr.WrapErrServiceUnavailableErr(errCipherPluginMissing, "cannot register import cipher contexts")
	}
	// Register the authoritative target before checking a same-zone source.
	if target != nil {
		if err := CreateLocalEZ(target.GetEncryptionZoneId(), target.GetEncryptionKey()); err != nil {
			return err
		}
	}
	if source != nil {
		if err := RegisterLocalImportEZ(source.GetEncryptionZoneId(), source.GetEncryptionKey()); err != nil {
			return err
		}
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
