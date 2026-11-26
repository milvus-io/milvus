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

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

func encodeEZContext(ezID int64, key []byte) string {
	return fmt.Sprintf("%d:%s", ezID, base64.StdEncoding.EncodeToString(key))
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

	err := cipher.Init(config)
	if err == nil {
		return nil
	}
	if merr.IsRetryableErr(err) || merr.IsCanceledOrTimeout(err) {
		return merr.Wrapf(err, "register local encryption zone %d", ezID)
	}
	if isTemporaryError(err) {
		return merr.WrapErrServiceUnavailableErr(err, "register local encryption zone %d", ezID)
	}
	if merr.IsMilvusError(err) {
		return merr.Wrapf(err, "register local encryption zone %d", ezID)
	}
	// The EZ id/key come from the request's plugin context; an unclassified failed
	// local KMS init is a deterministic request/config rejection, not an
	// unclassified worker error.
	return merr.WrapErrParameterInvalidMsg("register local encryption zone %d: %v", ezID, err)
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

func requiredEZKey(ezID, collectionID int64) ([]byte, error) {
	cipher := GetCipher()
	if cipher == nil {
		return nil, merr.Wrapf(merr.ErrServiceUnavailable, "cipher plugin is unavailable for ezID=%d, collectionID=%d", ezID, collectionID)
	}

	key := cipher.GetUnsafeKey(ezID, collectionID)
	if len(key) == 0 {
		return nil, merr.Wrapf(merr.ErrServiceUnavailable, "cannot get ez key for ezID=%d, collectionID=%d", ezID, collectionID)
	}
	return key, nil
}

func GetPluginContext(ezID int64, collectionID int64) ([]*commonpb.KeyValuePair, error) {
	key, err := requiredEZKey(ezID, collectionID)
	if err != nil {
		return nil, err
	}
	return ezPluginContext(ezID, key), nil
}

func isTemporaryError(err error) bool {
	for err != nil {
		if tmp, ok := err.(interface{ Temporary() bool }); ok && tmp.Temporary() {
			return true
		}
		if joined, ok := err.(interface{ Unwrap() []error }); ok {
			for _, cause := range joined.Unwrap() {
				if isTemporaryError(cause) {
					return true
				}
			}
			return false
		}
		if next := errors.Unwrap(err); next != nil {
			err = next
			continue
		}
		if causer, ok := err.(interface{ Cause() error }); ok {
			err = causer.Cause()
			continue
		}
		break
	}
	return false
}

// ResolveImportEZ resolves the backup's own keyset for this dispatch.
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
		return nil, merr.Wrap(merr.ErrServiceUnavailable, "cipher plugin is unavailable for encrypted backup")
	}
	resolver, ok := cipher.(EZBackupCipher)
	if !ok {
		return nil, merr.Wrap(merr.ErrServiceUnimplemented, "cipher plugin does not support ResolveImportEZ")
	}
	ezID, key, err := resolver.ResolveImportEZ(artifact)
	if err != nil {
		if !merr.IsRetryableErr(err) && !merr.IsCanceledOrTimeout(err) && isTemporaryError(err) {
			return nil, merr.WrapErrServiceUnavailableErr(err, "failed to resolve source EZK")
		}
		return nil, merr.Wrap(err, "failed to resolve source EZK")
	}
	if ezID <= 0 || len(key) == 0 {
		return nil, merr.WrapErrServiceInternalMsg("cipher plugin returned an invalid source EZK")
	}
	if ezID != sourceID {
		return nil, merr.WrapErrServiceInternalMsg("cipher plugin resolved source ezID %d, expected %d", ezID, sourceID)
	}
	return ezPluginContext(ezID, key), nil
}

func ezPluginContext(ezID int64, key []byte) []*commonpb.KeyValuePair {
	return []*commonpb.KeyValuePair{{
		Key:   CipherConfigUnsafeEZK,
		Value: encodeEZContext(ezID, key),
	}}
}

func GetEzIDByImportEzk(importEzk string) (int64, error) {
	ezkBytes, err := base64.StdEncoding.DecodeString(importEzk)
	if err != nil {
		return 0, merr.WrapErrParameterInvalidMsg("failed to decode EZK: %v", err)
	}

	type EZKData struct {
		EzID int64 `json:"ez_id"`
	}
	var ezkData EZKData
	if err := json.Unmarshal(ezkBytes, &ezkData); err != nil {
		return 0, merr.WrapErrParameterInvalidMsg("failed to unmarshal EZK")
	}
	if ezkData.EzID <= 0 {
		return 0, merr.WrapErrParameterInvalidMsg("backup EZK must include a positive ez_id")
	}
	return ezkData.EzID, nil
}
