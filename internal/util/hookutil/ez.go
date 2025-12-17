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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

const (
	// Used in Plugins
	CipherConfigCreateEZ       = "cipher.ez.create"
	CipherConfigRemoveEZ       = "cipher.ez.remove"
	CipherConfigImportEZ       = "cipher.ez.import"
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
		return 0, "", fmt.Errorf("invalid EZ context format: %s", encoded)
	}

	ezID, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("invalid ezID in context: %w", err)
	}

	return ezID, parts[1], nil
}

func ParseEzIDFromProperties(properties []*commonpb.KeyValuePair) (int64, bool) {
	for _, property := range properties {
		if property.Key == EncryptionEzIDKey {
			ezID, err := strconv.ParseInt(property.Value, 10, 64)
			if err != nil {
				return 0, false
			}
			return ezID, true
		}
	}
	return 0, false
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
	cipher := GetCipher()
	if cipher == nil {
		return nil
	}

	config := map[string]string{
		CipherConfigCreateEZ:  strconv.FormatInt(ezID, 10),
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
		return "", ErrCipherPluginMissing
	}

	backupProvider, ok := cipher.(BackupInterface)
	if !ok {
		return "", fmt.Errorf("cipher plugin does not support backup operation")
	}

	return backupProvider.Backup(ezID)
}

func GetPluginContext(ezID int64, collectionID int64) ([]*commonpb.KeyValuePair, error) {
	cipher := GetCipher()
	if cipher == nil {
		return nil, nil
	}

	key := cipher.GetUnsafeKey(ezID, collectionID)
	if len(key) == 0 {
		return nil, errors.Newf("cannot get ez key for ezID=%d, collectionID=%d", ezID, collectionID)
	}

	return []*commonpb.KeyValuePair{{
		Key:   CipherConfigUnsafeEZK,
		Value: encodeEZContext(ezID, key),
	}}, nil
}

func ImportEZ(importEzk string) ([]*commonpb.KeyValuePair, error) {
	cipher := GetCipher()
	if cipher == nil || importEzk == "" {
		return nil, nil
	}

	config := map[string]string{
		CipherConfigImportEZ: importEzk,
	}
	if err := cipher.Init(config); err != nil {
		return nil, fmt.Errorf("failed to import EZK: %w", err)
	}

	ezID, err := GetEzIDByImportEzk(importEzk)
	if err != nil {
		return nil, err
	}
	return GetPluginContext(ezID, 0)
}

func GetEzIDByImportEzk(importEzk string) (int64, error) {
	ezkBytes, err := base64.StdEncoding.DecodeString(importEzk)
	if err != nil {
		return 0, fmt.Errorf("failed to decode EZK: %w", err)
	}

	type EZKData struct {
		EzID int64 `json:"ez_id"`
	}
	var ezkData EZKData
	if err := json.Unmarshal(ezkBytes, &ezkData); err != nil {
		return 0, fmt.Errorf("failed to unmarshal EZK: %w", err)
	}
	return ezkData.EzID, nil
}
