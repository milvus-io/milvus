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
	"bytes"
	"fmt"
	"plugin"
	"strconv"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/hook"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var (
	Cipher                 atomic.Value
	initCipherOnce         sync.Once
	ErrCipherPluginMissing = errors.New("cipher plugin is missing")
)

// GetCipher returns singleton hook.Cipher instance.
// If Milvus is not built with cipher plugin, it will return nil
// If Milvus is built with cipher plugin, it will return hook.Cipher
func GetCipher() hook.Cipher {
	InitOnceCipher()
	return Cipher.Load().(cipherContainer).cipher
}

func IsClusterEncyptionEnabled() bool {
	return GetCipher() != nil
}

const (
	// Used in db and collection properties
	EncryptionEnabledKey = "cipher.enabled"
	EncryptionRootKeyKey = "cipher.key"
	EncryptionEzIDKey    = "cipher.ezID"

	// Used in Plugins
	CipherConfigCreateEZ       = "cipher.ez.create"
	CipherConfigRemoveEZ       = "cipher.ez.remove"
	CipherConfigMilvusRoleName = "cipher.milvusRoleName"
	CipherConfigKeyKmsKeyArn   = "cipher.kmsKeyArn"
	CipherConfigUnsafeEZK      = "cipher.ezk"
)

type EZ struct {
	EzID         int64
	CollectionID int64
}

func (ez *EZ) AsMessageConfig() *message.CipherConfig {
	if ez == nil {
		return nil
	}
	return &message.CipherConfig{EzID: ez.EzID, CollectionID: ez.CollectionID}
}

type CipherContext struct {
	EZ
	key []byte
}

func ContainsCipherProperties(properties []*commonpb.KeyValuePair, deletedKeys []string) bool {
	for _, property := range properties {
		if property.Key == EncryptionEnabledKey ||
			property.Key == EncryptionEzIDKey ||
			property.Key == EncryptionRootKeyKey {
			return true
		}
	}
	return lo.ContainsBy(deletedKeys, func(data string) bool {
		return lo.Contains([]string{EncryptionEnabledKey, EncryptionEzIDKey, EncryptionRootKeyKey}, data)
	})
}

func GetEzByCollProperties(collProperties []*commonpb.KeyValuePair, collectionID int64) *EZ {
	if len(collProperties) == 0 {
		return nil
	}
	for _, property := range collProperties {
		if property.Key == EncryptionEzIDKey {
			ezID, _ := strconv.ParseInt(property.Value, 10, 64)
			return &EZ{
				EzID:         ezID,
				CollectionID: collectionID,
			}
		}
	}
	return nil
}

// GetStoragePluginContext returns the local plugin context for RPC from datacoord to datanode
func GetStoragePluginContext(properties []*commonpb.KeyValuePair, collectionID int64) []*commonpb.KeyValuePair {
	if GetCipher() == nil {
		return nil
	}

	if ez := GetEzByCollProperties(properties, collectionID); ez != nil {
		key := GetCipher().GetUnsafeKey(ez.EzID, ez.CollectionID)
		pluginContext := []*commonpb.KeyValuePair{
			{
				Key:   CipherConfigCreateEZ,
				Value: strconv.FormatInt(ez.EzID, 10),
			},
			{
				Key:   CipherConfigUnsafeEZK,
				Value: string(key),
			},
		}
		return pluginContext
	}

	return nil
}

func GetDBCipherProperties(ezID uint64, kmsKey string) []*commonpb.KeyValuePair {
	return []*commonpb.KeyValuePair{
		{
			Key:   EncryptionEnabledKey,
			Value: "true",
		},
		{
			Key:   EncryptionEzIDKey,
			Value: strconv.FormatUint(ezID, 10),
		},
		{
			Key:   EncryptionRootKeyKey,
			Value: kmsKey,
		},
	}
}

func RemoveEZByDBProperties(dbProperties []*commonpb.KeyValuePair) error {
	if GetCipher() == nil {
		return nil
	}

	ezIdStr := ""
	for _, property := range dbProperties {
		if property.Key == EncryptionEzIDKey {
			ezIdStr = property.Value
		}
	}
	if len(ezIdStr) == 0 {
		return nil
	}

	dropConfig := map[string]string{CipherConfigRemoveEZ: ezIdStr}
	if err := GetCipher().Init(dropConfig); err != nil {
		return err
	}
	return nil
}

func CreateLocalEZByPluginContext(context []*commonpb.KeyValuePair) (*indexcgopb.StoragePluginContext, error) {
	if GetCipher() == nil {
		return nil, nil
	}
	config := make(map[string]string)
	ctx := &indexcgopb.StoragePluginContext{}
	for _, value := range context {
		if value.GetKey() == CipherConfigCreateEZ {
			ezID, err := strconv.ParseInt(value.GetValue(), 10, 64)
			if err != nil {
				return nil, err
			}
			config[CipherConfigCreateEZ] = value.GetValue()
			ctx.EncryptionZoneId = ezID
		}
		if value.GetKey() == CipherConfigUnsafeEZK {
			config[CipherConfigUnsafeEZK] = value.GetValue()
			ctx.EncryptionKey = value.GetValue()
		}
	}
	if len(config) == 2 {
		return ctx, GetCipher().Init(config)
	}
	return nil, nil
}

func CreateEZByDBProperties(dbProperties []*commonpb.KeyValuePair) error {
	if GetCipher() == nil {
		return nil
	}

	config := make(map[string]string)
	for _, property := range dbProperties {
		if property.GetKey() == EncryptionEzIDKey {
			config[CipherConfigCreateEZ] = property.Value
		}
		if property.GetKey() == EncryptionRootKeyKey {
			config[CipherConfigKeyKmsKeyArn] = property.GetValue()
		}
	}

	if len(config) == 2 {
		return GetCipher().Init(config)
	}

	return nil
}

func TidyDBCipherProperties(ezID int64, dbProperties []*commonpb.KeyValuePair) ([]*commonpb.KeyValuePair, error) {
	dbEncryptionEnabled := IsDBEncryptionEnabled(dbProperties)
	if GetCipher() == nil {
		if dbEncryptionEnabled {
			return nil, ErrCipherPluginMissing
		}
		return dbProperties, nil
	}

	if dbEncryptionEnabled {
		ezIDKv := &commonpb.KeyValuePair{
			Key:   EncryptionEzIDKey,
			Value: strconv.FormatInt(ezID, 10),
		}
		// kmsKey already in the properties
		for _, property := range dbProperties {
			if property.Key == EncryptionRootKeyKey {
				dbProperties = append(dbProperties, ezIDKv)
				return dbProperties, nil
			}
		}

		if defaultRootKey := paramtable.GetCipherParams().DefaultRootKey.GetValue(); defaultRootKey != "" {
			// set default root key from config if EncryuptionRootKeyKey left empty
			dbProperties = append(dbProperties,
				ezIDKv,
				&commonpb.KeyValuePair{
					Key:   EncryptionRootKeyKey,
					Value: defaultRootKey,
				},
			)
			return dbProperties, nil
		}
		return nil, fmt.Errorf("Empty default root key for encrypted database without kms key")
	}
	return dbProperties, nil
}

func GetEzPropByDBProperties(dbProperties []*commonpb.KeyValuePair) *commonpb.KeyValuePair {
	for _, property := range dbProperties {
		if property.Key == EncryptionEzIDKey {
			return &commonpb.KeyValuePair{
				Key:   EncryptionEzIDKey,
				Value: property.Value,
			}
		}
	}
	return nil
}

func IsDBEncryptionEnabled(dbProperties []*commonpb.KeyValuePair) bool {
	for _, property := range dbProperties {
		if property.Key == EncryptionEnabledKey && strings.ToLower(property.Value) == "true" {
			return true
		}
	}
	return false
}

// For test only
func InitTestCipher() {
	InitOnceCipher()
	storeCipher(testCipher{})
}

// cipherContainer is Container to wrap hook.Cipher interface
// this struct is used to be stored in atomic.Value
// since different type stored in it will cause panicking.
type cipherContainer struct {
	cipher hook.Cipher
}

func storeCipher(cipher hook.Cipher) {
	Cipher.Store(cipherContainer{cipher: cipher})
}

func initCipher() error {
	storeCipher(nil)

	pathGo := paramtable.GetCipherParams().SoPathGo.GetValue()
	pathCpp := paramtable.GetCipherParams().SoPathCpp.GetValue()
	if pathGo == "" || pathCpp == "" {
		log.Info("empty so path for cipher plugin, skip to load plugin")
		return nil
	}

	log.Info("start to load cipher go plugin", zap.String("path", pathGo))
	p, err := plugin.Open(pathGo)
	if err != nil {
		return fmt.Errorf("fail to open the cipher plugin, error: %s", err.Error())
	}
	log.Info("cipher plugin opened", zap.String("path", pathGo))

	h, err := p.Lookup("CipherPlugin")
	if err != nil {
		return fmt.Errorf("fail to the 'CipherPlugin' object in the plugin, error: %s", err.Error())
	}

	cipherVal, ok := h.(hook.Cipher)
	if !ok {
		return fmt.Errorf("fail to convert the `CipherPlugin` interface")
	}

	initConfigs := lo.Assign(paramtable.Get().EtcdCfg.GetAll(), paramtable.GetCipherParams().GetAll())
	initConfigs[CipherConfigMilvusRoleName] = paramtable.GetRole()
	if err = cipherVal.Init(initConfigs); err != nil {
		return fmt.Errorf("fail to init configs for the cipher plugin, error: %s", err.Error())
	}
	storeCipher((cipherVal))
	return nil
}

func InitOnceCipher() {
	initCipherOnce.Do(func() {
		err := initCipher()
		if err != nil {
			log.Panic("fail to init cipher plugin",
				zap.String("Go so path", paramtable.GetCipherParams().SoPathGo.GetValue()),
				zap.String("Cpp so path", paramtable.GetCipherParams().SoPathCpp.GetValue()),
				zap.Error(err))
		}
	})
}

// testCipher encryption will append magicStr to plainText, magicStr is str of ezID and collectionID
type testCipher struct{}

var (
	_ hook.Cipher    = (*testCipher)(nil)
	_ hook.Encryptor = (*testCryptoImpl)(nil)
	_ hook.Decryptor = (*testCryptoImpl)(nil)
)

func (d testCipher) Init(params map[string]string) error {
	return nil
}

func (d testCipher) GetEncryptor(ezID, collectionID int64) (encryptor hook.Encryptor, safeKey []byte, err error) {
	return createTestCryptoImpl(ezID, collectionID), []byte("safe key"), nil
}

func (d testCipher) GetDecryptor(ezID, collectionID int64, safeKey []byte) (hook.Decryptor, error) {
	return createTestCryptoImpl(ezID, collectionID), nil
}

func (d testCipher) GetUnsafeKey(ezID, collectionID int64) []byte {
	return []byte("unsafe key")
}

// append magicStr to plainText
type testCryptoImpl struct {
	magicStr string
}

func createTestCryptoImpl(ezID, collectionID int64) testCryptoImpl {
	return testCryptoImpl{fmt.Sprintf("%d%d", ezID, collectionID)}
}

func (c testCryptoImpl) Encrypt(plainText []byte) (cipherText []byte, err error) {
	return append(plainText, []byte(c.magicStr)...), nil
}

func (c testCryptoImpl) Decrypt(cipherText []byte) (plainText []byte, err error) {
	return bytes.TrimSuffix(cipherText, []byte(c.magicStr)), nil
}
