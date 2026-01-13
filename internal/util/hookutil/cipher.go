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
	"context"
	"encoding/base64"
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
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

var (
	Cipher                 atomic.Value
	initCipherOnce         sync.Once
	cipherReloadMutex      sync.RWMutex
	ErrCipherPluginMissing = errors.New("cipher plugin is missing")
)

type BackupInterface interface {
	Backup(ezID int64) (string, error)
}

// GetCipher returns singleton hook.Cipher instance.
// If Milvus is not built with cipher plugin, it will return nil
// If Milvus is built with cipher plugin, it will return hook.Cipher
func GetCipher() hook.Cipher {
	InitOnceCipher()
	return Cipher.Load().(cipherContainer).cipher
}

func GetCipherWithState() CipherWithState {
	cipher := GetCipher()
	if cipher == nil {
		return nil
	}

	cipherWithState, ok := cipher.(CipherWithState)
	if !ok {
		return nil
	}
	return cipherWithState
}

func IsClusterEncryptionEnabled() bool {
	return GetCipher() != nil
}

// KeyState represents the state of a KMS key
type KeyState string

const (
	KeyStateEnabled         KeyState = "Enabled"
	KeyStateDisabled        KeyState = "Disabled"
	KeyStatePendingDeletion KeyState = "PendingDeletion"
	KeyStateUnknown         KeyState = "Unknown"
)

// CipherWithState extends the base Cipher interface with
// GetStates
type CipherWithState interface {
	hook.Cipher

	// GetStates returns the state of KMS keys.
	// Returns a map of ezID -> state.
	GetStates() (map[int64]string, error)
}

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

// GetEzStates queries the state of KMS keys for the given EZ IDs.
// This uses type assertion to check if the cipher plugin implements CipherWithState.
// If not supported, returns an empty map and no error (backward compatible).
func GetEzStates() (map[int64]KeyState, error) {
	if GetCipherWithState() == nil {
		return make(map[int64]KeyState), nil
	}

	states, err := GetCipherWithState().GetStates()
	if err != nil {
		return nil, err
	}
	result := make(map[int64]KeyState)
	for ezID, state := range states {
		result[ezID] = KeyState(state)
	}

	return result, nil
}

func ContainsCipherProperties(properties []*commonpb.KeyValuePair, deletedKeys []string) bool {
	for _, property := range properties {
		if property.Key == common.EncryptionEnabledKey ||
			property.Key == common.EncryptionEzIDKey ||
			property.Key == common.EncryptionRootKeyKey {
			return true
		}
	}
	return lo.ContainsBy(deletedKeys, func(data string) bool {
		return lo.Contains([]string{
			common.EncryptionEnabledKey,
			common.EncryptionEzIDKey,
			common.EncryptionRootKeyKey,
		}, data)
	})
}

func GetEzByCollProperties(collProperties []*commonpb.KeyValuePair, collectionID int64) *EZ {
	if len(collProperties) == 0 {
		return nil
	}
	for _, property := range collProperties {
		if property.Key == common.EncryptionEzIDKey {
			ezID, _ := strconv.ParseInt(property.Value, 10, 64)
			return &EZ{
				EzID:         ezID,
				CollectionID: collectionID,
			}
		}
	}
	return nil
}

func CreateEZByDBProperties(dbProperties []*commonpb.KeyValuePair) error {
	ezID, hasEzID := ParseEzIDFromProperties(dbProperties)
	if !hasEzID {
		return nil
	}

	for _, property := range dbProperties {
		if property.GetKey() == common.EncryptionRootKeyKey {
			return CreateEZ(ezID, property.GetValue())
		}
	}

	return nil
}

// When creating a new database with encryption eabled, set the ezID to the dbProperties,
// and try to use the the config rootKey if rootKey not provided
// An encrypted DB's properties will contain three properties:
// cipher.enabled, cipher.ezID, cipher.key
//
// Property consistency: DB without cipher.enabled key is NOT encrypted
// - Encrypted DB: has cipher.enabled=true in properties
// - Non-encrypted DB: no cipher.enabled key in properties
func TidyDBCipherProperties(ezID int64, dbProperties []*commonpb.KeyValuePair) ([]*commonpb.KeyValuePair, error) {
	cipher := GetCipher()
	if cipher == nil {
		// No cipher plugin loaded
		if IsDBEncrypted(dbProperties) {
			return nil, ErrCipherPluginMissing
		}
		return dbProperties, nil
	}

	defaultRootKey := paramtable.GetCipherParams().DefaultRootKey.GetValue()

	// Parse user intent from properties
	var explicitlyEnabled, explicitlyDisabled bool
	var userProvidedKey string

	for _, property := range dbProperties {
		if property.Key == common.EncryptionEnabledKey {
			if strings.ToLower(property.Value) == "true" {
				explicitlyEnabled = true
			} else if strings.ToLower(property.Value) == "false" {
				explicitlyDisabled = true
			}
		}
		if property.Key == common.EncryptionRootKeyKey {
			userProvidedKey = property.Value
		}
	}

	// Determine if encryption should be enabled based on decision matrix
	shouldEncrypt := false
	if explicitlyDisabled {
		shouldEncrypt = false
	} else if explicitlyEnabled {
		shouldEncrypt = true
	} else if defaultRootKey != "" {
		// Implicitly enable encryption when defaultKey is set
		shouldEncrypt = true
	}

	// Build result properties (filter out existing cipher properties)
	result := make([]*commonpb.KeyValuePair, 0, len(dbProperties))
	for _, property := range dbProperties {
		if property.Key != common.EncryptionEnabledKey &&
			property.Key != common.EncryptionEzIDKey &&
			property.Key != common.EncryptionRootKeyKey {
			result = append(result, property)
		}
	}

	if !shouldEncrypt {
		// Non-encrypted DB: no cipher.* keys in properties
		return result, nil
	}

	// Encrypted DB: add cipher.enabled, cipher.ezID, cipher.key
	var keyToUse string
	if userProvidedKey != "" {
		keyToUse = userProvidedKey
	} else if defaultRootKey != "" {
		keyToUse = defaultRootKey
	} else {
		return nil, fmt.Errorf("encryption enabled but no key provided and no default key configured")
	}

	result = append(result,
		&commonpb.KeyValuePair{
			Key:   common.EncryptionEnabledKey,
			Value: "true",
		},
		&commonpb.KeyValuePair{
			Key:   common.EncryptionEzIDKey,
			Value: strconv.FormatInt(ezID, 10),
		},
		&commonpb.KeyValuePair{
			Key:   common.EncryptionRootKeyKey,
			Value: keyToUse,
		},
	)

	return result, nil
}

func TidyCollPropsByDBProps(collProps, dbProps []*commonpb.KeyValuePair) []*commonpb.KeyValuePair {
	newCollProps := []*commonpb.KeyValuePair{}
	for _, property := range collProps {
		// Ignore already have ez property, likely from backup collection's schema
		if property.Key == common.EncryptionEzIDKey {
			continue
		}
		newCollProps = append(newCollProps, property)
	}

	// Set the new database's encryption properties
	if ezProps := getCollEzPropsByDBProps(dbProps); ezProps != nil {
		newCollProps = append(newCollProps, ezProps)
	}
	return newCollProps
}

func getCollEzPropsByDBProps(dbProperties []*commonpb.KeyValuePair) *commonpb.KeyValuePair {
	for _, property := range dbProperties {
		if property.Key == common.EncryptionEzIDKey {
			return &commonpb.KeyValuePair{
				Key:   common.EncryptionEzIDKey,
				Value: property.Value,
			}
		}
	}
	return nil
}

func IsDBEncrypted(dbProperties []*commonpb.KeyValuePair) bool {
	for _, property := range dbProperties {
		if property.Key == common.EncryptionEnabledKey && strings.ToLower(property.Value) == "true" {
			return true
		}
	}
	return false
}

func RemoveEZByDBProperties(dbProperties []*commonpb.KeyValuePair) error {
	ezID, has := ParseEzIDFromProperties(dbProperties)
	if !has {
		return nil
	}

	return RemoveEZ(ezID)
}

// GetStoragePluginContext returns the local plugin context for RPC from datacoord to datanode
func GetStoragePluginContext(collProps []*commonpb.KeyValuePair, collectionID int64) []*commonpb.KeyValuePair {
	if ez := GetEzByCollProperties(collProps, collectionID); ez != nil {
		pluginContext, err := GetPluginContext(ez.EzID, ez.CollectionID)
		if err != nil {
			log.Error("failed to get plugin context", zap.Error(err))
			return nil
		}
		return pluginContext
	}
	return nil
}

// Non nill return
func GetReadStoragePluginContext(importEzk string) []*commonpb.KeyValuePair {
	readContext, err := ImportEZ(importEzk)
	if err != nil {
		log.Error("failed to import ezk", zap.Error(err))
		return []*commonpb.KeyValuePair{}
	}
	return readContext
}

// RegisterEZsFromPluginContext registers all EZ contexts from plugin context.
// This processes ALL CipherConfigUnsafeEZK entries (for both read and write contexts).
func RegisterEZsFromPluginContext(context []*commonpb.KeyValuePair) error {
	if !IsClusterEncryptionEnabled() {
		return nil
	}

	for _, value := range context {
		if value.GetKey() == CipherConfigUnsafeEZK {
			ezID, encryptionKey, err := decodeEZContext(value.GetValue())
			if err != nil {
				return err
			}
			if err := CreateLocalEZ(ezID, encryptionKey); err != nil {
				return err
			}
		}
	}
	return nil
}

// GetCPluginContext gets C++ plugin context from the first CipherConfigUnsafeEZK entry.
// Used for creating indexcgopb.StoragePluginContext for C++ segcore.
func GetCPluginContext(context []*commonpb.KeyValuePair, collectionID int64) (*indexcgopb.StoragePluginContext, error) {
	if !IsClusterEncryptionEnabled() {
		return nil, nil
	}

	for _, value := range context {
		if value.GetKey() == CipherConfigUnsafeEZK {
			ezID, encryptionKey, err := decodeEZContext(value.GetValue())
			if err != nil {
				return nil, err
			}

			return &indexcgopb.StoragePluginContext{
				CollectionId:     collectionID,
				EncryptionZoneId: ezID,
				EncryptionKey:    encryptionKey,
			}, nil
		}
	}

	return nil, nil
}

func GetCPluginContextByEzID(ezID int64) (*indexcgopb.StoragePluginContext, error) {
	if !IsClusterEncryptionEnabled() {
		return nil, nil
	}
	key := GetCipher().GetUnsafeKey(ezID, 0)
	if len(key) == 0 {
		return nil, errors.Newf("cannot get ez key for ezID=%d", ezID)
	}
	return &indexcgopb.StoragePluginContext{
		EncryptionZoneId: ezID,
		EncryptionKey:    base64.StdEncoding.EncodeToString(key),
		CollectionId:     0,
	}, nil
}

func BackupEZKFromDBProperties(dbProperties []*commonpb.KeyValuePair) (string, error) {
	if !IsDBEncrypted(dbProperties) {
		return "", fmt.Errorf("not an encryption zone")
	}

	ezID, hasEzID := ParseEzIDFromProperties(dbProperties)
	if !hasEzID {
		return "", fmt.Errorf("encryption enabled but no ezID found")
	}

	return BackupEZ(ezID)
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

	initConfigs := buildCipherInitConfig()
	if err = cipherVal.Init(initConfigs); err != nil {
		return fmt.Errorf("fail to init configs for the cipher plugin, error: %s", err.Error())
	}

	registerCallback()
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

func buildCipherInitConfig() map[string]string {
	initConfigs := lo.Assign(paramtable.Get().EtcdCfg.GetAll(), paramtable.GetCipherParams().GetAll())
	initConfigs[CipherConfigMilvusRoleName] = paramtable.GetRole()
	initConfigs[paramtable.Get().ServiceParam.LocalStorageCfg.Path.Key] = paramtable.Get().ServiceParam.LocalStorageCfg.Path.GetValue()
	return initConfigs
}

func registerCallback() {
	params := paramtable.GetCipherParams()
	params.DefaultRootKey.RegisterCallback(reloadCipherConfig)
	params.KmsAwsRoleARN.RegisterCallback(reloadCipherConfig)
	params.KmsAwsExternalID.RegisterCallback(reloadCipherConfig)
	params.RotationPeriodInHours.RegisterCallback(reloadCipherConfig)
	params.UpdatePerieldInMinutes.RegisterCallback(reloadCipherConfig)
	log.Info("cipher config callbacks registered")
}

func reloadCipherConfig(ctx context.Context, key, oldValue, newValue string) error {
	cipher := GetCipher()
	if cipher == nil {
		log.Warn("cipher plugin not loaded, skip config reload", zap.String("key", key))
		return nil
	}

	cipherReloadMutex.Lock()
	defer cipherReloadMutex.Unlock()

	log.Info("reloading cipher plugin config",
		zap.String("key", key),
		zap.String("oldValue", oldValue),
		zap.String("newValue", newValue))

	initConfigs := buildCipherInitConfig()
	if err := cipher.Init(initConfigs); err != nil {
		log.Error("fail to reload cipher plugin config",
			zap.String("key", key),
			zap.Error(err))
		return err
	}

	log.Info("cipher plugin config reloaded successfully", zap.String("key", key))
	return nil
}

// testCipher encryption will append magicStr to plainText, magicStr is str of ezID and collectionID
type testCipher struct{}

var (
	_ hook.Cipher     = (*testCipher)(nil)
	_ CipherWithState = (*testCipher)(nil)

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

func (d testCipher) GetStates() (map[int64]string, error) {
	return map[int64]string{}, nil
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
