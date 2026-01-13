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
	"context"
	"encoding/base64"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type CipherSuite struct {
	suite.Suite
}

func TestCipherSuite(t *testing.T) {
	suite.Run(t, new(CipherSuite))
}

func (s *CipherSuite) SetupSuite() {
	paramtable.Init()
}

func (s *CipherSuite) SetupTest() {
	initCipherOnce = sync.Once{}
	storeCipher(nil)
}

func (s *CipherSuite) TestGetCipherNil() {
	s.Nil(GetCipher())
}

func (s *CipherSuite) TestGetTestCipher() {
	InitTestCipher()
	cipher := GetCipher()
	s.NotNil(cipher)
	s.IsType(testCipher{}, cipher)

	ezID, collectionID := int64(1), int64(2)
	encryptor, safeKey, err := GetCipher().GetEncryptor(ezID, collectionID)
	s.NoError(err)
	s.Equal([]byte("safe key"), safeKey)

	plainText := []byte("test plain text")
	cipherText, err := encryptor.Encrypt(plainText)
	s.NoError(err)
	s.Equal(append(plainText, []byte(fmt.Sprintf("%d%d", ezID, collectionID))...), cipherText)

	decryptor, err := GetCipher().GetDecryptor(ezID, collectionID, safeKey)
	s.NoError(err)
	s.NotNil(decryptor)
	gotPlainText, err := decryptor.Decrypt(cipherText)
	s.NoError(err)
	s.Equal(plainText, gotPlainText)

	// test GetUnsafeKey
	s.Equal([]byte("unsafe key"), GetCipher().GetUnsafeKey(1, 2))
}

func (s *CipherSuite) TestGetEzByCollProperties() {
	collProperties := []*commonpb.KeyValuePair{
		{Key: common.EncryptionEzIDKey, Value: "123"},
	}
	result := GetEzByCollProperties(collProperties, 456)
	s.NotNil(result)
	s.Equal(int64(123), result.EzID)
	s.Equal(int64(456), result.CollectionID)

	emptyResult := GetEzByCollProperties([]*commonpb.KeyValuePair{}, 456)
	s.Nil(emptyResult)
}

func (s *CipherSuite) TestTidyDBCipherProperties() {
	InitTestCipher()
	// Test with encryption enabled and root key already present
	dbPropertiesWithRootKey := []*commonpb.KeyValuePair{
		{Key: common.EncryptionEnabledKey, Value: "true"},
		{Key: common.EncryptionRootKeyKey, Value: "existing-root-key"},
	}
	result, err := TidyDBCipherProperties(1, dbPropertiesWithRootKey)
	s.NoError(err)
	s.Equal(3, len(result))
	for _, kv := range result {
		switch kv.Key {
		case common.EncryptionEnabledKey:
			s.Equal(kv.Value, "true")
		case common.EncryptionEzIDKey:
			s.Equal(kv.Value, "1")
		case common.EncryptionRootKeyKey:
			s.Equal(kv.Value, "existing-root-key")
		default:
			s.Fail("unexpected key")
		}
	}

	// Test with encryption enabled and test cipher available
	// Default rootkey is empty
	InitTestCipher()
	dbPropertiesWithoutRootKey := []*commonpb.KeyValuePair{
		{Key: common.EncryptionEnabledKey, Value: "true"},
	}
	result, err = TidyDBCipherProperties(1, dbPropertiesWithoutRootKey)
	s.Error(err)
	s.Nil(result)

	// Test without encryption enabled
	dbPropertiesWithoutEncryption := []*commonpb.KeyValuePair{}
	result, err = TidyDBCipherProperties(1, dbPropertiesWithoutEncryption)
	s.NoError(err)
	s.NotNil(result)
	s.Equal(dbPropertiesWithoutEncryption, result)
}

func (s *CipherSuite) TestIsDBEncryptionEnabled() {
	dbProperties := []*commonpb.KeyValuePair{
		{Key: common.EncryptionEnabledKey, Value: "true"},
	}
	s.True(IsDBEncrypted(dbProperties))

	dbProperties = []*commonpb.KeyValuePair{}
	s.False(IsDBEncrypted(dbProperties))
}

func (s *CipherSuite) TestTidyDBCipherPropertiesError() {
	// Reset cipher to nil to test error case
	storeCipher(nil)
	dbProperties := []*commonpb.KeyValuePair{
		{Key: common.EncryptionEnabledKey, Value: "true"},
	}
	_, err := TidyDBCipherProperties(1, dbProperties)
	s.Error(err)
	s.Equal(ErrCipherPluginMissing, err)
}

func (s *CipherSuite) TestTestCipherInit() {
	cipher := testCipher{}
	err := cipher.Init(map[string]string{"key": "value"})
	s.NoError(err)
}

func (s *CipherSuite) TestIsClusterEncryptionEnabled() {
	// Test when cipher is nil
	storeCipher(nil)
	s.False(IsClusterEncryptionEnabled())

	// Test when cipher is not nil
	InitTestCipher()
	s.True(IsClusterEncryptionEnabled())
}

func (s *CipherSuite) TestContainsCipherProperty() {
	tests := []struct {
		props    []*commonpb.KeyValuePair
		keys     []string
		expected bool
	}{
		{[]*commonpb.KeyValuePair{{Key: common.EncryptionEnabledKey, Value: "true"}}, nil, true},
		{[]*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "123"}}, nil, true},
		{[]*commonpb.KeyValuePair{{Key: common.EncryptionRootKeyKey, Value: "abc"}}, nil, true},
		{nil, []string{common.EncryptionEnabledKey}, true},
		{nil, []string{common.EncryptionEzIDKey}, true},
		{nil, []string{common.EncryptionRootKeyKey}, true},
		{[]*commonpb.KeyValuePair{{Key: "key1", Value: "value1"}}, []string{"others"}, false},
	}

	for _, test := range tests {
		s.Equal(test.expected, ContainsCipherProperties(test.props, test.keys))
	}
}

func (s *CipherSuite) TestBuildCipherInitConfig() {
	config := buildCipherInitConfig()
	s.NotNil(config)
	s.Contains(config, CipherConfigMilvusRoleName)
}

func (s *CipherSuite) TestReloadCipherConfig() {
	InitTestCipher()

	ctx := context.Background()
	err := reloadCipherConfig(ctx, "test.key", "oldValue", "newValue")
	s.NoError(err)

	storeCipher(nil)
	err = reloadCipherConfig(ctx, "test.key", "oldValue", "newValue")
	s.NoError(err)
}

func (s *CipherSuite) TestNewKmsConfigFields() {
	params := paramtable.GetCipherParams()

	s.NotNil(params.DefaultRootKey.GetValue())
	s.NotNil(params.KmsAwsRoleARN.GetValue())
	s.NotNil(params.KmsAwsExternalID.GetValue())
	s.NotNil(params.RotationPeriodInHours.GetValue())
	s.NotNil(params.UpdatePerieldInMinutes.GetValue())

	s.Equal("cipherPlugin.kms.defaultKey", params.DefaultRootKey.Key)
	s.Equal("cipherPlugin.kms.credentials.aws.roleARN", params.KmsAwsRoleARN.Key)
	s.Equal("cipherPlugin.kms.credentials.aws.externalID", params.KmsAwsExternalID.Key)
}

func (s *CipherSuite) TestAsMessageConfig() {
	ez := &EZ{EzID: 123, CollectionID: 456}
	config := ez.AsMessageConfig()
	s.NotNil(config)
	s.Equal(int64(123), config.EzID)
	s.Equal(int64(456), config.CollectionID)

	var nilEz *EZ
	s.Nil(nilEz.AsMessageConfig())
}

func (s *CipherSuite) TestGetStoragePluginContext() {
	storeCipher(nil)
	result := GetStoragePluginContext([]*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "1"}}, 2)
	s.Nil(result)

	InitTestCipher()
	properties := []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "1"}}
	result = GetStoragePluginContext(properties, 2)
	s.NotNil(result)
	s.Equal(1, len(result))
	s.Equal(CipherConfigUnsafeEZK, result[0].Key)
	// Verify encoded format: "1:base64Key"
	s.Contains(result[0].Value, ":")
	parts := strings.Split(result[0].Value, ":")
	s.Equal("1", parts[0])
	s.Equal(base64.StdEncoding.EncodeToString([]byte("unsafe key")), parts[1])

	result = GetStoragePluginContext([]*commonpb.KeyValuePair{}, 2)
	s.Nil(result)
}

func (s *CipherSuite) TestRemoveEZByDBProperties() {
	storeCipher(nil)
	err := RemoveEZByDBProperties([]*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "1"}})
	s.NoError(err)

	InitTestCipher()
	err = RemoveEZByDBProperties([]*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "1"}})
	s.NoError(err)

	err = RemoveEZByDBProperties([]*commonpb.KeyValuePair{{Key: "other", Value: "value"}})
	s.NoError(err)

	err = RemoveEZByDBProperties([]*commonpb.KeyValuePair{})
	s.NoError(err)
}

func (s *CipherSuite) TestCreateEZByDBProperties() {
	storeCipher(nil)
	err := CreateEZByDBProperties([]*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "1"}})
	s.NoError(err)

	InitTestCipher()
	props := []*commonpb.KeyValuePair{
		{Key: common.EncryptionEzIDKey, Value: "123"},
		{Key: common.EncryptionRootKeyKey, Value: "test-root-key"},
	}
	err = CreateEZByDBProperties(props)
	s.NoError(err)

	err = CreateEZByDBProperties([]*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "1"}})
	s.NoError(err)

	err = CreateEZByDBProperties([]*commonpb.KeyValuePair{})
	s.NoError(err)
}

func (s *CipherSuite) TestGetEzByCollPropertiesInvalidValue() {
	props := []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "invalid"}}
	result := GetEzByCollProperties(props, 456)
	s.NotNil(result)
	s.Equal(int64(0), result.EzID)
	s.Equal(int64(456), result.CollectionID)
}

func (s *CipherSuite) TestIsDBEncryptionEnabledCaseInsensitive() {
	props := []*commonpb.KeyValuePair{{Key: common.EncryptionEnabledKey, Value: "True"}}
	s.True(IsDBEncrypted(props))
}

func (s *CipherSuite) TestTidyDBCipherPropertiesWithDefaultKey() {
	InitTestCipher()

	// Save original config and restore after test
	originalDefaultKey := paramtable.GetCipherParams().DefaultRootKey.GetValue()
	defer func() {
		paramtable.GetCipherParams().Save("cipherPlugin.kms.defaultKey", originalDefaultKey)
	}()

	// Test 1: defaultKey is empty, no cipher.enabled in request -> not encrypted
	paramtable.GetCipherParams().Save("cipherPlugin.kms.defaultKey", "")
	props := []*commonpb.KeyValuePair{
		{Key: "other.key", Value: "other.value"},
	}
	result, err := TidyDBCipherProperties(100, props)
	s.NoError(err)
	s.Equal(1, len(result))
	s.Equal("other.key", result[0].Key)
	// Verify no cipher properties
	for _, prop := range result {
		s.NotEqual(common.EncryptionEnabledKey, prop.Key)
		s.NotEqual(common.EncryptionEzIDKey, prop.Key)
		s.NotEqual(common.EncryptionRootKeyKey, prop.Key)
	}

	// Test 2: defaultKey is empty, cipher.enabled=false -> not encrypted
	props = []*commonpb.KeyValuePair{
		{Key: common.EncryptionEnabledKey, Value: "false"},
		{Key: "other.key", Value: "other.value"},
	}
	result, err = TidyDBCipherProperties(101, props)
	s.NoError(err)
	// Verify no cipher properties
	for _, prop := range result {
		s.NotEqual(common.EncryptionEnabledKey, prop.Key)
		s.NotEqual(common.EncryptionEzIDKey, prop.Key)
		s.NotEqual(common.EncryptionRootKeyKey, prop.Key)
	}

	// Test 3: defaultKey is empty, cipher.enabled=true, user provides key -> encrypted with user key
	props = []*commonpb.KeyValuePair{
		{Key: common.EncryptionEnabledKey, Value: "true"},
		{Key: common.EncryptionRootKeyKey, Value: "user-key"},
		{Key: "other.key", Value: "other.value"},
	}
	result, err = TidyDBCipherProperties(102, props)
	s.NoError(err)
	s.True(hasKeyValue(result, common.EncryptionEnabledKey, "true"))
	s.True(hasKeyValue(result, common.EncryptionEzIDKey, "102"))
	s.True(hasKeyValue(result, common.EncryptionRootKeyKey, "user-key"))
	s.True(hasKeyValue(result, "other.key", "other.value"))

	// Test 4: defaultKey is empty, cipher.enabled=true, no user key -> error
	props = []*commonpb.KeyValuePair{
		{Key: common.EncryptionEnabledKey, Value: "true"},
	}
	_, err = TidyDBCipherProperties(103, props)
	s.Error(err)
	s.Contains(err.Error(), "no key provided")

	// Test 5: defaultKey is set, no cipher.enabled in request -> encrypted with defaultKey
	paramtable.GetCipherParams().Save("cipherPlugin.kms.defaultKey", "default-kms-key")
	props = []*commonpb.KeyValuePair{
		{Key: "other.key", Value: "other.value"},
	}
	result, err = TidyDBCipherProperties(104, props)
	s.NoError(err)
	s.True(hasKeyValue(result, common.EncryptionEnabledKey, "true"))
	s.True(hasKeyValue(result, common.EncryptionEzIDKey, "104"))
	s.True(hasKeyValue(result, common.EncryptionRootKeyKey, "default-kms-key"))
	s.True(hasKeyValue(result, "other.key", "other.value"))

	// Test 6: defaultKey is set, cipher.enabled=false -> not encrypted
	props = []*commonpb.KeyValuePair{
		{Key: common.EncryptionEnabledKey, Value: "false"},
		{Key: "other.key", Value: "other.value"},
	}
	result, err = TidyDBCipherProperties(105, props)
	s.NoError(err)
	// Verify no cipher properties
	for _, prop := range result {
		s.NotEqual(common.EncryptionEnabledKey, prop.Key)
		s.NotEqual(common.EncryptionEzIDKey, prop.Key)
		s.NotEqual(common.EncryptionRootKeyKey, prop.Key)
	}
	s.True(hasKeyValue(result, "other.key", "other.value"))

	// Test 7: defaultKey is set, cipher.enabled=true with user key -> encrypted with user key
	props = []*commonpb.KeyValuePair{
		{Key: common.EncryptionEnabledKey, Value: "true"},
		{Key: common.EncryptionRootKeyKey, Value: "user-override-key"},
		{Key: "other.key", Value: "other.value"},
	}
	result, err = TidyDBCipherProperties(106, props)
	s.NoError(err)
	s.True(hasKeyValue(result, common.EncryptionEnabledKey, "true"))
	s.True(hasKeyValue(result, common.EncryptionEzIDKey, "106"))
	s.True(hasKeyValue(result, common.EncryptionRootKeyKey, "user-override-key"))
	s.True(hasKeyValue(result, "other.key", "other.value"))

	// Test 8: defaultKey is set, cipher.enabled=true without user key -> encrypted with defaultKey
	props = []*commonpb.KeyValuePair{
		{Key: common.EncryptionEnabledKey, Value: "true"},
		{Key: "other.key", Value: "other.value"},
	}
	result, err = TidyDBCipherProperties(107, props)
	s.NoError(err)
	s.True(hasKeyValue(result, common.EncryptionEnabledKey, "true"))
	s.True(hasKeyValue(result, common.EncryptionEzIDKey, "107"))
	s.True(hasKeyValue(result, common.EncryptionRootKeyKey, "default-kms-key"))
	s.True(hasKeyValue(result, "other.key", "other.value"))
}

// Helper function to check if a key-value pair exists in the result
func hasKeyValue(props []*commonpb.KeyValuePair, key, value string) bool {
	for _, prop := range props {
		if prop.Key == key && prop.Value == value {
			return true
		}
	}
	return false
}

func (s *CipherSuite) TestIsDBEncryptionEnabledVariousCases() {
	props := []*commonpb.KeyValuePair{{Key: common.EncryptionEnabledKey, Value: "TRUE"}}
	s.True(IsDBEncrypted(props))

	props = []*commonpb.KeyValuePair{{Key: common.EncryptionEnabledKey, Value: "false"}}
	s.False(IsDBEncrypted(props))
}

func (s *CipherSuite) TestRegisterCallback() {
	initCipherOnce = sync.Once{}
	storeCipher(nil)
	InitTestCipher()

	registerCallback()

	ctx := context.Background()
	err := reloadCipherConfig(ctx, "test.key", "old", "new")
	s.NoError(err)
}

func (s *CipherSuite) TestGetEzByCollPropertiesNotFound() {
	props := []*commonpb.KeyValuePair{
		{Key: "other_key", Value: "value"},
	}
	result := GetEzByCollProperties(props, 456)
	s.Nil(result)
}

func (s *CipherSuite) TestBackupEZ() {
	storeCipher(nil)
	_, err := BackupEZ(123)
	s.Error(err)
	s.Equal(ErrCipherPluginMissing, err)

	InitTestCipher()
	_, err = BackupEZ(123)
	s.Error(err)
	s.Contains(err.Error(), "does not support backup operation")
}

func (s *CipherSuite) TestBackupEZKFromDBProperties() {
	storeCipher(nil)
	props := []*commonpb.KeyValuePair{
		{Key: common.EncryptionEnabledKey, Value: "true"},
		{Key: common.EncryptionEzIDKey, Value: "123"},
	}
	_, err := BackupEZKFromDBProperties(props)
	s.Error(err)
	s.Equal(ErrCipherPluginMissing, err)

	props = []*commonpb.KeyValuePair{
		{Key: common.EncryptionEnabledKey, Value: "false"},
	}
	_, err = BackupEZKFromDBProperties(props)
	s.Error(err)
	s.Contains(err.Error(), "not an encryption zone")

	props = []*commonpb.KeyValuePair{
		{Key: common.EncryptionEnabledKey, Value: "true"},
	}
	_, err = BackupEZKFromDBProperties(props)
	s.Error(err)
	s.Contains(err.Error(), "no ezID found")
}

func (s *CipherSuite) TestImportEZ() {
	storeCipher(nil)
	result, err := ImportEZ("")
	s.NoError(err)
	s.Nil(result)

	result, err = ImportEZ("some-base64-string")
	s.NoError(err)
	s.Nil(result)

	InitTestCipher()
	result, err = ImportEZ("invalid-base64")
	s.Error(err)
	s.Nil(result)
	s.Contains(err.Error(), "failed to decode EZK")

	result, err = ImportEZ(base64.StdEncoding.EncodeToString([]byte("not-json")))
	s.Error(err)
	s.Nil(result)
	s.Contains(err.Error(), "failed to unmarshal EZK")

	ezkJSON := `{"ez_id":123}`
	ezkBase64 := base64.StdEncoding.EncodeToString([]byte(ezkJSON))
	result, err = ImportEZ(ezkBase64)
	s.NoError(err)
	s.NotNil(result)
	s.Equal(1, len(result))
	s.Equal(CipherConfigUnsafeEZK, result[0].Key)
	// Verify encoded format: "123:base64Key"
	s.Contains(result[0].Value, ":")
	parts := strings.Split(result[0].Value, ":")
	s.Equal("123", parts[0])
	s.Equal(base64.StdEncoding.EncodeToString([]byte("unsafe key")), parts[1])
}

func (s *CipherSuite) TestIsEncryptionEnabled() {
	storeCipher(nil)
	s.False(IsClusterEncryptionEnabled())

	InitTestCipher()
	s.True(IsClusterEncryptionEnabled())
}

func (s *CipherSuite) TestParseEzIDFromProperties() {
	props := []*commonpb.KeyValuePair{
		{Key: common.EncryptionEzIDKey, Value: "123"},
	}
	ezID, found := ParseEzIDFromProperties(props)
	s.True(found)
	s.Equal(int64(123), ezID)

	props = []*commonpb.KeyValuePair{
		{Key: "other_key", Value: "value"},
	}
	ezID, found = ParseEzIDFromProperties(props)
	s.False(found)
	s.Equal(int64(0), ezID)

	props = []*commonpb.KeyValuePair{
		{Key: common.EncryptionEzIDKey, Value: "invalid"},
	}
	ezID, found = ParseEzIDFromProperties(props)
	s.False(found)
	s.Equal(int64(0), ezID)
}

func (s *CipherSuite) TestCreateEZ() {
	storeCipher(nil)
	err := CreateEZ(123, "test-kms-key")
	s.NoError(err)

	InitTestCipher()
	err = CreateEZ(456, "test-kms-key")
	s.NoError(err)
}

func (s *CipherSuite) TestCreateLocalEZ() {
	storeCipher(nil)
	err := CreateLocalEZ(123, "test-encryption-key")
	s.NoError(err)

	InitTestCipher()
	err = CreateLocalEZ(456, "test-encryption-key")
	s.NoError(err)
}

func (s *CipherSuite) TestRemoveEZ() {
	storeCipher(nil)
	err := RemoveEZ(123)
	s.NoError(err)

	InitTestCipher()
	err = RemoveEZ(456)
	s.NoError(err)
}

func (s *CipherSuite) TestGetPluginContext() {
	storeCipher(nil)
	result, err := GetPluginContext(123, 456)
	s.NoError(err)
	s.Nil(result)

	InitTestCipher()
	result, err = GetPluginContext(123, 456)
	s.NoError(err)
	s.NotNil(result)
	s.Equal(1, len(result))
	s.Equal(CipherConfigUnsafeEZK, result[0].Key)
	// Verify encoded format: "123:base64Key"
	s.Contains(result[0].Value, ":")
	parts := strings.Split(result[0].Value, ":")
	s.Equal("123", parts[0])
	s.Equal(base64.StdEncoding.EncodeToString([]byte("unsafe key")), parts[1])
}
