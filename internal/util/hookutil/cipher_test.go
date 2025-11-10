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
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
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
		{Key: EncryptionEzIDKey, Value: "123"},
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
		{Key: EncryptionEnabledKey, Value: "true"},
		{Key: EncryptionRootKeyKey, Value: "existing-root-key"},
	}
	result, err := TidyDBCipherProperties(1, dbPropertiesWithRootKey)
	s.NoError(err)
	s.Equal(3, len(result))
	for _, kv := range result {
		switch kv.Key {
		case EncryptionEnabledKey:
			s.Equal(kv.Value, "true")
		case EncryptionEzIDKey:
			s.Equal(kv.Value, "1")
		case EncryptionRootKeyKey:
			s.Equal(kv.Value, "existing-root-key")
		default:
			s.Fail("unexpected key")
		}
	}

	// Test with encryption enabled and test cipher available
	// Default rootkey is empty
	InitTestCipher()
	dbPropertiesWithoutRootKey := []*commonpb.KeyValuePair{
		{Key: EncryptionEnabledKey, Value: "true"},
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
		{Key: EncryptionEnabledKey, Value: "true"},
	}
	s.True(IsDBEncryptionEnabled(dbProperties))

	dbProperties = []*commonpb.KeyValuePair{}
	s.False(IsDBEncryptionEnabled(dbProperties))
}

func (s *CipherSuite) TestTidyDBCipherPropertiesError() {
	// Reset cipher to nil to test error case
	storeCipher(nil)
	dbProperties := []*commonpb.KeyValuePair{
		{Key: EncryptionEnabledKey, Value: "true"},
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

func (s *CipherSuite) TestIsClusterEncyptionEnabled() {
	// Test when cipher is nil
	storeCipher(nil)
	s.False(IsClusterEncyptionEnabled())

	// Test when cipher is not nil
	InitTestCipher()
	s.True(IsClusterEncyptionEnabled())
}

func (s *CipherSuite) TestContainsCipherProperty() {
	tests := []struct {
		props    []*commonpb.KeyValuePair
		keys     []string
		expected bool
	}{
		{[]*commonpb.KeyValuePair{{Key: EncryptionEnabledKey, Value: "true"}}, nil, true},
		{[]*commonpb.KeyValuePair{{Key: EncryptionEzIDKey, Value: "123"}}, nil, true},
		{[]*commonpb.KeyValuePair{{Key: EncryptionRootKeyKey, Value: "abc"}}, nil, true},
		{nil, []string{EncryptionEnabledKey}, true},
		{nil, []string{EncryptionEzIDKey}, true},
		{nil, []string{EncryptionRootKeyKey}, true},
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
	result := GetStoragePluginContext([]*commonpb.KeyValuePair{{Key: EncryptionEzIDKey, Value: "1"}}, 2)
	s.Nil(result)

	InitTestCipher()
	properties := []*commonpb.KeyValuePair{{Key: EncryptionEzIDKey, Value: "1"}}
	result = GetStoragePluginContext(properties, 2)
	s.NotNil(result)
	s.Equal(2, len(result))
	s.Equal(CipherConfigCreateEZ, result[0].Key)
	s.Equal("1", result[0].Value)
	s.Equal(CipherConfigUnsafeEZK, result[1].Key)
	s.Equal("unsafe key", result[1].Value)

	result = GetStoragePluginContext([]*commonpb.KeyValuePair{}, 2)
	s.Nil(result)
}

func (s *CipherSuite) TestGetDBCipherProperties() {
	props := GetDBCipherProperties(123, "test-kms-key")
	s.Equal(3, len(props))
	s.Equal(EncryptionEnabledKey, props[0].Key)
	s.Equal("true", props[0].Value)
	s.Equal(EncryptionEzIDKey, props[1].Key)
	s.Equal("123", props[1].Value)
	s.Equal(EncryptionRootKeyKey, props[2].Key)
	s.Equal("test-kms-key", props[2].Value)
}

func (s *CipherSuite) TestRemoveEZByDBProperties() {
	storeCipher(nil)
	err := RemoveEZByDBProperties([]*commonpb.KeyValuePair{{Key: EncryptionEzIDKey, Value: "1"}})
	s.NoError(err)

	InitTestCipher()
	err = RemoveEZByDBProperties([]*commonpb.KeyValuePair{{Key: EncryptionEzIDKey, Value: "1"}})
	s.NoError(err)

	err = RemoveEZByDBProperties([]*commonpb.KeyValuePair{{Key: "other", Value: "value"}})
	s.NoError(err)

	err = RemoveEZByDBProperties([]*commonpb.KeyValuePair{})
	s.NoError(err)
}

func (s *CipherSuite) TestCreateLocalEZByPluginContext() {
	storeCipher(nil)
	ctx, err := CreateLocalEZByPluginContext([]*commonpb.KeyValuePair{{Key: CipherConfigCreateEZ, Value: "1"}})
	s.NoError(err)
	s.Nil(ctx)

	InitTestCipher()
	context := []*commonpb.KeyValuePair{
		{Key: CipherConfigCreateEZ, Value: "123"},
		{Key: CipherConfigUnsafeEZK, Value: "test-key"},
	}
	ctx, err = CreateLocalEZByPluginContext(context)
	s.NoError(err)
	s.NotNil(ctx)
	s.Equal(int64(123), ctx.EncryptionZoneId)
	s.Equal("test-key", ctx.EncryptionKey)

	ctx, err = CreateLocalEZByPluginContext([]*commonpb.KeyValuePair{{Key: CipherConfigCreateEZ, Value: "invalid"}})
	s.Error(err)
	s.Nil(ctx)

	ctx, err = CreateLocalEZByPluginContext([]*commonpb.KeyValuePair{{Key: CipherConfigCreateEZ, Value: "1"}})
	s.NoError(err)
	s.Nil(ctx)
}

func (s *CipherSuite) TestCreateEZByDBProperties() {
	storeCipher(nil)
	err := CreateEZByDBProperties([]*commonpb.KeyValuePair{{Key: EncryptionEzIDKey, Value: "1"}})
	s.NoError(err)

	InitTestCipher()
	props := []*commonpb.KeyValuePair{
		{Key: EncryptionEzIDKey, Value: "123"},
		{Key: EncryptionRootKeyKey, Value: "test-root-key"},
	}
	err = CreateEZByDBProperties(props)
	s.NoError(err)

	err = CreateEZByDBProperties([]*commonpb.KeyValuePair{{Key: EncryptionEzIDKey, Value: "1"}})
	s.NoError(err)

	err = CreateEZByDBProperties([]*commonpb.KeyValuePair{})
	s.NoError(err)
}

func (s *CipherSuite) TestGetEzPropByDBProperties() {
	props := []*commonpb.KeyValuePair{{Key: EncryptionEzIDKey, Value: "123"}}
	result := GetEzPropByDBProperties(props)
	s.NotNil(result)
	s.Equal(EncryptionEzIDKey, result.Key)
	s.Equal("123", result.Value)

	result = GetEzPropByDBProperties([]*commonpb.KeyValuePair{{Key: "other", Value: "value"}})
	s.Nil(result)

	result = GetEzPropByDBProperties([]*commonpb.KeyValuePair{})
	s.Nil(result)
}

func (s *CipherSuite) TestGetEzByCollPropertiesInvalidValue() {
	props := []*commonpb.KeyValuePair{{Key: EncryptionEzIDKey, Value: "invalid"}}
	result := GetEzByCollProperties(props, 456)
	s.NotNil(result)
	s.Equal(int64(0), result.EzID)
	s.Equal(int64(456), result.CollectionID)
}

func (s *CipherSuite) TestIsDBEncryptionEnabledCaseInsensitive() {
	props := []*commonpb.KeyValuePair{{Key: EncryptionEnabledKey, Value: "True"}}
	s.True(IsDBEncryptionEnabled(props))

	props = []*commonpb.KeyValuePair{{Key: EncryptionEnabledKey, Value: "TRUE"}}
	s.True(IsDBEncryptionEnabled(props))

	props = []*commonpb.KeyValuePair{{Key: EncryptionEnabledKey, Value: "false"}}
	s.False(IsDBEncryptionEnabled(props))
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

func (s *CipherSuite) TestTidyDBCipherPropertiesWithDefaultKey() {
	initCipherOnce = sync.Once{}
	storeCipher(nil)
	InitTestCipher()

	oldValue := paramtable.GetCipherParams().DefaultRootKey.GetValue()
	defer paramtable.GetCipherParams().DefaultRootKey.SwapTempValue(oldValue)

	paramtable.GetCipherParams().DefaultRootKey.SwapTempValue("default-test-key")

	dbProperties := []*commonpb.KeyValuePair{
		{Key: EncryptionEnabledKey, Value: "true"},
	}
	result, err := TidyDBCipherProperties(1, dbProperties)
	s.NoError(err)
	s.NotNil(result)
	s.Equal(3, len(result))

	foundEzID := false
	foundRootKey := false
	for _, kv := range result {
		if kv.Key == EncryptionEzIDKey {
			s.Equal("1", kv.Value)
			foundEzID = true
		}
		if kv.Key == EncryptionRootKeyKey {
			s.Equal("default-test-key", kv.Value)
			foundRootKey = true
		}
	}
	s.True(foundEzID)
	s.True(foundRootKey)
}

func (s *CipherSuite) TestGetEzByCollPropertiesNotFound() {
	props := []*commonpb.KeyValuePair{
		{Key: "other_key", Value: "value"},
	}
	result := GetEzByCollProperties(props, 456)
	s.Nil(result)
}
