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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
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
	s.Equal(2, len(result))
	for _, kv := range result {
		switch kv.Key {
		case common.EncryptionEnabledKey:
			s.Fail("unexpected key")
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
	s.False(IsDBEncrypted(dbProperties))

	dbProperties = []*commonpb.KeyValuePair{}
	s.False(IsDBEncrypted(dbProperties))

	dbProperties = []*commonpb.KeyValuePair{
		{Key: common.EncryptionEzIDKey, Value: "1"},
		{Key: common.EncryptionRootKeyKey, Value: "abc"},
	}
	s.True(IsDBEncrypted(dbProperties))
}

func (s *CipherSuite) TestTidyDBCipherPropertiesError() {
	// Reset cipher to nil to test error case
	storeCipher(nil)
	dbProperties := []*commonpb.KeyValuePair{
		{Key: common.EncryptionEnabledKey, Value: "true"},
	}
	_, err := TidyDBCipherProperties(1, dbProperties)
	s.Error(err)
	s.Equal(errCipherPluginMissing, err)
}

func (s *CipherSuite) TestTidyDBCipherPropertiesInvalidValue() {
	InitTestCipher()

	// Test invalid cipher.enabled values
	invalidValues := []string{"1", "0", "yes", "no", "enabled", "disabled", "on", "off", ""}
	for _, invalidValue := range invalidValues {
		props := []*commonpb.KeyValuePair{
			{Key: common.EncryptionEnabledKey, Value: invalidValue},
		}
		_, err := TidyDBCipherProperties(1, props)
		s.Error(err, "expected error for invalid value: %q", invalidValue)
		s.Contains(err.Error(), "invalid value")
		s.Contains(err.Error(), invalidValue)
	}

	// Test valid values should work (case-insensitive)
	validValues := []string{"true", "false", "True", "False", "TRUE", "FALSE", "TrUe", "FaLsE"}
	for _, validValue := range validValues {
		props := []*commonpb.KeyValuePair{
			{Key: common.EncryptionEnabledKey, Value: validValue},
			{Key: common.EncryptionRootKeyKey, Value: "some-key"},
		}
		_, err := TidyDBCipherProperties(1, props)
		s.NoError(err, "expected no error for valid value: %q", validValue)
	}
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
	properties := []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "1"}}
	storeCipher(nil)
	result, err := GetStoragePluginContext(properties, 2)
	s.ErrorIs(err, errCipherPluginMissing)
	s.Nil(result)

	InitTestCipher()
	result, err = GetStoragePluginContext(properties, 2)
	s.Require().NoError(err)
	s.NotNil(result)
	s.Equal(1, len(result))
	s.Equal(CipherConfigUnsafeEZK, result[0].Key)
	// Verify encoded format: "1:base64Key"
	s.Contains(result[0].Value, ":")
	parts := strings.Split(result[0].Value, ":")
	s.Equal("1", parts[0])
	s.Equal(base64.StdEncoding.EncodeToString([]byte("unsafe key")), parts[1])

	result, err = GetStoragePluginContext(nil, 2)
	s.NoError(err)
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
	s.False(IsDBEncrypted(result))
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
	// only has ezID and key
	s.False(hasKeyValue(result, common.EncryptionEnabledKey, "true"))
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
	s.True(IsDBEncrypted(result))
	s.False(hasKeyValue(result, common.EncryptionEnabledKey, "true"))
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
	s.False(IsDBEncrypted(result))
	for _, prop := range result {
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
	s.True(IsDBEncrypted(result))
	s.False(hasKeyValue(result, common.EncryptionEnabledKey, "true"))
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
	s.True(IsDBEncrypted(result))
	s.False(hasKeyValue(result, common.EncryptionEnabledKey, "true"))
	s.True(hasKeyValue(result, common.EncryptionEzIDKey, "107"))
	s.True(hasKeyValue(result, common.EncryptionRootKeyKey, "default-kms-key"))
	s.True(hasKeyValue(result, "other.key", "other.value"))

	// Test 9: defaultKey is set, user provides key, no cipher.enabled -> encrypted with user key
	// This tests that user key overrides defaultKey even when cipher.enabled is not specified
	props = []*commonpb.KeyValuePair{
		{Key: common.EncryptionRootKeyKey, Value: "user-custom-key"},
		{Key: "other.key", Value: "other.value"},
	}
	result, err = TidyDBCipherProperties(108, props)
	s.NoError(err)
	s.True(IsDBEncrypted(result))
	s.False(hasKeyValue(result, common.EncryptionEnabledKey, "true"))
	s.True(hasKeyValue(result, common.EncryptionEzIDKey, "108"))
	s.True(hasKeyValue(result, common.EncryptionRootKeyKey, "user-custom-key"))
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
	s.Equal(errCipherPluginMissing, err)

	InitTestCipher()
	_, err = BackupEZ(123)
	s.Error(err)
	s.Contains(err.Error(), "does not support backup operation")

	storeCipher(importResolverCipher{})
	backup, err := BackupEZ(123)
	s.NoError(err)
	s.Equal("test-backup", backup)
}

func (s *CipherSuite) TestBackupEZKFromDBProperties() {
	storeCipher(nil)
	props := []*commonpb.KeyValuePair{
		{Key: common.EncryptionEzIDKey, Value: "123"},
		{Key: common.EncryptionRootKeyKey, Value: "abc-key"},
	}
	_, err := BackupEZKFromDBProperties(props)
	s.Error(err)
	s.Equal(errCipherPluginMissing, err)

	props = []*commonpb.KeyValuePair{
		{Key: common.EncryptionRootKeyKey, Value: "abc-key"},
	}
	_, err = BackupEZKFromDBProperties(props)
	s.Error(err)
	s.Contains(err.Error(), "not an encryption zone")

	props = []*commonpb.KeyValuePair{
		{Key: common.EncryptionRootKeyKey, Value: "abc-key"},
		{Key: common.EncryptionEzIDKey, Value: "fack"},
	}
	_, err = BackupEZKFromDBProperties(props)
	s.Error(err)
	s.Contains(err.Error(), "no ezID found")
}

type importResolverCipher struct {
	testCipher
	resolve func(string) (int64, []byte, error)
}

func (importResolverCipher) Backup(int64) (string, error) {
	return "test-backup", nil
}

func (c importResolverCipher) ResolveImportEZ(artifact string) (int64, []byte, error) {
	return c.resolve(artifact)
}

func (importResolverCipher) GetUnsafeKey(int64, int64) []byte { return nil }

type temporaryError struct {
	msg       string
	temporary bool
}

func (e temporaryError) Error() string   { return e.msg }
func (e temporaryError) Temporary() bool { return e.temporary }

func (s *CipherSuite) TestImportEZ() {
	result, err := ResolveImportEZ("")
	s.NoError(err)
	s.Nil(result)

	artifact := base64.StdEncoding.EncodeToString([]byte(`{"ez_id":123}`))
	result, err = ResolveImportEZ(artifact)
	s.ErrorIs(err, errCipherPluginMissing)
	s.Nil(result)

	InitTestCipher() // Represents an older plugin without ResolveImportEZ.
	result, err = ResolveImportEZ(artifact)
	s.ErrorContains(err, "ResolveImportEZ")
	s.Nil(result)

	denied := errors.New("source KMS denied Decrypt")
	rotated := base64.StdEncoding.EncodeToString([]byte(`{"ez_id":123,"version":2}`))
	var resolveErr error
	var calls []string
	storeCipher(importResolverCipher{resolve: func(backup string) (int64, []byte, error) {
		calls = append(calls, backup)
		if backup == rotated {
			return 123, []byte("new"), nil
		}
		return 123, []byte{0xfb, 0xff}, resolveErr
	}})
	for _, invalid := range []string{"%%", base64.StdEncoding.EncodeToString([]byte(`{"ez_id":0}`))} {
		_, err = ResolveImportEZ(invalid)
		s.Error(err)
	}
	s.Empty(calls)
	result, err = ResolveImportEZ(artifact)
	s.Require().NoError(err)
	s.Require().Len(result, 1)
	s.Equal(CipherConfigUnsafeEZK, result[0].GetKey())
	s.Equal("123:+/8=", result[0].GetValue())

	result, err = ResolveImportEZ(rotated)
	s.Require().NoError(err)
	s.Require().Len(result, 1)
	s.Equal("123:bmV3", result[0].GetValue())

	resolveErr = denied
	result, err = ResolveImportEZ(artifact)
	s.ErrorIs(err, denied)
	s.False(merr.IsRetryableErr(err))
	s.Nil(result)

	transient := temporaryError{msg: "source KMS throttled", temporary: true}
	resolveErr = errors.Wrap(transient, "kms call failed")
	result, err = ResolveImportEZ(artifact)
	s.ErrorIs(err, merr.ErrServiceUnavailable)
	s.ErrorIs(err, transient)
	s.True(merr.IsRetryableErr(err))
	s.Nil(result)

	nonTransient := temporaryError{msg: "permanent error", temporary: false}
	resolveErr = nonTransient
	result, err = ResolveImportEZ(artifact)
	s.ErrorIs(err, nonTransient)
	s.NotErrorIs(err, merr.ErrServiceUnavailable)
	s.False(merr.IsRetryableErr(err))
	s.Nil(result)

	rateLimit := merr.WrapErrTooManyRequests(10)
	resolveErr = rateLimit
	result, err = ResolveImportEZ(artifact)
	s.ErrorIs(err, merr.ErrServiceTooManyRequests)
	s.NotErrorIs(err, merr.ErrServiceUnavailable)
	s.True(merr.IsRetryableErr(err))
	s.Nil(result)

	resolveErr = fmt.Errorf("rpc call: %w", context.DeadlineExceeded)
	result, err = ResolveImportEZ(artifact)
	s.ErrorIs(err, context.DeadlineExceeded)
	s.True(merr.IsCanceledOrTimeout(err))
	s.NotErrorIs(err, merr.ErrServiceUnavailable)
	s.Nil(result)

	s.Equal([]string{artifact, rotated, artifact, artifact, artifact, artifact, artifact}, calls)
}

func (s *CipherSuite) TestImportTargetEZKRequired() {
	target := []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "123"}}
	result, err := GetImportStoragePluginContext("", target, 7)
	s.ErrorIs(err, errCipherPluginMissing)
	s.Nil(result)

	storeCipher(importResolverCipher{resolve: func(string) (int64, []byte, error) {
		return 123, []byte("source"), nil
	}})
	artifact := base64.StdEncoding.EncodeToString([]byte(`{"ez_id":123}`))
	result, err = GetImportStoragePluginContext(artifact, target, 7)
	s.ErrorIs(err, merr.ErrServiceUnavailable)
	s.ErrorContains(err, "target EZK")
	s.Nil(result)

	target[0].Value = "9223372036854775808"
	result, err = GetImportStoragePluginContext(artifact, target, 7)
	s.ErrorContains(err, "invalid target")
	s.Nil(result)

	InitTestCipher()
	target[0].Value = "123"
	target = append(target, &commonpb.KeyValuePair{Key: common.EncryptionEzIDKey, Value: "456"})
	result, err = GetImportStoragePluginContext("", target, 7)
	s.ErrorContains(err, "duplicate")
	s.Nil(result)
}

func (s *CipherSuite) TestImportEZRejectsInvalidResolverResult() {
	InitOnceCipher()
	artifact := base64.StdEncoding.EncodeToString([]byte(`{"ez_id":123}`))
	for _, tc := range []struct {
		id  int64
		key []byte
	}{
		{123, nil},
		{0, []byte("key")},
		{456, []byte("another EZ's keyset")},
	} {
		storeCipher(importResolverCipher{resolve: func(string) (int64, []byte, error) {
			return tc.id, tc.key, nil
		}})
		result, err := ResolveImportEZ(artifact)
		s.Error(err)
		s.Nil(result)
	}
}

func (s *CipherSuite) TestGetEzIDByImportEzk() {
	for _, payload := range []string{`{}`, `null`, `{"ez_id":0}`, `{"ez_id":-1}`, `{"ez_id":"invalid"}`, `not-json`} {
		_, err := GetEzIDByImportEzk(base64.StdEncoding.EncodeToString([]byte(payload)))
		s.Error(err, "payload %q", payload)
	}
	_, err := GetEzIDByImportEzk("invalid-base64")
	s.Error(err)
	id, err := GetEzIDByImportEzk(base64.StdEncoding.EncodeToString([]byte(`{"ez_id":123}`)))
	s.NoError(err)
	s.Equal(int64(123), id)
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
	s.ErrorIs(err, errCipherPluginMissing)
	s.Nil(result)

	storeCipher(importResolverCipher{})
	result, err = GetPluginContext(123, 456)
	s.ErrorIs(err, merr.ErrServiceUnavailable)
	s.ErrorContains(err, "ezID=123, collectionID=456")
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

type importReadCipher struct {
	testCipher
	calls []map[string]string
	err   error
}

func (c *importReadCipher) Init(cfg map[string]string) error {
	c.calls = append(c.calls, cfg)
	return c.err
}

func (c *importReadCipher) GetUnsafeKey(int64, int64) []byte {
	panic("import must use key material from the request")
}

func TestMalformedEZContextDoesNotEchoInput(t *testing.T) {
	for _, wire := range []string{"private-context", "private-context:key"} {
		_, _, err := decodeEZContext(wire)
		require.Error(t, err)
		require.NotContains(t, err.Error(), "private-context")
	}
}

func TestRegisterImportCipherContexts(t *testing.T) {
	paramtable.Init()
	const collectionID = 35
	artifact := base64.StdEncoding.EncodeToString([]byte(`{"ez_id":7}`))
	schema := []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: "7"}}
	source := &commonpb.KeyValuePair{Key: CipherConfigUnsafeEZK, Value: "7:b2xk"}
	target := &commonpb.KeyValuePair{Key: CipherConfigUnsafeEZK, Value: "7:bmV3"}
	other := &commonpb.KeyValuePair{Key: "other.plugin", Value: "context"}

	for _, tc := range []struct {
		name                   string
		artifact               string
		props                  []*commonpb.KeyValuePair
		wire                   []*commonpb.KeyValuePair
		wantSource, wantTarget bool
	}{
		{"plaintext", "", nil, []*commonpb.KeyValuePair{other}, false, false},
		{"encrypted source", artifact, nil, []*commonpb.KeyValuePair{other, source}, true, false},
		{"encrypted target", "", schema, []*commonpb.KeyValuePair{target, other}, false, true},
		{"same zone on both sides", artifact, schema, []*commonpb.KeyValuePair{source, other, target}, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &importReadCipher{}
			t.Cleanup(SetTestCipher(c))
			source, target, err := ParseImportCipherContexts(tc.artifact, tc.props, tc.wire, collectionID)
			require.NoError(t, err)
			gotSource, err := GetImportSourcePluginContext(tc.artifact, tc.wire)
			require.NoError(t, err)
			require.Equal(t, source, gotSource)
			var want []map[string]string
			if tc.wantSource {
				require.Equal(t, "b2xk", source.GetEncryptionKey())
			} else {
				require.Nil(t, source)
			}
			if tc.wantTarget {
				want = append(want, map[string]string{CipherConfigCreateEZ: "7", CipherConfigUnsafeEZK: "bmV3"})
				require.EqualValues(t, 7, target.GetEncryptionZoneId())
				require.EqualValues(t, collectionID, target.GetCollectionId())
				require.Equal(t, "bmV3", target.GetEncryptionKey())
			} else {
				require.Nil(t, target)
			}
			if tc.wantSource {
				want = append(want, map[string]string{CipherConfigImportEZ: "7", CipherConfigUnsafeEZK: "b2xk"})
			}
			require.Empty(t, c.calls)
			err = RegisterImportCipherContexts(tc.artifact, tc.props, tc.wire, collectionID)
			require.NoError(t, err)
			require.Equal(t, want, c.calls)
		})
	}
}

func TestRegisterImportCipherContextsRejectsInvalidRequest(t *testing.T) {
	paramtable.Init()
	const collectionID = 35
	artifact := base64.StdEncoding.EncodeToString([]byte(`{"ez_id":7}`))
	missingID := base64.StdEncoding.EncodeToString([]byte(`{}`))
	zone := func(s string) []*commonpb.KeyValuePair {
		return []*commonpb.KeyValuePair{{Key: common.EncryptionEzIDKey, Value: s}}
	}
	wire := func(values ...string) []*commonpb.KeyValuePair {
		var kvs []*commonpb.KeyValuePair
		for _, v := range values {
			kvs = append(kvs, &commonpb.KeyValuePair{Key: CipherConfigUnsafeEZK, Value: v})
		}
		return kvs
	}
	for _, tc := range []struct {
		name, artifact string
		props, entries []*commonpb.KeyValuePair
	}{
		{"missing source", artifact, nil, nil},
		{"missing target", "", zone("8"), nil},
		{"too few entries for equal IDs", artifact, zone("7"), wire("7:b2xk")},
		{"extra entry", "", nil, wire("7:b2xk")},
		{"reversed roles", artifact, zone("8"), wire("8:bmV3", "7:b2xk")},
		{"wrong target", artifact, zone("8"), wire("7:b2xk", "7:bmV3")},
		{"invalid schema zone", "", zone("abc"), nil},
		{"duplicate schema zone", "", append(zone("8"), zone("8")...), wire("8:bmV3", "8:bmV3")},
		{"invalid artifact", "%%", nil, nil},
		{"missing artifact ID", missingID, nil, wire("0:b2xk")},
		{"invalid wire format", artifact, nil, wire("7:b2xk:secret")},
		{"invalid target encoding", artifact, zone("8"), wire("7:b2xk", "8:secret!")},
		{"empty keyset", artifact, nil, wire("7:")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &importReadCipher{}
			t.Cleanup(SetTestCipher(c))
			err := RegisterImportCipherContexts(tc.artifact, tc.props, tc.entries, collectionID)
			require.ErrorIs(t, err, merr.ErrImportFailed)
			require.NotContains(t, err.Error(), "secret")
			require.Empty(t, c.calls)
		})
	}
	t.Run("disabled cipher", func(t *testing.T) {
		t.Cleanup(SetTestCipher(nil))
		err := RegisterImportCipherContexts(artifact, nil, wire("7:b2xk"), collectionID)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		err = RegisterImportCipherContexts("", zone("8"), wire("8:bmV3"), collectionID)
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		err = RegisterImportCipherContexts("", nil, nil, collectionID)
		require.NoError(t, err)
	})
	t.Run("incompatible cached read key", func(t *testing.T) {
		conflict := errors.New("CipherPlugin: cannot register ezk, ezID=7, err=conflicting key ID 24 for ezID=7")
		t.Cleanup(SetTestCipher(&importReadCipher{err: conflict}))
		err := RegisterImportCipherContexts(artifact, nil, wire("7:b2xk"), collectionID)
		require.ErrorIs(t, err, conflict)
	})
	t.Run("target failure stops before source", func(t *testing.T) {
		targetErr := merr.WrapErrServiceUnavailable("target key cache unavailable")
		c := &importReadCipher{err: targetErr}
		t.Cleanup(SetTestCipher(c))
		err := RegisterImportCipherContexts(artifact, zone("7"), wire("7:b2xk", "7:bmV3"), collectionID)
		require.ErrorIs(t, err, targetErr)
		require.Equal(t, []map[string]string{{CipherConfigCreateEZ: "7", CipherConfigUnsafeEZK: "bmV3"}}, c.calls)
	})
}
