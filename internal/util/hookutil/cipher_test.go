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
	"fmt"
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
	// Test with encryption enabled and root key already present
	dbPropertiesWithRootKey := []*commonpb.KeyValuePair{
		{Key: EncryptionEnabledKey, Value: "true"},
		{Key: EncryptionRootKeyKey, Value: "existing-root-key"},
	}
	result, err := TidyDBCipherProperties(dbPropertiesWithRootKey)
	s.NoError(err)
	s.Equal(dbPropertiesWithRootKey, result)

	// Test with encryption enabled and test cipher available
	InitTestCipher()
	dbPropertiesWithoutRootKey := []*commonpb.KeyValuePair{
		{Key: EncryptionEnabledKey, Value: "true"},
	}
	result, err = TidyDBCipherProperties(dbPropertiesWithoutRootKey)
	s.NoError(err)
	s.Len(result, 2) // should have EncryptionEnabledKey + added default root key
	s.Equal(EncryptionEnabledKey, result[0].Key)
	s.Equal(EncryptionRootKeyKey, result[1].Key)

	// Test without encryption enabled
	dbPropertiesWithoutEncryption := []*commonpb.KeyValuePair{}
	result, err = TidyDBCipherProperties(dbPropertiesWithoutEncryption)
	s.NoError(err)
	s.NotNil(result)
	s.Equal(dbPropertiesWithoutEncryption, result)
}

func (s *CipherSuite) TestIsDBEncyptionEnabled() {
	dbProperties := []*commonpb.KeyValuePair{
		{Key: EncryptionEnabledKey, Value: "true"},
	}
	s.True(IsDBEncyptionEnabled(dbProperties))

	dbProperties = []*commonpb.KeyValuePair{}
	s.False(IsDBEncyptionEnabled(dbProperties))
}

func (s *CipherSuite) TestGetEZRootKeyByDBProperties() {
	dbProperties := []*commonpb.KeyValuePair{
		{Key: EncryptionRootKeyKey, Value: "rootKey"},
	}
	rootKey := GetEZRootKeyByDBProperties(dbProperties)
	s.Equal("rootKey", rootKey)

	emptyProperties := []*commonpb.KeyValuePair{}
	defaultRootKey := GetEZRootKeyByDBProperties(emptyProperties)
	s.Equal(paramtable.GetCipherParams().DefaultRootKey.GetValue(), defaultRootKey)
}

func (s *CipherSuite) TestTidyDBCipherPropertiesError() {
	// Reset cipher to nil to test error case
	storeCipher(nil)
	dbProperties := []*commonpb.KeyValuePair{
		{Key: EncryptionEnabledKey, Value: "true"},
	}
	_, err := TidyDBCipherProperties(dbProperties)
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
