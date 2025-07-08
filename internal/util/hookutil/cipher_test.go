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
