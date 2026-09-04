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

package main

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
)

const (
	createEZKey            = "cipher.ez.create"
	unsafeEZK              = "cipher.ezk"
	kmsKeyARN              = "cipher.kmsKeyArn"
	expectedFixtureRootKey = "fixture-root-key"
	fixtureMasterKey       = "milvus-cmek-fixture-master-v1"
	ezkDomain              = "ezk-v1\x00"
	dekDomain              = "dek-v1\x00"
	edekDomain             = "edek-v1\x00"
	edekVersion            = "v1"
	nonceSize              = 16
)

// CipherPlugin is intentionally a concrete exported value. Go's plugin
// loader returns a pointer to exported variables, so *fixtureCipher must
// implement hook.Cipher for the host type assertion to succeed.
var CipherPlugin = fixtureCipher{}

type fixtureCipher struct{}

func (c *fixtureCipher) Init(params map[string]string) error {
	ezText := params[createEZKey]
	if ezText == "" {
		return nil
	}
	ezID, err := strconv.ParseInt(ezText, 10, 64)
	if err != nil {
		return fmt.Errorf("fixture cipher: invalid EZ id %q: %w", ezText, err)
	}

	key := params[unsafeEZK]
	if key != "" {
		if parts := strings.SplitN(key, ":", 2); len(parts) == 2 {
			contextEZID, parseErr := strconv.ParseInt(parts[0], 10, 64)
			if parseErr != nil || contextEZID != ezID {
				return fmt.Errorf("fixture cipher: unexpected EZ key context %q for EZ %d", parts[0], ezID)
			}
			key, err = decodeKey(parts[1])
		} else {
			key, err = decodeKey(key)
		}
		if err != nil {
			return fmt.Errorf("fixture cipher: decode EZ key: %w", err)
		}
		if !hmac.Equal([]byte(key), deriveEZKey(ezID)) {
			return fmt.Errorf("fixture cipher: unexpected EZ key for EZ %d", ezID)
		}
	} else {
		key = params[kmsKeyARN]
		if key == "" {
			return fmt.Errorf("fixture cipher: missing EZ key for EZ %d", ezID)
		}
		if key != expectedFixtureRootKey {
			return fmt.Errorf("fixture cipher: unexpected root key %q for EZ %d", key, ezID)
		}
	}
	return nil
}

func (c *fixtureCipher) GetEncryptor(ezID, collectionID int64) (hook.Encryptor, []byte, error) {
	nonce := make([]byte, nonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, nil, fmt.Errorf("fixture cipher: generate nonce: %w", err)
	}

	ezk := deriveEZKey(ezID)
	tag := deriveEDEKTag(ezk, nonce, ezID, collectionID)
	edek := []byte(fmt.Sprintf("%s:%s:%s", edekVersion, hex.EncodeToString(nonce), hex.EncodeToString(tag)))
	return &fixtureEncryptor{key: deriveDataKey(ezk, nonce, ezID, collectionID)}, edek, nil
}

func (c *fixtureCipher) GetDecryptor(ezID, collectionID int64, safeKey []byte) (hook.Decryptor, error) {
	nonce, tag, err := decodeEDEK(safeKey)
	if err != nil {
		return nil, fmt.Errorf("fixture cipher: decode EDEK for EZ %d: %w", ezID, err)
	}

	ezk := deriveEZKey(ezID)
	expectedTag := deriveEDEKTag(ezk, nonce, ezID, collectionID)
	if !hmac.Equal(tag, expectedTag) {
		return nil, fmt.Errorf("fixture cipher: EDEK authentication failed for EZ %d and collection %d", ezID, collectionID)
	}
	return &fixtureDecryptor{key: deriveDataKey(ezk, nonce, ezID, collectionID)}, nil
}

func (c *fixtureCipher) GetUnsafeKey(ezID, _ int64) []byte {
	return deriveEZKey(ezID)
}

func deriveEZKey(ezID int64) []byte {
	message := make([]byte, len(ezkDomain)+8)
	copy(message, ezkDomain)
	binary.BigEndian.PutUint64(message[len(ezkDomain):], uint64(ezID))

	return hmacSHA256([]byte(fixtureMasterKey), message)
}

type fixtureEncryptor struct{ key []byte }

func (e *fixtureEncryptor) Encrypt(plainText []byte) ([]byte, error) {
	return xor(plainText, e.key), nil
}

type fixtureDecryptor struct{ key []byte }

func (d *fixtureDecryptor) Decrypt(cipherText []byte) ([]byte, error) {
	return xor(cipherText, d.key), nil
}

func decodeKey(encoded string) (string, error) {
	key, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return "", err
	}
	if len(key) == 0 {
		return "", fmt.Errorf("empty key")
	}
	return string(key), nil
}

func deriveDataKey(ezk, nonce []byte, ezID, collectionID int64) []byte {
	return deriveContextKey(ezk, dekDomain, nonce, ezID, collectionID)
}

func deriveEDEKTag(ezk, nonce []byte, ezID, collectionID int64) []byte {
	return deriveContextKey(ezk, edekDomain, nonce, ezID, collectionID)
}

func deriveContextKey(ezk []byte, domain string, nonce []byte, ezID, collectionID int64) []byte {
	message := make([]byte, len(domain)+len(nonce)+16)
	copy(message, domain)
	copy(message[len(domain):], nonce)
	ids := message[len(domain)+len(nonce):]
	binary.BigEndian.PutUint64(ids, uint64(ezID))
	binary.BigEndian.PutUint64(ids[8:], uint64(collectionID))
	return hmacSHA256(ezk, message)
}

func hmacSHA256(key, message []byte) []byte {
	hash := hmac.New(sha256.New, key)
	_, _ = hash.Write(message)
	return hash.Sum(nil)
}

func decodeEDEK(edek []byte) ([]byte, []byte, error) {
	parts := strings.Split(string(edek), ":")
	if len(parts) != 3 {
		return nil, nil, fmt.Errorf("invalid field count")
	}
	if parts[0] != edekVersion {
		return nil, nil, fmt.Errorf("unsupported version %q", parts[0])
	}
	if parts[1] != strings.ToLower(parts[1]) || parts[2] != strings.ToLower(parts[2]) {
		return nil, nil, fmt.Errorf("fields must use lowercase hex")
	}
	nonce, err := hex.DecodeString(parts[1])
	if err != nil || len(nonce) != nonceSize {
		return nil, nil, fmt.Errorf("invalid nonce")
	}
	tag, err := hex.DecodeString(parts[2])
	if err != nil || len(tag) != sha256.Size {
		return nil, nil, fmt.Errorf("invalid authentication tag")
	}
	return nonce, tag, nil
}

func xor(data, key []byte) []byte {
	result := make([]byte, len(data))
	for i, value := range data {
		result[i] = value ^ key[i%len(key)]
	}
	return result
}
