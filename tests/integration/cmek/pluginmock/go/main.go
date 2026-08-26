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
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
)

const (
	createEZKey = "cipher.ez.create"
	unsafeEZK   = "cipher.ezk"
	kmsKeyARN   = "cipher.kmsKeyArn"
)

// CipherPlugin is intentionally a concrete exported value. Go's plugin
// loader returns a pointer to exported variables, so *fixtureCipher must
// implement hook.Cipher for the host type assertion to succeed.
var CipherPlugin = fixtureCipher{
	keys: make(map[int64][]byte),
}

type fixtureCipher struct {
	mu      sync.RWMutex
	keys    map[int64][]byte
	counter uint64
}

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
			key, err = decodeKey(parts[1])
		} else {
			key, err = decodeKey(key)
		}
		if err != nil {
			return fmt.Errorf("fixture cipher: decode EZ key: %w", err)
		}
	} else {
		key = params[kmsKeyARN]
		if key == "" {
			return fmt.Errorf("fixture cipher: missing EZ key for EZ %d", ezID)
		}
		key = "fixture-root/" + key
	}

	c.mu.Lock()
	c.keys[ezID] = []byte(key)
	c.mu.Unlock()
	return nil
}

func (c *fixtureCipher) GetEncryptor(ezID, collectionID int64) (hook.Encryptor, []byte, error) {
	key, ok := c.key(ezID)
	if !ok {
		return nil, nil, fmt.Errorf("fixture cipher: EZ %d is not initialized", ezID)
	}

	c.mu.Lock()
	c.counter++
	sequence := c.counter
	c.mu.Unlock()

	rawEdek := digest(string(key), ezID, collectionID, sequence)
	edek := []byte(hex.EncodeToString(rawEdek))
	return &fixtureEncryptor{key: digestWithEdek(key, ezID, collectionID, edek)}, edek, nil
}

func (c *fixtureCipher) GetDecryptor(ezID, collectionID int64, safeKey []byte) (hook.Decryptor, error) {
	key, ok := c.key(ezID)
	if !ok {
		return nil, fmt.Errorf("fixture cipher: EZ %d is not initialized", ezID)
	}
	if len(safeKey) == 0 {
		return nil, fmt.Errorf("fixture cipher: empty EDEK for EZ %d", ezID)
	}
	return &fixtureDecryptor{key: digestWithEdek(key, ezID, collectionID, safeKey)}, nil
}

func (c *fixtureCipher) GetUnsafeKey(ezID, _ int64) []byte {
	key, ok := c.key(ezID)
	if !ok {
		return nil
	}
	return append([]byte(nil), key...)
}

func (c *fixtureCipher) key(ezID int64) ([]byte, bool) {
	c.mu.RLock()
	key, ok := c.keys[ezID]
	c.mu.RUnlock()
	return append([]byte(nil), key...), ok
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

func digest(key string, ezID, collectionID int64, sequence uint64) []byte {
	return digestBytes([]byte(key), ezID, collectionID, sequence)
}

func digestWithEdek(key []byte, ezID, collectionID int64, edek []byte) []byte {
	seed := append(append([]byte(nil), key...), edek...)
	return digestBytes(seed, ezID, collectionID, 0)
}

func digestBytes(key []byte, ezID, collectionID int64, sequence uint64) []byte {
	h := sha256.New()
	h.Write(key)
	h.Write([]byte(strconv.FormatInt(ezID, 10)))
	h.Write([]byte("/"))
	h.Write([]byte(strconv.FormatInt(collectionID, 10)))
	h.Write([]byte("/"))
	h.Write([]byte(strconv.FormatUint(sequence, 10)))
	return h.Sum(nil)
}

func xor(data, key []byte) []byte {
	result := make([]byte, len(data))
	for i, value := range data {
		result[i] = value ^ key[i%len(key)]
	}
	return result
}
