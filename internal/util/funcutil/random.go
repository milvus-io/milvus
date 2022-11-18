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

package funcutil

import (
	"fmt"
	"math/rand"
	"time"
)

var r *rand.Rand

func init() {
	r = rand.New(rand.NewSource(time.Now().UnixNano()))
}

var letterRunes = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// RandomBytes returns a batch of random string
func RandomBytes(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterRunes[r.Intn(len(letterRunes))]
	}
	return b
}

// RandomString returns a batch of random string
func RandomString(n int) string {
	return string(RandomBytes(n))
}

// GenRandomBytes generates a random bytes.
func GenRandomBytes() []byte {
	l := rand.Uint64()%10 + 1
	b := make([]byte, l)
	if _, err := rand.Read(b); err != nil {
		return nil
	}
	return b
}

func GenRandomBytesWithLength(length int64) []byte {
	b := make([]byte, length)
	if _, err := rand.Read(b); err != nil {
		return nil
	}
	return b
}

// GenRandomStr generates a random string.
func GenRandomStr() string {
	return fmt.Sprintf("%X", GenRandomBytes())
}
