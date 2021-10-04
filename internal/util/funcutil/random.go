// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package funcutil

import (
	"fmt"
	"math/rand"
	"time"
)

var r *rand.Rand = nil

func init() {
	r = rand.New(rand.NewSource(time.Now().UnixNano()))
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// RandomString returns a batch of random string
func RandomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[r.Intn(len(letterRunes))]
	}
	return string(b)
}

// GenRandomStr generates a random string.
func GenRandomStr() string {
	l := rand.Uint64()%10 + 1
	b := make([]byte, l)
	if _, err := rand.Read(b); err != nil {
		return ""
	}
	return fmt.Sprintf("%X", b)
}
