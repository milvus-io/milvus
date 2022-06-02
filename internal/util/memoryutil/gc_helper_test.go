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

package memoryutil

import (
	"math/rand"
	"runtime"
	"testing"
	"time"
)

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func RandStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func GenerateMap(entries int) map[int]string {
	m := map[int]string{}
	for i := 0; i <= entries; i++ {
		m[i] = RandStringRunes(10000)
	}
	return m
}
func Test_GoGCHelper(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Error("The code paniced", r)
		}
	}()
	rand.Seed(time.Now().UnixNano())

	ch := make(chan int)
	// hook
	action := func(GOGC int) {
		ch <- GOGC
	}

	go NewHelper(0.3, 50, 200, action)
	// TODO need more test on it
	runtime.GC()
}
