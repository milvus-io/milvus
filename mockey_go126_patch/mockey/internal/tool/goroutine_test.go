/*
 * Copyright 2022 ByteDance Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tool

import (
	"bytes"
	"runtime"
	"strconv"
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestGetGoroutineID(t *testing.T) {
	getSlow := func() int64 {
		b := make([]byte, 64)
		b = b[:runtime.Stack(b, false)]
		b = bytes.TrimPrefix(b, []byte("goroutine "))
		b = b[:bytes.IndexByte(b, ' ')]
		n, _ := strconv.ParseInt(string(b), 10, 64)
		return n
	}
	convey.Convey("TestGetGoroutineID", t, func() {
		gid1 := GetGoroutineID()
		gid2 := getSlow()
		convey.So(gid1, convey.ShouldEqual, gid2)
	})
}
