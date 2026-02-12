/*
 * Copyright 2023 ByteDance Inc.
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

package unsafereflect

import (
	"crypto/sha256"
	"reflect"
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func TestMethodByName(t *testing.T) {
	convey.Convey("MethodByName", t, func() {
		inst := sha256.New()
		// private structure private method: *sha256.digest.checkSum
		typ, fn, ok := MethodByName(reflect.TypeOf(inst), "checkSum")
		convey.So(ok, convey.ShouldBeTrue)
		convey.So(fn, convey.ShouldNotBeZeroValue)
		convey.So(typ, convey.ShouldEqual, reflect.TypeOf(func() [sha256.Size]byte { return [sha256.Size]byte{} }))
	})
}
