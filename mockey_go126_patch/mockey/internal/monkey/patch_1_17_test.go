//go:build go1.17 && !go1.18
// +build go1.17,!go1.18

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

package monkey

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func UnsafeTarget() {}

func TestPatchUnsafeFunc(t *testing.T) {
	Convey("TestPatchUnsafeFunc", t, func() {
		var proxy func()
		var hook = func() { panic("good") }
		Convey("normal", func() {
			So(func() { PatchFunc(UnsafeTarget, hook, &proxy, false) }, ShouldPanicWith, "function is too short to patch")
		})
		Convey("with unsafe", func() {
			patch := PatchFunc(UnsafeTarget, hook, &proxy, true)
			So(func() { UnsafeTarget() }, ShouldPanicWith, "good")
			So(func() { proxy() }, ShouldNotPanic)
			patch.Unpatch()
			So(func() { UnsafeTarget() }, ShouldNotPanic)
		})
	})
}
