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
	"reflect"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func Target(in string) string {
	return strings.Repeat(in, 1)
}

func Hook(in string) string {
	return "MOCKED!"
}

func TestPatchFunc(t *testing.T) {
	Convey("TestPatchFunc", t, func() {
		Convey("normal", func() {
			var proxy func(string) string
			patch := PatchFunc(Target, Hook, &proxy, false)
			So(Target("anything"), ShouldEqual, "MOCKED!")
			So(proxy("anything"), ShouldEqual, "anything")
			patch.Unpatch()
			So(Target("anything"), ShouldEqual, "anything")
		})
		Convey("anonymous hook", func() {
			var proxy func(string) string
			patch := PatchFunc(Target, func(string) string { return "MOCKED!" }, &proxy, false)
			So(Target("anything"), ShouldEqual, "MOCKED!")
			So(proxy("anything"), ShouldEqual, "anything")
			patch.Unpatch()
			So(Target("anything"), ShouldEqual, "anything")
		})
		Convey("closure hook", func() {
			var proxy func(string) string
			hookBuilder := func(x string) func(string) string {
				return func(string) string { return x }
			}
			patch := PatchFunc(Target, hookBuilder("MOCKED!"), &proxy, false)
			So(Target("anything"), ShouldEqual, "MOCKED!")
			So(proxy("anything"), ShouldEqual, "anything")
			patch.Unpatch()
			So(Target("anything"), ShouldEqual, "anything")
		})
		Convey("reflect hook", func() {
			var proxy func(string) string
			hookVal := reflect.MakeFunc(reflect.TypeOf(Hook), func(args []reflect.Value) (results []reflect.Value) {
				return []reflect.Value{reflect.ValueOf("MOCKED!")}
			})
			patch := PatchFunc(Target, hookVal.Interface(), &proxy, false)
			So(Target("anything"), ShouldEqual, "MOCKED!")
			So(proxy("anything"), ShouldEqual, "anything")
			patch.Unpatch()
			So(Target("anything"), ShouldEqual, "anything")
		})
	})
}
