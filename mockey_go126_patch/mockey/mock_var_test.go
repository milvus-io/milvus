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

package mockey

import (
	"fmt"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestVarPatchConvey(t *testing.T) {
	b := 1
	a := 10
	PatchConvey("test mock2", t, func() {
		PatchConvey("test mock3", func() {
			MockValue(&a).To(20)
			So(a, ShouldEqual, 20)
			PatchConvey("test mock4", func() {
				MockValue(&a).To(30)
				MockValue(&b).To(40)
				So(b, ShouldEqual, 40)
				So(a, ShouldEqual, 30)
			})
			So(b, ShouldEqual, 1)
		})
		So(b, ShouldEqual, 1)
		So(a, ShouldEqual, 10)

		PatchConvey("test mock5", func() {
			MockValue(&a).To(30)
			So(a, ShouldEqual, 30)
		})

		So(a, ShouldEqual, 10)
	})
}

type testStruct struct {
	a string
	b int
}

func TestVarStruct(t *testing.T) {
	ttt := &testStruct{
		a: "1",
		b: 2,
	}
	PatchConvey("test mock2", t, func() {
		PatchConvey("test mock3", func() {
			MockValue(&ttt).To(&testStruct{
				a: "2",
				b: 3,
			})
			So(ttt.a, ShouldEqual, "2")
			PatchConvey("test mock3 a", func() {
				MockValue(&ttt).To(&testStruct{
					a: "3",
					b: 3,
				})
				So(ttt.a, ShouldEqual, "3")
			})
			PatchConvey("test mock3 b", func() {
				MockValue(&ttt).To(nil)
				So(ttt, ShouldBeNil)
			})
		})
	})
}

func (t *testStruct) String() string {
	return t.a
}

func TestVarStruct2(t *testing.T) {
	Convey("test mock nil", t, func() {
		var ttt fmt.Stringer
		PatchConvey("test mock3", func() {
			MockValue(&ttt).To(&testStruct{
				a: "2",
				b: 3,
			})
			So(ttt.(*testStruct).a, ShouldEqual, "2")
		})
		So(ttt, ShouldBeNil)
	})
}
