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

package fn

import (
	"math"
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

func callSystemFunc(msg string) int

func callTwoGoFunc(a int) int {
	b := math.Abs(float64(a))
	c := math.Ilogb(b)
	return c
}

func callTwoAsmFunc() (uint, uint)

func asmFunc1() uint

func asmFunc2() uint

var (
	callSystemFuncStub = func(string) int { return 0 }
	callTwoGoFuncStub  = func(int) int { return 0 }
	callTwoAsmFuncStub = func() (uint, uint) { return 0, 0 }
)

func TestCopy(t *testing.T) {
	Copy(&callSystemFuncStub, callSystemFunc)
	Copy(&callTwoGoFuncStub, callTwoGoFunc)
	Copy(&callTwoAsmFuncStub, callTwoAsmFunc)

	convey.Convey("TestCopy", t, func() {
		callSystemFunc("hello ")
		callSystemFuncStub("world!\n")

		a1 := callTwoGoFunc(-10)
		a2 := callTwoGoFuncStub(-10)
		convey.So(a1, convey.ShouldEqual, 3)
		convey.So(a2, convey.ShouldEqual, 3)

		b1, c1 := callTwoAsmFunc()
		b2, c2 := callTwoAsmFuncStub()
		convey.So(b1, convey.ShouldEqual, 1)
		convey.So(c1, convey.ShouldEqual, 2)
		convey.So(b2, convey.ShouldEqual, 1)
		convey.So(c2, convey.ShouldEqual, 2)
	})
}
