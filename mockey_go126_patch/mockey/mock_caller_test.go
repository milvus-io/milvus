// Copyright 2023 2022 ByteDance Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package mockey

import (
	"fmt"
	"strings"
	"testing"

	"github.com/bytedance/mockey/internal/tool"
	"github.com/smartystreets/goconvey/convey"
)

func callerFunc() {
	panic("CallerFunc")
}

type callerStruct struct {
	_ int
}

func (c *callerStruct) Foo() {
	panic("CallerStruct")
}

var callerValue string

func TestReMockPanic(t *testing.T) {
	PatchConvey("TestReMockPanic", t, func() {
		PatchConvey("callerFunc", func() {
			mocker := Mock(callerFunc).To(func() { fmt.Println("should not panic") }).Build()
			mocker.To(func() { fmt.Println("should also not panic") })
			lastCaller := tool.Caller()
			lastCaller.Line -= 1
			var err interface{}
			func() {
				defer func() { err = recover() }()
				Mock(callerFunc).To(func() { fmt.Println("should panic, but recovered") }).Build()
			}()
			errString, ok := err.(string)
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(strings.Contains(errString, lastCaller.String()), convey.ShouldBeTrue)
		})
		PatchConvey("callerStruct", func() {
			mocker := Mock((*callerStruct).Foo).To(func() { fmt.Println("should not panic") }).Build()
			mocker.To(func() { fmt.Println("should also not panic") })
			lastCaller := tool.Caller()
			lastCaller.Line -= 1
			var err interface{}
			func() {
				defer func() { err = recover() }()
				Mock((*callerStruct).Foo).To(func() { fmt.Println("should panic, but recovered") }).Build()
			}()
			errString, ok := err.(string)
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(strings.Contains(errString, lastCaller.String()), convey.ShouldBeTrue)
		})
		PatchConvey("callerValue", func() {
			MockValue(&callerValue).To("should not panic")
			lastCaller := tool.Caller()
			lastCaller.Line -= 1
			var err interface{}
			func() {
				defer func() { err = recover() }()
				MockValue(&callerValue).To("should panic, but recovered")
			}()
			errString, ok := err.(string)
			convey.So(ok, convey.ShouldBeTrue)
			convey.So(strings.Contains(errString, lastCaller.String()), convey.ShouldBeTrue)
		})
	})
}
