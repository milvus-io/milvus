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

	"github.com/smartystreets/goconvey/convey"
)

type multiConditionStruct struct {
	Field string
}

func (m *multiConditionStruct) Foo(in int) string {
	return m.Field
}

func TestMultiCondition(t *testing.T) {
	PatchConvey("multi condition", t, func() {
		PatchConvey("function", func() {
			fn := func(i int) string {
				fmt.Println() // to avoid an old unresolved bug,see https://github.com/bytedance/mockey/issues/24
				return "fn:not here"
			}

			builder := Mock(fn)
			builder.When(func(i int) bool { return i > 0 && i < 3 }).To(func(i int) string {
				if i == 1 {
					return "to:1"
				}
				if i == 2 {
					return "to:2"
				}
				panic("to:not here")
			})
			builder.Return("return:3").When(func(i int) bool { return i == 3 })

			PatchConvey("normal", func() {
				builder.Build()
				convey.So(fn(0), convey.ShouldEqual, "fn:not here")
				convey.So(fn(1), convey.ShouldEqual, "to:1")
				convey.So(fn(2), convey.ShouldEqual, "to:2")
				convey.So(fn(3), convey.ShouldEqual, "return:3")
				convey.So(fn(4), convey.ShouldEqual, "fn:not here")
			})

			PatchConvey("missing when in last case", func() {
				builder.To(func(i int) string { return "to:default" }).Build()
				convey.So(fn(0), convey.ShouldEqual, "to:default")
				convey.So(fn(1), convey.ShouldEqual, "to:1")
				convey.So(fn(2), convey.ShouldEqual, "to:2")
				convey.So(fn(3), convey.ShouldEqual, "return:3")
				convey.So(fn(4), convey.ShouldEqual, "to:default")
			})

			PatchConvey("missing to/return in last case", func() {
				builder.When(func(i int) bool { return i == 4 }).Build()
				convey.So(fn(0), convey.ShouldEqual, "fn:not here")
				convey.So(fn(1), convey.ShouldEqual, "to:1")
				convey.So(fn(2), convey.ShouldEqual, "to:2")
				convey.So(fn(3), convey.ShouldEqual, "return:3")
				convey.So(fn(4), convey.ShouldEqual, "fn:not here")
			})

			PatchConvey("re-mock when not allowed in multi-condition", func() {
				mocker := builder.Build()
				convey.So(func() { mocker.When(func(i int) bool { return true }) }, convey.ShouldPanic)
			})

			PatchConvey("re-mock to not allowed in multi-condition", func() {
				mocker := builder.Build()
				convey.So(func() { mocker.To(func(i int) string { return "to" }) }, convey.ShouldPanic)
			})

			PatchConvey("continuous when/to/return not allowed in multi-condition", func() {
				anyWhen := func(i int) bool { return true }
				anyTo := func(i int) string { return "" }
				anyReturn := ""
				PatchConvey("when-when", func() {
					convey.So(func() { builder.When(anyWhen).When(anyWhen) }, convey.ShouldPanic)
				})
				PatchConvey("to", func() {
					builder.To(anyTo)
					PatchConvey("to-to", func() {
						convey.So(func() { builder.To(anyTo) }, convey.ShouldPanic)
					})
					PatchConvey("to-return", func() {
						convey.So(func() { builder.Return(anyReturn) }, convey.ShouldPanic)
					})
				})
				PatchConvey("return", func() {
					builder.Return(anyReturn)
					PatchConvey("return-to", func() {
						convey.So(func() { builder.To(anyTo) }, convey.ShouldPanic)
					})
					PatchConvey("return-return", func() {
						convey.So(func() { builder.Return(anyReturn) }, convey.ShouldPanic)
					})
				})
			})
		})

		PatchConvey("struct", func() {
			PatchConvey("origin with receiver", func() {
				builder := Mock(GetMethod(&multiConditionStruct{}, "Foo"))
				var origin func(self *multiConditionStruct, i int) string
				builder.Origin(&origin)
				builder.When(func(in int) bool { return in > 0 && in < 3 }).To(func(self *multiConditionStruct, i int) string {
					if i == 1 {
						return "to:1"
					}
					if i == 2 {
						return "to:origin:" + origin(self, i)
					}
					panic("to:not here")
				})
				builder.Return("return:3").When(func(i int) bool { return i == 3 })
				builder.Build()
				obj := &multiConditionStruct{Field: "fn:not here"}
				convey.So(obj.Foo(0), convey.ShouldEqual, "fn:not here")
				convey.So(obj.Foo(1), convey.ShouldEqual, "to:1")
				convey.So(obj.Foo(2), convey.ShouldEqual, "to:origin:fn:not here")
				convey.So(obj.Foo(3), convey.ShouldEqual, "return:3")
				convey.So(obj.Foo(4), convey.ShouldEqual, "fn:not here")
			})

			builder := Mock(GetMethod(&multiConditionStruct{}, "Foo"))
			var origin func(i int) string
			builder.Origin(&origin)
			builder.When(func(in int) bool { return in > 0 && in < 3 }).To(func(i int) string {
				if i == 1 {
					return "to:1"
				}
				if i == 2 {
					return "to:origin:" + origin(i)
				}
				panic("to:not here")
			})
			builder.Return("return:3").When(func(i int) bool { return i == 3 })

			PatchConvey("normal", func() {
				builder.Build()
				obj := &multiConditionStruct{Field: "fn:not here"}
				convey.So(obj.Foo(0), convey.ShouldEqual, "fn:not here")
				convey.So(obj.Foo(1), convey.ShouldEqual, "to:1")
				convey.So(obj.Foo(2), convey.ShouldEqual, "to:origin:fn:not here")
				convey.So(obj.Foo(3), convey.ShouldEqual, "return:3")
				convey.So(obj.Foo(4), convey.ShouldEqual, "fn:not here")
			})
			PatchConvey("missing when in last case", func() {
				builder.To(func(i int) string { return "to:default" })
				builder.Build()
				obj := &multiConditionStruct{Field: "fn:not here"}
				convey.So(obj.Foo(0), convey.ShouldEqual, "to:default")
				convey.So(obj.Foo(1), convey.ShouldEqual, "to:1")
				convey.So(obj.Foo(2), convey.ShouldEqual, "to:origin:fn:not here")
				convey.So(obj.Foo(3), convey.ShouldEqual, "return:3")
				convey.So(obj.Foo(4), convey.ShouldEqual, "to:default")
			})
			PatchConvey("missing to/return in last case", func() {
				builder.When(func(i int) bool { return i == 4 })
				builder.Build()
				obj := &multiConditionStruct{Field: "fn:not here"}
				convey.So(obj.Foo(0), convey.ShouldEqual, "fn:not here")
				convey.So(obj.Foo(1), convey.ShouldEqual, "to:1")
				convey.So(obj.Foo(2), convey.ShouldEqual, "to:origin:fn:not here")
				convey.So(obj.Foo(3), convey.ShouldEqual, "return:3")
				convey.So(obj.Foo(4), convey.ShouldEqual, "fn:not here")
			})

			PatchConvey("re-mock when not allowed in multi-condition", func() {
				mocker := builder.Build()
				convey.So(func() { mocker.When(func(i int) bool { return true }) }, convey.ShouldPanic)
			})

			PatchConvey("re-mock to not allowed in multi-condition", func() {
				mocker := builder.Build()
				convey.So(func() { mocker.To(func(i int) string { return "to" }) }, convey.ShouldPanic)
			})

			PatchConvey("re-mock return not allowed in multi-condition", func() {
				mocker := builder.Build()
				convey.So(func() { mocker.Return("to") }, convey.ShouldPanic)
			})

			PatchConvey("continuous when/to/return not allowed in multi-condition", func() {
				anyWhen := func(i int) bool { return true }
				anyTo := func(i int) string { return "" }
				anyReturn := ""
				PatchConvey("when-when", func() {
					convey.So(func() { builder.When(anyWhen).When(anyWhen) }, convey.ShouldPanic)
				})
				PatchConvey("to", func() {
					builder.To(anyTo)
					PatchConvey("to-to", func() {
						convey.So(func() { builder.To(anyTo) }, convey.ShouldPanic)
					})
					PatchConvey("to-return", func() {
						convey.So(func() { builder.Return(anyReturn) }, convey.ShouldPanic)
					})
				})
				PatchConvey("return", func() {
					builder.Return(anyReturn)
					PatchConvey("return-to", func() {
						convey.So(func() { builder.To(anyTo) }, convey.ShouldPanic)
					})
					PatchConvey("return-return", func() {
						convey.So(func() { builder.Return(anyReturn) }, convey.ShouldPanic)
					})
				})
			})
		})
	})
}
