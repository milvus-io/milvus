//go:build go1.18
// +build go1.18

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
	"reflect"
	"testing"

	"github.com/smartystreets/goconvey/convey"
)

type testAG[T any] struct {
	t T
	testCG[T]
}

func (a testAG[T]) FooA() {}

func (a *testAG[T]) BarA() {}

func (a testAG[T]) FooC() {
	panic("shouldn't here")
}

func (a *testAG[T]) BarC() {
	panic("shouldn't here")
}

type testBG[T any] struct {
	t T
	*testCG[T]
}

func (b testBG[T]) FooB() {}

func (b *testBG[T]) BarB() {}

type testCG[T any] struct{ t T }

func (s testCG[T]) FooC() {
	fmt.Print("")
}

func (s *testCG[T]) BarC() {
	fmt.Print("")
}

type testDG[T any] struct {
	t T
	*testBG[T]
}

func (s testDG[T]) FooD() {}

func (s *testDG[T]) BarD() {}

func TestGetMethod_Generic(t *testing.T) {
	convey.Convey("TestGetMethod", t, func() {
		convey.Convey("basic cases", func() {
			convey.Convey("case testAG", func() {
				instance := testAG[int]{}
				convey.So(func() { GetMethod(instance, "FooB") }, convey.ShouldPanicWith, "can't reflect instance method: FooB")
				convey.So(func() { GetMethod(instance, "FooA") }, convey.ShouldNotPanic)
				convey.So(func() { GetMethod(instance, "BarA") }, convey.ShouldNotPanic)
				convey.So(func() { GetMethod(instance, "FooC") }, convey.ShouldNotPanic)
				convey.So(func() { GetMethod(instance, "BarC") }, convey.ShouldNotPanic)
				convey.So(func() { instance.FooC() }, convey.ShouldPanicWith, "shouldn't here")
				convey.So(func() {
					reflect.ValueOf(GetMethod(instance, "FooC")).Call([]reflect.Value{reflect.ValueOf(instance.testCG)})
				}, convey.ShouldNotPanic)
				convey.So(func() { instance.BarC() }, convey.ShouldPanicWith, "shouldn't here")
				convey.So(func() {
					reflect.ValueOf(GetMethod(instance, "BarC")).Call([]reflect.Value{reflect.ValueOf(&instance.testCG)})
				}, convey.ShouldNotPanic)
			})

			convey.Convey("case testBG", func() {
				instance := testBG[string]{}
				convey.So(func() { GetMethod(instance, "FooA") }, convey.ShouldPanicWith, "can't reflect instance method: FooA")
				convey.So(func() { GetMethod(instance, "FooB") }, convey.ShouldNotPanic)
				convey.So(func() { GetMethod(instance, "BarB") }, convey.ShouldNotPanic)
				convey.So(func() { GetMethod(instance, "FooC") }, convey.ShouldNotPanic)
				convey.So(func() { GetMethod(instance, "BarC") }, convey.ShouldNotPanic)
			})

			convey.Convey("case testD", func() {
				instance := testDG[string]{}
				convey.So(func() { GetMethod(instance, "FooA") }, convey.ShouldPanicWith, "can't reflect instance method: FooA")
				convey.So(func() { GetMethod(instance, "FooD") }, convey.ShouldNotPanic)
				convey.So(func() { GetMethod(instance, "BarD") }, convey.ShouldNotPanic)
				convey.So(func() { GetMethod(instance, "FooB") }, convey.ShouldNotPanic)
				convey.So(func() { GetMethod(instance, "BarB") }, convey.ShouldNotPanic)
				convey.So(func() { GetMethod(instance, "FooC") }, convey.ShouldNotPanic)
				convey.So(func() { GetMethod(instance, "BarC") }, convey.ShouldNotPanic)
			})
		})

		PatchConvey("patch cases", func() {
			PatchConvey("case testAG", func() {
				instance := testAG[int]{}
				MockGeneric(GetMethod(instance, "FooC")).To(func() { panic("should here") }).Build()
				MockGeneric(GetMethod(instance, "BarC")).To(func() { panic("should here") }).Build()
				convey.So(func() { instance.FooC() }, convey.ShouldPanicWith, "shouldn't here") // no effect, didn't call testCG.FooC()
				convey.So(func() { instance.BarC() }, convey.ShouldPanicWith, "shouldn't here") // no effect, didn't call testCG.BarC()
				convey.So(func() { instance.testCG.FooC() }, convey.ShouldPanicWith, "should here")
				convey.So(func() { instance.testCG.BarC() }, convey.ShouldPanicWith, "should here")
			})

			PatchConvey("case testBG", func() {
				instance := testBG[string]{testCG: &testCG[string]{}}
				MockGeneric(GetMethod(instance, "FooC")).To(func() { panic("should here") }).Build()
				MockGeneric(GetMethod(instance, "BarC")).To(func() { panic("should here") }).Build()
				convey.So(func() { instance.FooC() }, convey.ShouldPanicWith, "should here")
				convey.So(func() { instance.BarC() }, convey.ShouldPanicWith, "should here")
			})

			PatchConvey("case testD", func() {
				instance := testDG[string]{testBG: &testBG[string]{testCG: &testCG[string]{}}}
				MockGeneric(GetMethod(instance, "FooC")).To(func() { panic("should here") }).Build()
				MockGeneric(GetMethod(instance, "BarC")).To(func() { panic("should here") }).Build()
				convey.So(func() { instance.FooC() }, convey.ShouldPanicWith, "should here")
				convey.So(func() { instance.BarC() }, convey.ShouldPanicWith, "should here")
			})
		})
	})
}
