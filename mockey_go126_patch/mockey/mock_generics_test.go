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

func sum[T int | float64](l, r T) T {
	return l + r
}

type generic[T int | string] struct {
	a T
}

func (g generic[T]) Value() T {
	return g.a
}

func (g generic[T]) Value2(hint T) string {
	return fmt.Sprintf("%v %v", g.a, hint)
}

func (g *generic[T]) Value3(hint T) string {
	return fmt.Sprintf("%v %v", g.a, hint)
}

func (g generic[T]) func1(hint T) string {
	return fmt.Sprintf("%v %v", g.a, hint)
}

type genericMap[K comparable, V any] struct {
	m map[K]V
}

func (gm *genericMap[K, V]) Get(key K) V {
	return gm.m[key]
}

func TestGeneric(t *testing.T) {
	PatchConvey("generic", t, func() {
		PatchConvey("func", func() {
			arg1, arg2 := 1, 2
			mockGeneric(sum[int]).To(func(a, b int) int {
				convey.So(a, convey.ShouldEqual, arg1)
				convey.So(b, convey.ShouldEqual, arg2)
				return 999
			}).Build()
			convey.So(sum[int](1, 2), convey.ShouldEqual, 999)

			mockGeneric(sum[float64]).Return(888).Build()
			convey.So(sum[float64](1, 2), convey.ShouldEqual, 888)
		})
		PatchConvey("method", func() {
			mockGeneric(generic[int].Value).Return(999).Build()
			gi := generic[int]{a: 123}
			convey.So(gi.Value(), convey.ShouldEqual, 999)

			arg1 := "hint"
			mockGeneric(GetMethod(generic[string]{}, "Value2")).To(func(hint string) string {
				convey.So(hint, convey.ShouldEqual, arg1)
				return "mock"
			}).Build()
			gs := generic[string]{a: "abc"}
			convey.So(gi.Value(), convey.ShouldEqual, 999)
			convey.So(gs.Value2(arg1), convey.ShouldEqual, "mock")
		})
		PatchConvey("method, hook has receiver", func() {
			PatchConvey("access the receiver field", func() {
				var innerA int
				mockGeneric(generic[int].Value2).To(func(i generic[int], hint int) string {
					innerA = i.a
					return "mock"
				}).Build()
				gi := generic[int]{a: 123}
				res := gi.Value2(10000)
				convey.So(res, convey.ShouldEqual, "mock")
				convey.So(innerA, convey.ShouldEqual, 123)
			})
			PatchConvey("modify the receiver field", func() {
				mockGeneric((*generic[int]).Value3).To(func(i *generic[int], hint int) string {
					i.a = hint
					return "mock"
				}).Build()
				gi := generic[int]{a: 123}
				res := gi.Value3(10000)
				convey.So(res, convey.ShouldEqual, "mock")
				convey.So(gi.a, convey.ShouldEqual, 10000)
			})
		})
		PatchConvey("func1 method misjudge", func() {
			// FIXME: `func1` is misjudged as a function, `OptMethod` is used to explicitly specify it as method.
			PatchConvey("without receiver", func() {
				mockGeneric(generic[string].func1, misjudgeOpt...).To(func(hint string) string {
					convey.So(hint, convey.ShouldEqual, "hint")
					return "mock"
				}).Build()
				convey.So(generic[string]{a: "abc"}.func1("hint"), convey.ShouldEqual, "mock")
			})
			PatchConvey("with receiver", func() {
				mockGeneric(generic[string].func1, misjudgeOpt...).To(func(g generic[string], hint string) string {
					convey.So(g.a, convey.ShouldEqual, "abc")
					convey.So(hint, convey.ShouldEqual, "hint")
					return "mock"
				}).Build()
				convey.So(generic[string]{a: "abc"}.func1("hint"), convey.ShouldEqual, "mock")
			})
		})
		PatchConvey("origin", func() {
			PatchConvey("func", func() {
				var origin = sum[float64]
				decorator := func(a, b float64) float64 {
					return origin(a, b) + 1
				}
				mockGeneric(sum[float64]).To(decorator).Origin(&origin).Build()
				convey.So(sum[float64](1, 2), convey.ShouldEqual, 4)
			})

			PatchConvey("method", func() {
				var origin = generic[string].Value2
				decorator := func(a generic[string], hint string) string {
					return "decorated " + origin(a, hint)
				}
				mockGeneric(generic[string].Value2).To(decorator).Origin(&origin).Build()
				convey.So(generic[string]{a: "abc"}.Value2("123"), convey.ShouldEqual, "decorated abc 123")
			})

			PatchConvey("method without receiver", func() {
				var origin func(string) string
				decorator := func(hint string) string {
					return "decorated " + origin(hint)
				}
				mockGeneric(generic[string].Value2).To(decorator).Origin(&origin).Build()
				convey.So(generic[string]{a: "abc"}.Value2("123"), convey.ShouldEqual, "decorated abc 123")
			})
		})
		PatchConvey("when", func() {
			mockGeneric(sum[float64]).To(func(a, b float64) float64 {
				return 0
			}).When(func(a, b float64) bool {
				return a+b == 3
			}).Build()
			convey.So(sum[float64](1, 2), convey.ShouldEqual, 0)
			convey.So(sum[float64](1, 1), convey.ShouldEqual, 2)
		})
		PatchConvey("same gcshape", func() {
			a, b := 12345, "12345"
			PatchConvey("normal", func() {
				mockGeneric((*genericMap[int32, *int]).Get).To(func(m *genericMap[int32, *int], key int32) *int { return &a }).Build()
				mockGeneric((*genericMap[int32, *string]).Get).Return(&b).Build()
				res1 := new(genericMap[int32, *int]).Get(1)
				convey.So(res1, convey.ShouldEqual, &a)
				convey.So(*res1, convey.ShouldEqual, 12345)
				res2 := new(genericMap[int32, *string]).Get(2)
				convey.So(res2, convey.ShouldEqual, &b)
				convey.So(*res2, convey.ShouldEqual, "12345")
			})
			PatchConvey("re-mock", func() {
				convey.So(func() {
					mockGeneric((*genericMap[int32, *int]).Get).Return(&a).Build()
					mockGeneric((*genericMap[int32, *int]).Get).Return(&a).Build()
				}, remockResult)
			})
		})
		PatchConvey("not match", func() {
			mockGeneric((*generic[string]).Value2).Return("MOCKED!").Build()
			res1 := generic[string]{a: "abc"}.Value2("123")
			convey.So(res1, convey.ShouldEqual, notMatchResult)
		})
	})
}

type Large15[T any] struct {
	_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _ T
}

func TestGenericArg(t *testing.T) {
	PatchConvey("args", t, func() {
		GenericArgRunner[uint8]("uint8")
		GenericArgRunner[uint16]("uint16")
		GenericArgRunner[uint32]("uint32")
		GenericArgRunner[uint64]("uint64")
		GenericArgRunner[Large15[int]]("large15[int]")
		GenericArgRunner[Large15[Large15[int]]]("Large15[Large15[int]")
		GenericArgRunner[Large15[Large15[[100]byte]]]("Large15[Large15[[100]byte]")
	})
}

func TestGenericRet(t *testing.T) {
	PatchConvey("rets", t, func() {
		GenericRetRunner[uint8]("uint8")
		GenericRetRunner[uint16]("uint16")
		GenericRetRunner[uint32]("uint32")
		GenericRetRunner[uint64]("uint64")
		GenericRetRunner[Large15[int]]("large15[int]")
		GenericRetRunner[Large15[Large15[int]]]("Large15[Large15[int]")
		GenericRetRunner[Large15[Large15[[100]byte]]]("Large15[Large15[[100]byte]")
	})
}

func TestGenericArgRet(t *testing.T) {
	PatchConvey("args-rets", t, func() {
		GenericArgRetRunner[uint8]("uint8")
		GenericArgRetRunner[uint16]("uint16")
		GenericArgRetRunner[uint32]("uint32")
		GenericArgRetRunner[uint64]("uint64")
		GenericArgRetRunner[Large15[int]]("large15[int]")
		GenericArgRetRunner[Large15[Large15[int]]]("Large15[Large15[int]")
		GenericArgRetRunner[Large15[Large15[[100]byte]]]("Large15[Large15[[100]byte]")
	})
}

func TestGenericArgValues(t *testing.T) {
	PatchConvey("args-value", t, func() {
		PatchConvey("single", func() {
			var arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15 uintptr = 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
			mockGeneric(GenericsArg15[uintptr]).To(func(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15 uintptr) {
				convey.So(_1, convey.ShouldEqual, arg1)
				convey.So(_2, convey.ShouldEqual, arg2)
				convey.So(_3, convey.ShouldEqual, arg3)
				convey.So(_4, convey.ShouldEqual, arg4)
				convey.So(_5, convey.ShouldEqual, arg5)
				convey.So(_6, convey.ShouldEqual, arg6)
				convey.So(_7, convey.ShouldEqual, arg7)
				convey.So(_8, convey.ShouldEqual, arg8)
				convey.So(_9, convey.ShouldEqual, arg9)
				convey.So(_10, convey.ShouldEqual, arg10)
				convey.So(_11, convey.ShouldEqual, arg11)
				convey.So(_12, convey.ShouldEqual, arg12)
				convey.So(_13, convey.ShouldEqual, arg13)
				convey.So(_14, convey.ShouldEqual, arg14)
				convey.So(_15, convey.ShouldEqual, arg15)
			}).Build()
			GenericsArg15(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15)
		})
		PatchConvey("complex", func() {
			var arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15 string = "1", "2", "3", "4", "5", " 6", "7", "8", "9", "10", "11", "12", "13", "14", "15"
			mockGeneric(GenericsArg15[string]).To(func(_1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15 string) {
				convey.So(_1, convey.ShouldEqual, arg1)
				convey.So(_2, convey.ShouldEqual, arg2)
				convey.So(_3, convey.ShouldEqual, arg3)
				convey.So(_4, convey.ShouldEqual, arg4)
				convey.So(_5, convey.ShouldEqual, arg5)
				convey.So(_6, convey.ShouldEqual, arg6)
				convey.So(_7, convey.ShouldEqual, arg7)
				convey.So(_8, convey.ShouldEqual, arg8)
				convey.So(_9, convey.ShouldEqual, arg9)
				convey.So(_10, convey.ShouldEqual, arg10)
				convey.So(_11, convey.ShouldEqual, arg11)
				convey.So(_12, convey.ShouldEqual, arg12)
				convey.So(_13, convey.ShouldEqual, arg13)
				convey.So(_14, convey.ShouldEqual, arg14)
				convey.So(_15, convey.ShouldEqual, arg15)
			}).Build()
			GenericsArg15(arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14, arg15)
		})
		PatchConvey("args-type", func() {
			target := GenericsTemplate[int, float32, string, chan int, []byte, struct{ _ int }]
			mockGeneric(target).To(
				func(info GenericInfo, t1 int, t2 float32, t3 string) (r1 chan int, r2 []byte, r3 struct{ _ int }) {
					convey.So(info.UsedParamType(0), convey.ShouldEqual, reflect.TypeOf(t1))
					convey.So(info.UsedParamType(1), convey.ShouldEqual, reflect.TypeOf(t2))
					convey.So(info.UsedParamType(2), convey.ShouldEqual, reflect.TypeOf(t3))
					convey.So(info.UsedParamType(3), convey.ShouldEqual, reflect.TypeOf(r1))
					convey.So(info.UsedParamType(4), convey.ShouldEqual, reflect.TypeOf(r2))
					convey.So(info.UsedParamType(5), convey.ShouldEqual, reflect.TypeOf(r3))
					return
				}).Build()
			target(1, 2, "3")
		})
	})
}

func GenericsTemplate[T1, T2, T3, R1, R2, R3 any](t1 T1, t2 T2, t3 T3) (r1 R1, r2 R2, r3 R3) {
	fmt.Println(t1, t2, t3, r1, r2, r3)
	panic("not here")
}

func GenericsArg0[T any]()                                            { panic("0") }
func GenericsArg1[T any](_ T)                                         { panic("1") }
func GenericsArg2[T any](_, _ T)                                      { panic("2") }
func GenericsArg3[T any](_, _, _ T)                                   { panic("3") }
func GenericsArg4[T any](_, _, _, _ T)                                { panic("4") }
func GenericsArg5[T any](_, _, _, _, _ T)                             { panic("5") }
func GenericsArg6[T any](_, _, _, _, _, _ T)                          { panic("6") }
func GenericsArg7[T any](_, _, _, _, _, _, _ T)                       { panic("7") }
func GenericsArg8[T any](_, _, _, _, _, _, _, _ T)                    { panic("8") }
func GenericsArg9[T any](_, _, _, _, _, _, _, _, _ T)                 { panic("9") }
func GenericsArg10[T any](_, _, _, _, _, _, _, _, _, _ T)             { panic("10") }
func GenericsArg11[T any](_, _, _, _, _, _, _, _, _, _, _ T)          { panic("11") }
func GenericsArg12[T any](_, _, _, _, _, _, _, _, _, _, _, _ T)       { panic("12") }
func GenericsArg13[T any](_, _, _, _, _, _, _, _, _, _, _, _, _ T)    { panic("13") }
func GenericsArg14[T any](_, _, _, _, _, _, _, _, _, _, _, _, _, _ T) { panic("14") }
func GenericsArg15[T any](_, _, _, _, _, _, _, _, _, _, _, _, _, _, _ T) {
	panic("15")
}

func GenericArgRunner[T any](name string) {
	// type T = context.Context
	var arg T

	PatchConvey(name, func() {
		mockGeneric(GenericsArg0[T]).Return().Build()
		mockGeneric(GenericsArg1[T]).Return().Build()
		mockGeneric(GenericsArg2[T]).Return().Build()
		mockGeneric(GenericsArg3[T]).Return().Build()
		mockGeneric(GenericsArg4[T]).Return().Build()
		mockGeneric(GenericsArg5[T]).Return().Build()
		mockGeneric(GenericsArg6[T]).Return().Build()
		mockGeneric(GenericsArg7[T]).Return().Build()
		mockGeneric(GenericsArg8[T]).Return().Build()
		mockGeneric(GenericsArg9[T]).Return().Build()
		mockGeneric(GenericsArg10[T]).Return().Build()
		mockGeneric(GenericsArg11[T]).Return().Build()
		mockGeneric(GenericsArg12[T]).Return().Build()
		mockGeneric(GenericsArg13[T]).Return().Build()
		mockGeneric(GenericsArg14[T]).Return().Build()
		mockGeneric(GenericsArg15[T]).Return().Build()
		convey.So(func() { GenericsArg0[T]() }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArg1(arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArg2(arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArg3(arg, arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArg4(arg, arg, arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArg5(arg, arg, arg, arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArg6(arg, arg, arg, arg, arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArg7(arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArg8(arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArg9(arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArg10(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArg11(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArg12(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArg13(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArg14(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArg15(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldNotPanic)
	})

	convey.So(func() { GenericsArg0[T]() }, convey.ShouldPanic)
	convey.So(func() { GenericsArg1(arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArg2(arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArg3(arg, arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArg4(arg, arg, arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArg5(arg, arg, arg, arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArg6(arg, arg, arg, arg, arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArg7(arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArg8(arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArg9(arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArg10(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArg11(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArg12(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArg13(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArg14(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArg15(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldPanic)
}

func GenericsRet0[T any]()                                            { panic("0") }
func GenericsRet1[T any]() (_ T)                                      { panic("1") }
func GenericsRet2[T any]() (_, _ T)                                   { panic("2") }
func GenericsRet3[T any]() (_, _, _ T)                                { panic("3") }
func GenericsRet4[T any]() (_, _, _, _ T)                             { panic("4") }
func GenericsRet5[T any]() (_, _, _, _, _ T)                          { panic("5") }
func GenericsRet6[T any]() (_, _, _, _, _, _ T)                       { panic("6") }
func GenericsRet7[T any]() (_, _, _, _, _, _, _ T)                    { panic("7") }
func GenericsRet8[T any]() (_, _, _, _, _, _, _, _ T)                 { panic("8") }
func GenericsRet9[T any]() (_, _, _, _, _, _, _, _, _ T)              { panic("9") }
func GenericsRet10[T any]() (_, _, _, _, _, _, _, _, _, _ T)          { panic("10") }
func GenericsRet11[T any]() (_, _, _, _, _, _, _, _, _, _, _ T)       { panic("11") }
func GenericsRet12[T any]() (_, _, _, _, _, _, _, _, _, _, _, _ T)    { panic("12") }
func GenericsRet13[T any]() (_, _, _, _, _, _, _, _, _, _, _, _, _ T) { panic("13") }
func GenericsRet14[T any]() (_, _, _, _, _, _, _, _, _, _, _, _, _, _ T) {
	panic("14")
}

func GenericsRet15[T any]() (_, _, _, _, _, _, _, _, _, _, _, _, _, _, _ T) {
	panic("15")
}

func GenericRetRunner[T any](name string) {
	var arg T
	PatchConvey(name, func() {
		mockGeneric(GenericsRet0[T]).Return().Build()
		mockGeneric(GenericsRet1[T]).Return(arg).Build()
		mockGeneric(GenericsRet2[T]).Return(arg, arg).Build()
		mockGeneric(GenericsRet3[T]).Return(arg, arg, arg).Build()
		mockGeneric(GenericsRet4[T]).Return(arg, arg, arg, arg).Build()
		mockGeneric(GenericsRet5[T]).Return(arg, arg, arg, arg, arg).Build()
		mockGeneric(GenericsRet6[T]).Return(arg, arg, arg, arg, arg, arg).Build()
		mockGeneric(GenericsRet7[T]).Return(arg, arg, arg, arg, arg, arg, arg).Build()
		mockGeneric(GenericsRet8[T]).Return(arg, arg, arg, arg, arg, arg, arg, arg).Build()
		mockGeneric(GenericsRet9[T]).Return(arg, arg, arg, arg, arg, arg, arg, arg, arg).Build()
		mockGeneric(GenericsRet10[T]).Return(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg).Build()
		mockGeneric(GenericsRet11[T]).Return(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg).Build()
		mockGeneric(GenericsRet12[T]).Return(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg).Build()
		mockGeneric(GenericsRet13[T]).Return(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg).Build()
		mockGeneric(GenericsRet14[T]).Return(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg).Build()
		mockGeneric(GenericsRet15[T]).Return(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg).Build()
		convey.So(func() { GenericsRet0[T]() }, convey.ShouldNotPanic)
		convey.So(func() { GenericsRet1[T]() }, convey.ShouldNotPanic)
		convey.So(func() { GenericsRet2[T]() }, convey.ShouldNotPanic)
		convey.So(func() { GenericsRet3[T]() }, convey.ShouldNotPanic)
		convey.So(func() { GenericsRet4[T]() }, convey.ShouldNotPanic)
		convey.So(func() { GenericsRet5[T]() }, convey.ShouldNotPanic)
		convey.So(func() { GenericsRet6[T]() }, convey.ShouldNotPanic)
		convey.So(func() { GenericsRet7[T]() }, convey.ShouldNotPanic)
		convey.So(func() { GenericsRet8[T]() }, convey.ShouldNotPanic)
		convey.So(func() { GenericsRet9[T]() }, convey.ShouldNotPanic)
		convey.So(func() { GenericsRet10[T]() }, convey.ShouldNotPanic)
		convey.So(func() { GenericsRet11[T]() }, convey.ShouldNotPanic)
		convey.So(func() { GenericsRet12[T]() }, convey.ShouldNotPanic)
		convey.So(func() { GenericsRet13[T]() }, convey.ShouldNotPanic)
		convey.So(func() { GenericsRet14[T]() }, convey.ShouldNotPanic)
		convey.So(func() { GenericsRet15[T]() }, convey.ShouldNotPanic)
	})

	convey.So(func() { GenericsRet0[T]() }, convey.ShouldPanic)
	convey.So(func() { GenericsRet1[T]() }, convey.ShouldPanic)
	convey.So(func() { GenericsRet2[T]() }, convey.ShouldPanic)
	convey.So(func() { GenericsRet3[T]() }, convey.ShouldPanic)
	convey.So(func() { GenericsRet4[T]() }, convey.ShouldPanic)
	convey.So(func() { GenericsRet5[T]() }, convey.ShouldPanic)
	convey.So(func() { GenericsRet6[T]() }, convey.ShouldPanic)
	convey.So(func() { GenericsRet7[T]() }, convey.ShouldPanic)
	convey.So(func() { GenericsRet8[T]() }, convey.ShouldPanic)
	convey.So(func() { GenericsRet9[T]() }, convey.ShouldPanic)
	convey.So(func() { GenericsRet10[T]() }, convey.ShouldPanic)
	convey.So(func() { GenericsRet11[T]() }, convey.ShouldPanic)
	convey.So(func() { GenericsRet12[T]() }, convey.ShouldPanic)
	convey.So(func() { GenericsRet13[T]() }, convey.ShouldPanic)
	convey.So(func() { GenericsRet14[T]() }, convey.ShouldPanic)
	convey.So(func() { GenericsRet15[T]() }, convey.ShouldPanic)
}

func GenericsArgRet0[T any]()                                        { panic("0") }
func GenericsArgRet1[T any](_ T) (_ T)                               { panic("1") }
func GenericsArgRet2[T any](_, _ T) (_, _ T)                         { panic("2") }
func GenericsArgRet3[T any](_, _, _ T) (_, _, _ T)                   { panic("3") }
func GenericsArgRet4[T any](_, _, _, _ T) (_, _, _, _ T)             { panic("4") }
func GenericsArgRet5[T any](_, _, _, _, _ T) (_, _, _, _, _ T)       { panic("5") }
func GenericsArgRet6[T any](_, _, _, _, _, _ T) (_, _, _, _, _, _ T) { panic("6") }
func GenericsArgRet7[T any](_, _, _, _, _, _, _ T) (_, _, _, _, _, _, _ T) {
	panic("7")
}

func GenericsArgRet8[T any](_, _, _, _, _, _, _, _ T) (_, _, _, _, _, _, _, _ T) {
	panic("8")
}

func GenericsArgRet9[T any](_, _, _, _, _, _, _, _, _ T) (_, _, _, _, _, _, _, _, _ T) {
	panic("9")
}

func GenericsArgRet10[T any](_, _, _, _, _, _, _, _, _, _ T) (_, _, _, _, _, _, _, _, _, _ T) {
	panic("10")
}

func GenericsArgRet11[T any](_, _, _, _, _, _, _, _, _, _, _ T) (_, _, _, _, _, _, _, _, _, _, _ T) {
	panic("11")
}

func GenericsArgRet12[T any](_, _, _, _, _, _, _, _, _, _, _, _ T) (_, _, _, _, _, _, _, _, _, _, _, _ T) {
	panic("12")
}

func GenericsArgRet13[T any](_, _, _, _, _, _, _, _, _, _, _, _, _ T) (_, _, _, _, _, _, _, _, _, _, _, _, _ T) {
	panic("13")
}

func GenericsArgRet14[T any](_, _, _, _, _, _, _, _, _, _, _, _, _, _ T) (_, _, _, _, _, _, _, _, _, _, _, _, _, _ T) {
	panic("14")
}

func GenericsArgRet15[T any](_, _, _, _, _, _, _, _, _, _, _, _, _, _, _ T) (_, _, _, _, _, _, _, _, _, _, _, _, _, _, _ T) {
	panic("15")
}

func GenericArgRetRunner[T any](name string) {
	var arg T
	PatchConvey(name, func() {
		mockGeneric(GenericsArgRet0[T]).Return().Build()
		mockGeneric(GenericsArgRet1[T]).Return(arg).Build()
		mockGeneric(GenericsArgRet2[T]).Return(arg, arg).Build()
		mockGeneric(GenericsArgRet3[T]).Return(arg, arg, arg).Build()
		mockGeneric(GenericsArgRet4[T]).Return(arg, arg, arg, arg).Build()
		mockGeneric(GenericsArgRet5[T]).Return(arg, arg, arg, arg, arg).Build()
		mockGeneric(GenericsArgRet6[T]).Return(arg, arg, arg, arg, arg, arg).Build()
		mockGeneric(GenericsArgRet7[T]).Return(arg, arg, arg, arg, arg, arg, arg).Build()
		mockGeneric(GenericsArgRet8[T]).Return(arg, arg, arg, arg, arg, arg, arg, arg).Build()
		mockGeneric(GenericsArgRet9[T]).Return(arg, arg, arg, arg, arg, arg, arg, arg, arg).Build()
		mockGeneric(GenericsArgRet10[T]).Return(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg).Build()
		mockGeneric(GenericsArgRet11[T]).Return(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg).Build()
		mockGeneric(GenericsArgRet12[T]).Return(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg).Build()
		mockGeneric(GenericsArgRet13[T]).Return(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg).Build()
		mockGeneric(GenericsArgRet14[T]).Return(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg).Build()
		mockGeneric(GenericsArgRet15[T]).Return(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg).Build()
		convey.So(func() { GenericsArgRet0[T]() }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArgRet1(arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArgRet2(arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArgRet3(arg, arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArgRet4(arg, arg, arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArgRet5(arg, arg, arg, arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArgRet6(arg, arg, arg, arg, arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArgRet7(arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArgRet8(arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArgRet9(arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArgRet10(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArgRet11(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArgRet12(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArgRet13(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArgRet14(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldNotPanic)
		convey.So(func() { GenericsArgRet15(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldNotPanic)
	})

	convey.So(func() { GenericsArgRet0[T]() }, convey.ShouldPanic)
	convey.So(func() { GenericsArgRet1(arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArgRet2(arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArgRet3(arg, arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArgRet4(arg, arg, arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArgRet5(arg, arg, arg, arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArgRet6(arg, arg, arg, arg, arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArgRet7(arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArgRet8(arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArgRet9(arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArgRet10(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArgRet11(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArgRet12(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArgRet13(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArgRet14(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldPanic)
	convey.So(func() { GenericsArg15(arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg, arg) }, convey.ShouldPanic)
}
