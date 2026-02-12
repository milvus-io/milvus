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
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/bytedance/mockey/internal/tool"
	. "github.com/smartystreets/goconvey/convey"
)

func Fun(a string) string {
	fmt.Println(a)
	return a
}

type Class struct{}

func (*Class) FunA(a string) string {
	fmt.Println(a)
	return a
}

func (*Class) VariantParam(a string, b ...string) string {
	fmt.Println("VariantParam")
	return a
}

type class struct{ in string }

func (c class) func1(hint string) string {
	return fmt.Sprintf("%v %v", c.in, hint)
}

func MultiReturn() (int, int) {
	return 0, 0 * int(math.Abs(1))
}

func MultiReturnErr() (int, int, error) {
	return 0, 0, errors.New("old")
}

func VariantParam(a int, b ...int) (int, int) {
	return a, b[0]
}

func ShortFun() {}

func TestNoConvey(t *testing.T) {
	origin := Fun
	mock := func(p string) string {
		fmt.Println("b")
		origin(p)
		return "b"
	}
	mock2 := Mock(Fun).When(func(p string) bool { return p == "a" }).To(mock).Origin(&origin).Build()
	defer mock2.UnPatch()
	r := Fun("a")
	if r != "b" {
		t.Errorf("result = %s, expected = b", r)
	}
}

func TestMock(t *testing.T) {
	PatchConvey("test mock", t, func() {
		PatchConvey("test to", func() {
			origin := Fun
			mock := func(p string) string {
				fmt.Println("b")
				origin(p)
				return "b"
			}
			mock2 := Mock(Fun).When(func(p string) bool { return p == "a" }).To(mock).Origin(&origin).Build()
			r := Fun("a")
			So(r, ShouldEqual, "b")
			So(mock2.Times(), ShouldEqual, 1)
		})
		r := Fun("a")
		So(r, ShouldEqual, "a")
		PatchConvey("test return", func() {
			mock3 := Mock(Fun).When(func(p string) bool { return p == "a" }).Return("c").Build()
			r := Fun("a")
			So(r, ShouldEqual, "c")
			So(mock3.Times(), ShouldEqual, 1)
		})

		PatchConvey("test multi_return", func() {
			mock3 := Mock(MultiReturn).Return(1, 1).Build()
			a, b := MultiReturn()
			So(a, ShouldEqual, 1)
			So(b, ShouldEqual, 1)
			So(mock3.Times(), ShouldEqual, 1)
		})

		PatchConvey("test multi_return_err", func() {
			newErr := errors.New("new")
			mock3 := Mock(MultiReturnErr).Return(1, 1, newErr).Build()
			a, b, e := MultiReturnErr()
			So(a, ShouldEqual, 1)
			So(b, ShouldEqual, 1)
			So(e, ShouldBeError, newErr)
			So(mock3.Times(), ShouldEqual, 1)
		})

		PatchConvey("test variant param", func() {
			when := func(a int, bs ...int) bool {
				return bs[0] == 1
			}
			to := func(a int, bs ...int) (int, int) {
				return a + 1, bs[1]
			}
			mock4 := Mock(VariantParam).When(when).To(to).Build()
			a, b := VariantParam(0, 1, 2, 3)
			So(a, ShouldEqual, 1)
			So(b, ShouldEqual, 2)
			So(mock4.Times(), ShouldEqual, 1)
			So(mock4.MockTimes(), ShouldEqual, 1)
		})
	})
}

func TestParam(t *testing.T) {
	fmt.Printf("gid: %+v\n", tool.GetGoroutineID())
	PatchConvey("test variant param", t, func() {
		when := func(a int, bs ...int) bool {
			return bs[0] == 1
		}
		to := func(a int, bs ...int) (int, int) {
			return a + 1, bs[1]
		}
		mock4 := Mock(VariantParam).When(when).To(to).Build()
		PatchConvey("test when", func() {
			a, b := VariantParam(0, 1, 2, 3)
			So(a, ShouldEqual, 1)
			So(b, ShouldEqual, 2)
			So(mock4.Times(), ShouldEqual, 1)
			So(mock4.MockTimes(), ShouldEqual, 1)
		})
		PatchConvey("test no when", func() {
			a1, b1 := VariantParam(0, 2, 2, 3)
			So(a1, ShouldEqual, 0)
			So(b1, ShouldEqual, 2)
			So(mock4.Times(), ShouldEqual, 1)
			So(mock4.MockTimes(), ShouldEqual, 0)
		})
	})
}

func TestClass(t *testing.T) {
	PatchConvey("test class", t, func() {
		PatchConvey("test mock", func() {
			mock := func(self *Class, p string) string {
				fmt.Print("b")
				return "b"
			}
			m := Mock((*Class).FunA).When(func(self *Class, p string) bool { return p == "a" }).To(mock).Build()
			c := Class{}
			str := c.FunA("a")
			So(m.MockTimes(), ShouldEqual, 1)
			So(str, ShouldEqual, "b")
		})
		PatchConvey("test class variant param mock", func() {
			mock := func(self *Class, a string, b ...string) string { return b[0] }
			m := Mock((*Class).VariantParam).When(func(self *Class, a string, b ...string) bool { return a == "a" }).To(mock).Build()
			c := Class{}
			str := c.VariantParam("a", "b")
			So(m.MockTimes(), ShouldEqual, 1)
			So(str, ShouldEqual, "b")
		})

		PatchConvey("test  missing receiver mock", func() {
			mock := func(p string) string {
				fmt.Print("b")
				return "b"
			}
			m := Mock((*Class).FunA).When(func(p string) bool { return p == "a" }).To(mock).Build()
			c := Class{}
			str := c.FunA("a")
			So(m.MockTimes(), ShouldEqual, 1)
			So(str, ShouldEqual, "b")
		})
		PatchConvey("test missing receiver and more args", func() {
			mock := func() string {
				fmt.Print("b")
				return "b"
			}

			So(func() { Mock((*Class).FunA).When(func(p string) bool { return p == "a" }).To(mock).Build() }, ShouldPanic)
		})
		PatchConvey("func1 method misjudge", func() {
			PatchConvey("with receiver", func() {
				Mock(class.func1).To(func(c class, hint string) string {
					So(c.in, ShouldEqual, "abc")
					So(hint, ShouldEqual, "xxx")
					return "mock"
				}).Build()
				So(class{in: "abc"}.func1("xxx"), ShouldEqual, "mock")
			})
			PatchConvey("without receiver", func() {
				Mock(class.func1).To(func(hint string) string {
					So(hint, ShouldEqual, "xxx")
					return "mock"
				}).Build()
				So(class{in: "abc"}.func1("xxx"), ShouldEqual, "mock")
			})

		})
	})
}

type TestImpl struct {
	a string
}

func (i *TestImpl) A() string {
	fmt.Println(i.a)
	return i.a
}

type TestI interface {
	A() string
}

func ReturnImpl() TestI {
	return &TestImpl{a: "a"}
}

func TestInterface(t *testing.T) {
	PatchConvey("TestInterface", t, func() {
		PatchConvey("test mock", func() {
			m := Mock(ReturnImpl).Return(&TestImpl{a: "b"}).Build()
			str := ReturnImpl().A()
			So(m.MockTimes(), ShouldEqual, 1)
			So(str, ShouldEqual, "b")
		})
	})
}

func TestFilterGoRoutine(t *testing.T) {
	PatchConvey("filter go routine", t, func() {
		mock := Mock(Fun).ExcludeCurrentGoRoutine().Return("b").Build()
		r := Fun("a")
		So(r, ShouldEqual, "a")
		So(mock.Times(), ShouldEqual, 1)
		So(mock.MockTimes(), ShouldEqual, 0)

		mock.IncludeCurrentGoRoutine()
		r = Fun("a")
		So(r, ShouldEqual, "b")
		So(mock.Times(), ShouldEqual, 1)
		So(mock.MockTimes(), ShouldEqual, 1)

		mock.IncludeCurrentGoRoutine()
		go Fun("a")
		time.Sleep(1 * time.Second)
		So(mock.Times(), ShouldEqual, 1)
		So(mock.MockTimes(), ShouldEqual, 0)
	})
}

func TestResetPatch(t *testing.T) {
	PatchConvey("test mock", t, func() {
		PatchConvey("test to", func() {
			origin := Fun
			mock := func(p string) string {
				fmt.Println("b")
				origin(p)
				return "b"
			}
			mock2 := Mock(Fun).When(func(p string) bool { return p == "a" }).To(mock).Origin(&origin).Build()
			r := Fun("a")
			So(r, ShouldEqual, "b")
			So(mock2.Times(), ShouldEqual, 1)

			PatchConvey("test reset when", func() {
				mock2.When(func(p string) bool { return p == "b" })
				r := Fun("a")
				So(r, ShouldEqual, "a")
				So(mock2.MockTimes(), ShouldEqual, 0)
			})

			PatchConvey("test reset return", func() {
				mock2.Return("c")
				r := Fun("a")
				So(r, ShouldEqual, "c")
				So(mock2.MockTimes(), ShouldEqual, 1)
			})

			PatchConvey("test reset to and origin", func() {
				origin2 := Fun
				mock := func(p string) string {
					fmt.Println("d")
					return origin2("d") + p
				}
				mock2.To(mock).Origin(&origin2)
				r := Fun("a")
				So(r, ShouldEqual, "da")
				So(mock2.MockTimes(), ShouldEqual, 1)
			})
		})
	})
}

func TestRePatch(t *testing.T) {
	Convey("TestRePatch", t, func() {
		origin := Fun
		mock := func(p string) string {
			fmt.Println("b")
			origin(p)
			return "b"
		}
		mock2 := Mock(Fun).When(func(p string) bool { return p == "a" }).To(mock).Origin(&origin).Build().Patch().Patch()
		defer mock2.UnPatch()
		r := Fun("a")
		So(r, ShouldEqual, "b")
		mock2.UnPatch()
		mock2.UnPatch()
		mock2.UnPatch()
		fmt.Printf("re unpatch can be run")
	})
}

func TestMockUnsafe(t *testing.T) {
	Convey("TestMockUnsafe", t, func() {
		mock := MockUnsafe(ShortFun).To(func() { panic("in hook") }).Build()
		defer mock.UnPatch()
		So(func() { ShortFun() }, ShouldPanicWith, "in hook")
	})
}

type foo struct{ i int }

func (f *foo) Name(i int) string { return fmt.Sprintf("Fn-%v-%v", f.i, i) }

func (f *foo) Foo() int { return f.i * int(math.Abs(1)) }

func TestMockOrigin(t *testing.T) {
	PatchConvey("struct-origin", t, func() {
		PatchConvey("with receiver", func() {
			var ori1 func(*foo, int) string
			var ori2 func(*foo, int) string
			mocker := Mock((*foo).Name).To(func(f *foo, i int) string {
				if i == 1 {
					return ori1(f, i)
				}
				return ori2(f, i)
			}).Origin(&ori1).Build()

			ori2 = func(f *foo, i int) string { return fmt.Sprintf("Fn-mock2-%v", i) }
			So((&foo{100}).Name(1), ShouldEqual, "Fn-100-1")
			So((&foo{200}).Name(1), ShouldEqual, "Fn-200-1")
			So((&foo{100}).Name(2), ShouldEqual, "Fn-mock2-2")
			So((&foo{200}).Name(2), ShouldEqual, "Fn-mock2-2")

			ori1 = func(f *foo, i int) string { return fmt.Sprintf("Fn-mock1-%v", i) }
			mocker.Origin(&ori2)
			So((&foo{100}).Name(1), ShouldEqual, "Fn-mock1-1")
			So((&foo{200}).Name(1), ShouldEqual, "Fn-mock1-1")
			So((&foo{100}).Name(2), ShouldEqual, "Fn-100-2")
			So((&foo{200}).Name(2), ShouldEqual, "Fn-200-2")
		})
		PatchConvey("without receiver", func() {
			var ori1 func(int) string
			var ori2 func(int) string
			mocker := Mock((*foo).Name).To(func(i int) string {
				if i == 1 {
					return ori1(i)
				}
				return ori2(i)
			}).Origin(&ori1).Build()

			ori2 = func(i int) string { return fmt.Sprintf("Fn-mock2-%v", i) }
			So((&foo{100}).Name(1), ShouldEqual, "Fn-100-1")
			So((&foo{200}).Name(1), ShouldEqual, "Fn-200-1")
			So((&foo{100}).Name(2), ShouldEqual, "Fn-mock2-2")
			So((&foo{200}).Name(2), ShouldEqual, "Fn-mock2-2")

			ori1 = func(i int) string { return fmt.Sprintf("Fn-mock1-%v", i) }
			mocker.Origin(&ori2)
			So((&foo{100}).Name(1), ShouldEqual, "Fn-mock1-1")
			So((&foo{200}).Name(1), ShouldEqual, "Fn-mock1-1")
			So((&foo{100}).Name(2), ShouldEqual, "Fn-100-2")
			So((&foo{200}).Name(2), ShouldEqual, "Fn-200-2")
		})
	})
	PatchConvey("issue https://github.com/bytedance/mockey/issues/15", t, func() {
		var origin func() int
		f := &foo{}
		Mock(GetMethod(f, "Foo")).To(func() int { return origin() + 1 }).Origin(&origin).Build()
		So((&foo{1}).Foo(), ShouldEqual, 2)
		So((&foo{2}).Foo(), ShouldEqual, 3)
		So((&foo{3}).Foo(), ShouldEqual, 4)
	})
}

func TestMultiArgs(t *testing.T) {
	PatchConvey("multi-arg-result", t, func() {
		PatchConvey("multi-arg", func() {
			// Go supports passing function arguments from go 1.17
			//
			// Mockey used to use X10 register to make BR instruction in
			// arm64, which will cause arguments and results get a wrong value
			//
			// _0~_15 use x0~x15 register
			fn := func(_0, _1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, _17, _18, _19, _20 int64) {
				fmt.Println(_0, _1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, _17, _18, _19, _20)
			}
			ori := fn
			Mock(fn).To(func(_0, _1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, _17, _18, _19, _20 int64) {
				for _, _x := range []int64{_0, _1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, _17, _18, _19, _20} {
					So(_x, ShouldEqual, 0)
				}
			}).Origin(&ori).Build()
			fn(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
		})
		PatchConvey("multi-result", func() {
			fn := func() (_0, _1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, _17, _18, _19, _20 int64) {
				return 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
			}
			ori := fn
			Mock(fn).To(func() (_0, _1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, _17, _18, _19, _20 int64) {
				return ori()
			}).Origin(&ori).Build()
			_0, _1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, _17, _18, _19, _20 := fn()
			for _, _x := range []int64{_0, _1, _2, _3, _4, _5, _6, _7, _8, _9, _10, _11, _12, _13, _14, _15, _16, _17, _18, _19, _20} {
				So(_x, ShouldEqual, 0)
			}
		})
	})
}
