//go:build go1.20 && !go1.26
// +build go1.20,!go1.26

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

package iface

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"testing"
	"unsafe"

	"github.com/bytedance/mockey"
	. "github.com/smartystreets/goconvey/convey"
)

func TestMockInterface(t *testing.T) {
	mockey.PatchConvey("TestMockInterface", t, func() {
		mockey.PatchConvey("normal", func() {
			Mock(io.Reader.Read).Return(1, io.EOF).Build()

			reader1 := bytes.NewReader(nil)
			reader2 := bytes.NewBufferString("")
			reader3 := new(net.TCPConn)
			reader4 := new(os.File)

			fmt.Println(io.ReadAll(reader1)) // [0] <nil>
			fmt.Println(io.ReadAll(reader2)) // [0] <nil>
			fmt.Println(io.ReadAll(reader3)) // [0] <nil>
			fmt.Println(io.ReadAll(reader4)) // [0] <nil>
		})

		mockey.PatchConvey("with selector", func() {
			Mock(io.Reader.Read, SelectType("Reader")).Return(1, io.EOF).Build()

			reader1 := bytes.NewReader(nil)
			reader2 := bytes.NewBufferString("")

			fmt.Println(io.ReadAll(reader1)) // [0] <nil>
			fmt.Println(io.ReadAll(reader2)) // [] <nil>, not mocked
		})

	})

}

type MyI interface {
	Foo1(string) string
	Foo2()
}

type MyIImpl1 struct {
	inner string
}

func (m *MyIImpl1) Foo1(s string) string {
	return s + m.inner
}

func (m *MyIImpl1) Foo2() {}

type MyIImpl2 struct {
	inner  int
	inner2 int
}

func (m MyIImpl2) Foo1(s string) string {
	a := fmt.Sprintf("%d", m.inner)
	b := fmt.Sprintf("%d", m.inner2)
	return s + a + b
}

func (m MyIImpl2) Foo2() {}

type MyIImpl3 struct {
}

func (m MyIImpl3) Foo1(s string) string {
	return s + "12"
}

func (m MyIImpl3) Foo2() {}

type MyIImpl4 int64

func (m MyIImpl4) Foo1(s string) string {
	return s + fmt.Sprint(m)
}

func (m MyIImpl4) Foo2() {}

type NotImpl struct{}

func (n *NotImpl) Foo1(s string, i int) string {
	return s + fmt.Sprint(i)
}

func CallFoo(i MyI, s string) string {
	return i.Foo1(s)
}

func TestMockInterface_Return(t *testing.T) {
	Convey("TestMockInterface_Return", t, func() {
		s := "anything"
		impl1 := &MyIImpl1{inner: "12"}
		impl2 := MyIImpl2{inner: 1, inner2: 2}
		impl2p := &impl2
		impl3 := MyIImpl3{}
		impl3p := &impl3
		impl4 := MyIImpl4(12)
		impl4p := &impl4
		n := &NotImpl{}

		mocker := Mock(MyI.Foo1).Return("MOCKED!").Build()

		So(CallFoo(impl1, s), ShouldEqual, "MOCKED!")
		var i1 MyI
		i1 = impl1
		So(i1.Foo1(s), ShouldEqual, "MOCKED!")

		So(CallFoo(impl2, s), ShouldEqual, "MOCKED!")
		var i2 MyI
		i2 = impl2
		So(i2.Foo1(s), ShouldEqual, "MOCKED!")

		So(CallFoo(impl2p, s), ShouldEqual, "MOCKED!")
		var i2p MyI
		i2p = impl2p
		So(i2p.Foo1(s), ShouldEqual, "MOCKED!")

		So(CallFoo(impl3, s), ShouldEqual, "MOCKED!")
		var i3 MyI
		i3 = impl3
		So(i3.Foo1(s), ShouldEqual, "MOCKED!")

		So(CallFoo(impl3p, s), ShouldEqual, "MOCKED!")
		var i3p MyI
		i3p = impl3p
		So(i3p.Foo1(s), ShouldEqual, "MOCKED!")

		So(CallFoo(impl4, s), ShouldEqual, "MOCKED!")
		var i4 MyI
		i4 = impl4
		So(i4.Foo1(s), ShouldEqual, "MOCKED!")

		So(CallFoo(impl4p, s), ShouldEqual, "MOCKED!")
		var i4p MyI
		i4p = impl4p
		So(i4p.Foo1(s), ShouldEqual, "MOCKED!")

		So(n.Foo1(s, 12), ShouldEqual, "anything12")

		mocker.UnPatch()

		So(i1.Foo1(s), ShouldEqual, "anything12")
		So(i2.Foo1(s), ShouldEqual, "anything12")
		So(i3.Foo1(s), ShouldEqual, "anything12")
		So(i4.Foo1(s), ShouldEqual, "anything12")
		So(n.Foo1(s, 12), ShouldEqual, "anything12")
	})
}

func TestMockInterface_To(t *testing.T) {
	Convey("TestMockInterface_To", t, func() {
		s := "anything"
		impl1 := &MyIImpl1{inner: "12"}
		impl2 := MyIImpl2{inner: 1, inner2: 2}
		impl2p := &impl2
		impl3 := MyIImpl3{}
		impl3p := &impl3
		impl4 := MyIImpl4(12)
		impl4p := &impl4
		n := &NotImpl{}

		Convey("without receiver", func() {
			mocker := Mock(MyI.Foo1).To(func(s string) string {
				return "MOCKED!" + s
			}).Build()

			So(CallFoo(impl1, s), ShouldEqual, "MOCKED!anything")
			var i1 MyI
			i1 = impl1
			So(i1.Foo1(s), ShouldEqual, "MOCKED!anything")

			So(CallFoo(impl2, s), ShouldEqual, "MOCKED!anything")
			var i2 MyI
			i2 = impl2
			So(i2.Foo1(s), ShouldEqual, "MOCKED!anything")

			So(CallFoo(impl2p, s), ShouldEqual, "MOCKED!anything")
			var i2p MyI
			i2p = impl2p
			So(i2p.Foo1(s), ShouldEqual, "MOCKED!anything")

			So(CallFoo(impl3, s), ShouldEqual, "MOCKED!anything")
			var i3 MyI
			i3 = impl3
			So(i3.Foo1(s), ShouldEqual, "MOCKED!anything")

			So(CallFoo(impl3p, s), ShouldEqual, "MOCKED!anything")
			var i3p MyI
			i3p = impl3p
			So(i3p.Foo1(s), ShouldEqual, "MOCKED!anything")

			So(CallFoo(impl4, s), ShouldEqual, "MOCKED!anything")
			var i4 MyI
			i4 = impl4
			So(i4.Foo1(s), ShouldEqual, "MOCKED!anything")

			So(CallFoo(impl4p, s), ShouldEqual, "MOCKED!anything")
			var i4p MyI
			i4p = impl4p
			So(i4p.Foo1(s), ShouldEqual, "MOCKED!anything")

			So(n.Foo1(s, 12), ShouldEqual, "anything12")

			mocker.UnPatch()

			So(i1.Foo1(s), ShouldEqual, "anything12")
			So(i2.Foo1(s), ShouldEqual, "anything12")
			So(i3.Foo1(s), ShouldEqual, "anything12")
			So(i4.Foo1(s), ShouldEqual, "anything12")
			So(n.Foo1(s, 12), ShouldEqual, "anything12")
		})

		Convey("with receiver", func() {
			mocker := Mock(MyI.Foo1).To(func(r unsafe.Pointer, s string) string {
				prefix := "MOCKED!"
				switch r {
				case unsafe.Pointer(impl1):
					receiver := (*MyIImpl1)(r)
					return prefix + s + receiver.inner
				case unsafe.Pointer(impl2p):
					receiver := (*MyIImpl2)(r)
					a := fmt.Sprintf("%d", receiver.inner)
					b := fmt.Sprintf("%d", receiver.inner2)
					return prefix + s + a + b
				case unsafe.Pointer(&impl3p):
					return prefix + s
				case unsafe.Pointer(impl4p):
					receiver := (*MyIImpl4)(r)
					return prefix + s + fmt.Sprint(*receiver)
				default:
					return "MOCKED!"
				}
			}).Build()

			So(CallFoo(impl1, s), ShouldEqual, "MOCKED!anything12")
			var i1 MyI
			i1 = impl1
			So(i1.Foo1(s), ShouldEqual, "MOCKED!anything12")

			So(CallFoo(impl2, s), ShouldEqual, "MOCKED!")
			var i2 MyI
			i2 = impl2
			So(i2.Foo1(s), ShouldEqual, "MOCKED!")

			So(CallFoo(impl2p, s), ShouldEqual, "MOCKED!anything12")
			var i2p MyI
			i2p = impl2p
			So(i2p.Foo1(s), ShouldEqual, "MOCKED!anything12")

			So(CallFoo(impl3, s), ShouldEqual, "MOCKED!")
			var i3 MyI
			i3 = impl3
			So(i3.Foo1(s), ShouldEqual, "MOCKED!")

			So(CallFoo(impl4, s), ShouldEqual, "MOCKED!")
			var i4 MyI
			i4 = impl4
			So(i4.Foo1(s), ShouldEqual, "MOCKED!")

			So(CallFoo(impl4p, s), ShouldEqual, "MOCKED!anything12")
			var i4p MyI
			i4p = impl4p
			So(i4p.Foo1(s), ShouldEqual, "MOCKED!anything12")

			So(n.Foo1(s, 12), ShouldEqual, "anything12")

			mocker.UnPatch()

			So(i1.Foo1(s), ShouldEqual, "anything12")
			So(i2.Foo1(s), ShouldEqual, "anything12")
			So(i3.Foo1(s), ShouldEqual, "anything12")
			So(i4.Foo1(s), ShouldEqual, "anything12")
			So(n.Foo1(s, 12), ShouldEqual, "anything12")
		})
	})
}

func TestMockInterface_When(t *testing.T) {
	Convey("TestMockInterface_When", t, func() {
		s1 := "anything"
		s2 := "nothing"
		impl1 := &MyIImpl1{inner: "12"}

		Convey("without receiver", func() {
			mocker := Mock(MyI.Foo1).When(func(s string) bool {
				return s == "anything"
			}).To(func(s string) string {
				return "MOCKED!" + s
			}).Build()

			So(CallFoo(impl1, s1), ShouldEqual, "MOCKED!anything")
			So(CallFoo(impl1, s2), ShouldEqual, "nothing12")
			var i1 MyI
			i1 = impl1
			So(i1.Foo1(s1), ShouldEqual, "MOCKED!anything")
			So(i1.Foo1(s2), ShouldEqual, "nothing12")
			So(mocker.Times(), ShouldEqual, 4)
			So(mocker.MockTimes(), ShouldEqual, 2)

			mocker.UnPatch()

			So(i1.Foo1(s1), ShouldEqual, "anything12")
			So(i1.Foo1(s2), ShouldEqual, "nothing12")
			So(mocker.Times(), ShouldEqual, 0)
			So(mocker.MockTimes(), ShouldEqual, 0)
		})

		Convey("with receiver", func() {
			mocker := Mock(MyI.Foo1).When(func(r unsafe.Pointer, s string) bool {
				receiver := (*MyIImpl1)(r)
				return s == "anything" && receiver.inner == "12"
			}).To(func(r unsafe.Pointer, s string) string {
				receiver := (*MyIImpl1)(r)
				return "MOCKED!" + s + receiver.inner
			}).Build()

			So(CallFoo(impl1, s1), ShouldEqual, "MOCKED!anything12")
			So(CallFoo(impl1, s2), ShouldEqual, "nothing12")
			var i1 MyI
			i1 = impl1
			So(i1.Foo1(s1), ShouldEqual, "MOCKED!anything12")
			So(i1.Foo1(s2), ShouldEqual, "nothing12")
			So(mocker.Times(), ShouldEqual, 4)
			So(mocker.MockTimes(), ShouldEqual, 2)

			mocker.UnPatch()

			So(i1.Foo1(s1), ShouldEqual, "anything12")
			So(i1.Foo1(s2), ShouldEqual, "nothing12")
			So(mocker.Times(), ShouldEqual, 0)
			So(mocker.MockTimes(), ShouldEqual, 0)
		})
	})
}

func TestMockInterface_Selector(t *testing.T) {
	Convey("TestMockInterface_Selector", t, func() {
		s := "anything"
		impl1 := &MyIImpl1{inner: "12"}
		impl2 := MyIImpl2{inner: 1, inner2: 2}
		impl2p := &impl2
		impl3 := MyIImpl3{}
		impl3p := &impl3
		impl4 := MyIImpl4(12)
		impl4p := &impl4

		mockey.PatchConvey("type", func() {
			Mock(MyI.Foo1,
				SelectType("MyIImpl1", "MyIImpl2", "UNKNOWN"),
			).Return("MOCKED!").Build()

			So(CallFoo(impl1, s), ShouldEqual, "MOCKED!")
			So(CallFoo(impl2, s), ShouldEqual, "MOCKED!")
			So(CallFoo(impl2p, s), ShouldEqual, "MOCKED!")
			So(CallFoo(impl3, s), ShouldEqual, "anything12")
			So(CallFoo(impl3p, s), ShouldEqual, "anything12")
			So(CallFoo(impl4, s), ShouldEqual, "anything12")
			So(CallFoo(impl4p, s), ShouldEqual, "anything12")
		})

		mockey.PatchConvey("pkg", func() {
			Mock(MyI.Foo1,
				SelectPkg("github.com/bytedance/mockey/exp/iface", "UNKNOWN"),
			).Return("MOCKED!").Build()

			So(CallFoo(impl1, s), ShouldEqual, "MOCKED!")
			So(CallFoo(impl2, s), ShouldEqual, "MOCKED!")
			So(CallFoo(impl2p, s), ShouldEqual, "MOCKED!")
			So(CallFoo(impl3, s), ShouldEqual, "MOCKED!")
			So(CallFoo(impl3p, s), ShouldEqual, "MOCKED!")
			So(CallFoo(impl4, s), ShouldEqual, "MOCKED!")
			So(CallFoo(impl4p, s), ShouldEqual, "MOCKED!")
		})

		mockey.PatchConvey("type pkg", func() {
			Mock(MyI.Foo1,
				SelectType("MyIImpl1", "MyIImpl2", "UNKNOWN"),
				SelectPkg("github.com/bytedance/mockey/exp/iface", "UNKNOWN"),
			).Return("MOCKED!").Build()

			So(CallFoo(impl1, s), ShouldEqual, "MOCKED!")
			So(CallFoo(impl2, s), ShouldEqual, "MOCKED!")
			So(CallFoo(impl2p, s), ShouldEqual, "MOCKED!")
			So(CallFoo(impl3, s), ShouldEqual, "anything12")
			So(CallFoo(impl3p, s), ShouldEqual, "anything12")
			So(CallFoo(impl4, s), ShouldEqual, "anything12")
			So(CallFoo(impl4p, s), ShouldEqual, "anything12")
		})
	})
}
