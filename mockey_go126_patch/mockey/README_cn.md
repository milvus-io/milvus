# Mockey 

[English](README.md) | 中文

[![Release](https://img.shields.io/github/v/release/bytedance/mockey)](https://github.com/bytedance/mockey/releases)
[![License](https://img.shields.io/github/license/bytedance/mockey)](https://github.com/bytedance/mockey/blob/main/LICENSE-APACHE)
[![Go Report Card](https://goreportcard.com/badge/github.com/bytedance/mockey)](https://goreportcard.com/report/github.com/bytedance/mockey)
[![codecov](https://codecov.io/github/bytedance/mockey/graph/badge.svg?token=JKL3WSE3I3)](https://codecov.io/github/bytedance/mockey)
[![OpenIssue](https://img.shields.io/github/issues/bytedance/mockey)](https://github.com/bytedance/mockey/issues)
[![ClosedIssue](https://img.shields.io/github/issues-closed/bytedance/mockey)](https://github.com/bytedance/mockey/issues?q=is%3Aissue+is%3Aclosed)

Mockey 是一个简单易用的 Golang 打桩库，可以快速方便地进行函数和变量的 mock。目前，mockey 已广泛应用于字节跳动服务的单元测试编写中（7k+ 仓库）并积极维护，且已经成为一些部门的单元测试标准。它的底层原理是在运行时重写函数指令，这一点和 [monkey](https://github.com/bouk/monkey) 或 [gomonkey](https://github.com/agiledragon/gomonkey) 类似。

Mockey 使得 mock 函数、方法和变量变得容易，不需要将所有依赖指定为接口类型并注入对应的测试实现。

> 1. 要求**在编译时禁用内联和编译优化**，否则无法工作，详见[FAQ](#如何禁用内联和编译优化)。
> 2. 强烈建议在单元测试中与 [goconvey](https://github.com/smartystreets/goconvey) 库一起使用。

## 安装
```
go get github.com/bytedance/mockey@latest
```

## 快速上手
### 最简单的例子
```go
package main

import (
	"fmt"
	"math/rand"

	. "github.com/bytedance/mockey"
)

func main() {
	Mock(rand.Int).Return(1).Build() // mock `rand.Int` 总是返回 1
	
	fmt.Printf("rand.Int() 总是返回: %v\n", rand.Int()) // 试试还随机吗？
}
```

### 单元测试的例子
```go
package main_test

import (
	"math/rand"
	"testing"

	. "github.com/bytedance/mockey"
	. "github.com/smartystreets/goconvey/convey"
)

// Win 需要测试的函数，输入一个数，如果其比随机数大则获胜，否则失败
func Win(in int) bool {
	return in > rand.Int()
}

func TestWin(t *testing.T) {
	PatchConvey("TestWin", t, func() {
		Mock(rand.Int).Return(100).Build() // mock
		
		res1 := Win(101)                   // 执行
		So(res1, ShouldBeTrue)             // 断言
		
		res2 := Win(99)         // 执行
		So(res2, ShouldBeFalse) // 断言
	})
}
```

## 特性
- 函数和方法
  - 基础功能
    - 简单/泛型/可变参数函数或方法（值或指针接收器）
    - 支持钩子函数
    - 支持`PatchConvey`和`PatchRun`（每个测试用例后自动释放 mock）
    - 提供`GetMethod`处理特殊情况（如未导出类型、未导出方法和嵌套结构体中的方法）
  - 高级功能
    - 条件 mock
    - 序列返回
    - 装饰器模式（在 mock 的同时执行原始函数）
    - Goroutine过滤（包含、排除、目标定位）
    - 获取`Mocker`用于高级用法（如获取目标/mock 函数的执行次数）
- mock 变量
  - 普通变量
  - 函数变量

## 兼容性
### 操作系统支持
- Mac OS(Darwin)
- Linux
- Windows

### 架构支持
- AMD64
- ARM64

### 版本支持
- Go 1.13+

## 基础特性
### 简单函数/方法
使用 `Mock` 来 mock 函数/方法，使用 `Return` 来指定返回值，使用 `Build` 来使得 mock 生效：
```go
package main

import (
	"fmt"

	. "github.com/bytedance/mockey"
)

func Foo(in string) string {
	return in
}

type A struct{}

func (a A) Foo(in string) string { return in }

type B struct{}

func (b *B) Foo(in string) string { return in }

func main() {
	// mock 函数
	Mock(Foo).Return("MOCKED!").Build()
	fmt.Println(Foo("anything")) // MOCKED!

	// mock 方法（值接收器）
	Mock(A.Foo).Return("MOCKED!").Build()
	fmt.Println(A{}.Foo("anything")) // MOCKED!

	// mock 方法（指针接收器）
	Mock((*B).Foo).Return("MOCKED!").Build()
	fmt.Println(new(B).Foo("anything")) // MOCKED!
    
    // 提示：如果目标函数没有返回值，您仍然需要调用空的 `Return()` 或者使用 `To` 自定义钩子函数。
}
```

### 泛型函数/方法
> 从 v1.3.0 版本开始，`Mock`试验性地加入了自动识别泛型的能力（针对 go1.20+），可以使用`Mock`直接替换`MockGeneric`

使用 `MockGeneric` 来 mock 泛型函数/方法：
```go
package main

import (
	"fmt"

	. "github.com/bytedance/mockey"
)

func FooGeneric[T any](t T) T {
	return t
}

type GenericClass[T any] struct {
}

func (g *GenericClass[T]) Foo(t T) T {
	return t
}

func main() {
	// mock 泛型函数
	MockGeneric(FooGeneric[string]).Return("MOCKED!").Build() // `Mock(FooGeneric[string], OptGeneric)` 也可以工作
	fmt.Println(FooGeneric("anything"))                       // MOCKED!
	fmt.Println(FooGeneric(1))                                // 1 | 不工作，因为类型不匹配！

	// mock 泛型方法
	MockGeneric((*GenericClass[string]).Foo).Return("MOCKED!").Build()
	fmt.Println(new(GenericClass[string]).Foo("anything")) // MOCKED!
}
```

另外，Golang 泛型对于同底层类型的不同类型，其泛型实现是共用的。比如 `type MyString string` 中 `MyString` 和 `string` 的实现是一套。因此对于其中一种类型的 mock 会干扰另一种类型。
```go
package main

import (
	"fmt"

	. "github.com/bytedance/mockey"
)

type MyString string

func FooGeneric[T any](t T) T {
	return t
}

func main() {
	MockGeneric(FooGeneric[string]).Return("MOCKED!").Build()
	fmt.Println(FooGeneric("anything"))           // MOCKED!
	fmt.Println(FooGeneric[MyString]("anything")) // MOCKED! | 这里是因为mock了string类型后的相互干扰
}
```

在 v1.3.1 版本中上述问题得到了解决，我们支持了 mock 具有相同 gcshape 但实际类型不同的泛型函数/方法，上面的例子的表现会更加符合预期：
```go
package main

import (
	"fmt"

	. "github.com/bytedance/mockey"
)

type MyString string

func FooGeneric[T any](t T) T {
	return t
}

func main() {
	mocker1 := MockGeneric(FooGeneric[string]).Return("MOCKED!").Build()
	fmt.Println(FooGeneric("anything"))           // MOCKED!
	fmt.Println(FooGeneric[MyString]("anything")) // anything | 不再干扰
	mocker2 := MockGeneric(FooGeneric[MyString]).Return("MOCKED2!").Build()
	fmt.Println(FooGeneric("anything"))           // MOCKED!
	fmt.Println(FooGeneric[MyString]("anything")) // MOCKED2! ｜ 不再干扰

	// 注意：如果您需要手动释放 mocker，务必按照「后入先出」的顺序，否则会导致崩溃等非预期的结果
	mocker2.UnPatch()
	fmt.Println(FooGeneric("anything"))           // MOCKED!
	fmt.Println(FooGeneric[MyString]("anything")) // anything
	mocker1.UnPatch()
	fmt.Println(FooGeneric("anything"))           // anything
	fmt.Println(FooGeneric[MyString]("anything")) // anything
}
```

### 可变参数函数/方法
```go
package main

import (
	"fmt"

	. "github.com/bytedance/mockey"
)

func FooVariadic(in ...string) string {
	return in[0]
}

type A struct{}

func (a A) FooVariadic(in ...string) string { return in[0] }

func main() {
	// mock 可变参数函数
	Mock(FooVariadic).Return("MOCKED!").Build()
	fmt.Println(FooVariadic("anything")) // MOCKED!

	// mock 可变参数方法
	Mock(A.FooVariadic).Return("MOCKED!").Build()
	fmt.Println(A{}.FooVariadic("anything")) // MOCKED!
}
```

### 支持钩子函数
使用 `To` 来指定钩子函数：
```go
package main

import (
	"fmt"

	. "github.com/bytedance/mockey"
)

func Foo(in string) string {
	return in
}

type A struct {
	prefix string
}

func (a A) Foo(in string) string { return a.prefix + ":" + in }

func main() {
	// 注意：钩子函数必须与原始函数具有相同的函数签名！
	Mock(Foo).To(func(in string) string { return "MOCKED!" }).Build()
	fmt.Println(Foo("anything")) // MOCKED!

	// 注意：对于方法 mock，可以根据需要将接收器添加到钩子函数的签名中（如果没有用到接收器也可以省略不加，mockey可以兼容）。
	Mock(A.Foo).To(func(a A, in string) string { return a.prefix + ":inner:" + "MOCKED!" }).Build()
	fmt.Println(A{prefix: "prefix"}.Foo("anything")) // prefix:inner:MOCKED!
}
```

### 支持`PatchConvey`和`PatchRun`
> 从 v1.4.1 版本开始支持 `PatchRun`

`PatchConvey`和`PatchRun`都是用于管理 mock 生命周期的工具，它们会在测试用例或函数执行完成后自动释放 mock，从而免去`defer`的苦恼。`PatchConvey`和`PatchRun`都支持嵌套使用，每层只会释放自己内部的 mock。

适用场景对比：
- 当您需要使用 goconvey 框架的断言功能和测试组织能力时，推荐使用`PatchConvey`；嵌套`PatchConvey`的执行顺序和`Convey`一致，请参考 goconvey 相关[文档](https://github.com/smartystreets/goconvey/wiki/Execution-order)
- 当您不需要 goconvey 集成时，推荐使用更轻量级的`PatchRun`

`PatchConvey`示例如下：
```go
package main_test

import (
	"testing"

	. "github.com/bytedance/mockey"
	. "github.com/smartystreets/goconvey/convey"
)

func Foo(in string) string {
	return "ori:" + in
}

func TestXXX(t *testing.T) {
	PatchConvey("TestXXX", t, func() {
		// mock
		PatchConvey("mock 1", func() {
			Mock(Foo).Return("MOCKED-1!").Build() // mock
			res := Foo("anything")                // 调用
			So(res, ShouldEqual, "MOCKED-1!")     // 断言
		})
		// mock 已释放
		PatchConvey("mock released", func() {
			res := Foo("anything")               // 调用
			So(res, ShouldEqual, "ori:anything") // 断言
		})
		// 再次 mock
		PatchConvey("mock 2", func() {
			Mock(Foo).Return("MOCKED-2!").Build() // mock
			res := Foo("anything")                // 调用
			So(res, ShouldEqual, "MOCKED-2!")     // 断言
		})
	})
}
```

`PatchRun`示例如下：
```go
package main_test

import (
	"testing"

	. "github.com/bytedance/mockey"
)

func Foo(in string) string {
	return "ori:" + in
}

func TestXXX(t *testing.T) {
	// mock
	PatchRun(func() {
		Mock(Foo).Return("MOCKED-1!").Build() // mock
		res := Foo("anything")                // 调用
		if res != "MOCKED-1!" {
			t.Errorf("expected 'MOCKED-1!', got '%s'", res)
		}
	})

	// mock 已释放
	res := Foo("anything") // 调用
	if res != "ori:anything" {
		t.Errorf("expected 'ori:anything', got '%s'", res)
	}

	// 再次 mock
	PatchRun(func() {
		Mock(Foo).Return("MOCKED-2!").Build() // mock
		res := Foo("anything")                // 调用
		if res != "MOCKED-2!" {
			t.Errorf("expected 'MOCKED-2!', got '%s'", res)
		}
	})

	// mock 已释放
	res = Foo("anything") // 调用
	if res != "ori:anything" {
		t.Errorf("expected 'ori:anything', got '%s'", res)
	}
}

```

### 提供 `GetMethod` 处理特殊情况
在无法直接 mock 或者 mock 不生效特殊情况下，可以使用`GetMethod`在获取相应方法后 mock，使用前请确保传入的对象不为 nil。

通过实例 mock 方法（含接口类型的实例）：
```go
package main

import (
	"fmt"

	. "github.com/bytedance/mockey"
)

type A struct{}

func (a A) Foo(in string) string { return in }

func main() {
	a := new(A)

	// Mock(a.Foo) 不起作用，因为 `a` 是 `A` 的实例，不是类型 `A`
	Mock(GetMethod(a, "Foo")).Return("MOCKED!").Build()
	fmt.Println(a.Foo("anything")) // MOCKED!
	
	// 提示：如果实例是接口类型，同样可以使用
	// var ia interface{ Foo(string) string } = new(A)
	// Mock(GetMethod(ia, "Foo")).Return("MOCKED!").Build()
}
```

mock 未导出类型的方法：
```go
package main

import (
	"crypto/sha256"
	"fmt"

	. "github.com/bytedance/mockey"
)

func main() {
	// `sha256.New()` 返回未导出的 `*digest`，我们想要 mock 它的 `Sum` 方法
	Mock(GetMethod(sha256.New(), "Sum")).Return([]byte{0}).Build()

	fmt.Println(sha256.New().Sum([]byte("anything"))) // [0]
	
	// 提示：这里是「通过实例 mock 方法」的特殊情况，即实例对应的类型是未导出的
}
```

mock 未导出方法：
```go
package main

import (
	"bytes"
	"fmt"

	. "github.com/bytedance/mockey"
)

func main() {
	// `*bytes.Buffer` 有一个未导出的 `empty` 方法，这是我们想要 mock 的
	Mock(GetMethod(new(bytes.Buffer), "empty")).Return(true).Build()

	buf := bytes.NewBuffer([]byte{1, 2, 3, 4})
	b, err := buf.ReadByte()
	fmt.Println(b, err)      // 0 EOF | `ReadByte` 在内部调用 `empty` 方法检查缓冲区是否为空并返回 io.EOF
}
```
注意，Go 编译器有可能对未导出的方法直接抹除其类型信息，此时`GetMethod`将无法获取到该方法。这种情况下可以使用`OptUnexportedTargetType`指定其类型：
```go
package main

import (
	"bytes"
	"fmt"

	. "github.com/bytedance/mockey"
)

func main() {
	var targetType func() bool // `empty`方法的签名（不含receiver）
	target := GetMethod(new(bytes.Buffer), "empty", OptUnexportedTargetType(targetType))
	Mock(target).Return(true).Build()

	buf := bytes.NewBuffer([]byte{1, 2, 3, 4})
	b, err := buf.ReadByte()
	fmt.Println(b, err) // 0 EOF | `ReadByte` 在内部调用 `empty` 方法检查缓冲区是否为空并返回 io.EOF
}
```

mock 嵌套结构体中的方法：
```go
package main

import (
	"fmt"

	. "github.com/bytedance/mockey"
)

type Wrapper struct {
	inner // 嵌套结构体
}

type inner struct{}

func (i inner) Foo(in string) string {
	return in
}

func main() {
	// Mock(Wrapper.Foo) 不起作用，因为目标应该是 `inner.Foo`
	Mock(GetMethod(Wrapper{}, "Foo")).Return("MOCKED!").Build() // 或者 Mock(inner.Foo).Return("MOCKED!").Build()

	fmt.Println(Wrapper{}.Foo("anything")) // MOCKED! 
}
```

## 高级特性
### 条件 mock
使用 `When` 定义多个条件：
```go
package main

import (
	"fmt"

	. "github.com/bytedance/mockey"
)

func Foo(in string) string {
	return "ori:" + in
}

func main() {
	// 注意：条件函数必须与原始函数具有相同的输入签名！
	Mock(Foo).
		When(func(in string) bool { return len(in) == 0 }).Return("EMPTY").
		When(func(in string) bool { return len(in) <= 2 }).Return("SHORT").
		When(func(in string) bool { return len(in) <= 5 }).Return("MEDIUM").
		Build()

	fmt.Println(Foo(""))            // EMPTY
	fmt.Println(Foo("h"))           // SHORT
	fmt.Println(Foo("hello"))       // MEDIUM
	fmt.Println(Foo("hello world")) // ori:hello world
}
```

### 序列返回
使用 `Sequence` mock 多个返回值：
```go
package main

import (
	"fmt"

	. "github.com/bytedance/mockey"
)

func Foo(in string) string {
	return in
}

func main() {
	Mock(Foo).Return(Sequence("Alice").Then("Bob").Times(2).Then("Tom")).Build()
	fmt.Println(Foo("anything")) // Alice
	fmt.Println(Foo("anything")) // Bob
	fmt.Println(Foo("anything")) // Bob
	fmt.Println(Foo("anything")) // Tom
}
```

### 装饰器模式
使用 `Origin` 在 mock 的同时保留目标的原始逻辑：
```go
package main

import (
	"fmt"

	. "github.com/bytedance/mockey"
)

func Foo(in string) string {
	return "ori:" + in
}

func main() {
	// `origin` 将携带 `Foo` 函数的逻辑
	origin := Foo

	// `decorator` 将在 `origin` 周围做一些 AOP
	decorator := func(in string) string {
		fmt.Println("arg is", in)
		out := origin(in)
		fmt.Println("res is", out)
		return out
	}

	Mock(Foo).Origin(&origin).To(decorator).Build()

	fmt.Println(Foo("anything"))
	/*
		arg is anything
		res is ori:anything
		ori:anything
	*/
}
```

### Goroutine过滤
Mock 默认会在所有协程中生效，可以使用如下 API 指定在某些协程中生效，其他协程不生效：
- `IncludeCurrentGoRoutine`：只在当前 goroutine 生效
- `ExcludeCurrentGoRoutine`： 在当前 goroutine 外的所有 goroutine 生效
- `FilterGoRoutine`：Include 或者 Exclude指定的 goroutine（通过 goroutine id）

```go
package main

import (
	"fmt"
	"time"

	. "github.com/bytedance/mockey"
)

func Foo(in string) string {
	return in
}

func main() {
	// 使用 `ExcludeCurrentGoRoutine` 排除当前协程
	Mock(Foo).ExcludeCurrentGoRoutine().Return("MOCKED!").Build()
	fmt.Println(Foo("anything")) // anything | mock在当前协程中不起作用

	go func() {
		fmt.Println(Foo("anything")) // MOCKED! | mock在其他协程中起作用
	}()

	time.Sleep(time.Second) // 等待 goroutine 完成
	
	// 提示：可以使用`GetGoroutineId`获取当前协程的ID
}
```

### 获取 `Mocker`
获取 `Mocker` 以使用高级特性：
```go
package main

import (
	"fmt"

	. "github.com/bytedance/mockey"
)

func Foo(in string) string {
	return in
}

func main() {
	mocker := Mock(Foo).When(func(in string) bool { return len(in) > 5 }).Return("MOCKED!").Build()
	fmt.Println(Foo("any"))      // any
	fmt.Println(Foo("anything")) // MOCKED!

	// 使用 `MockTimes` 和 `Times` 跟踪 mock 工作次数和 `Foo` 被调用次数
	fmt.Println(mocker.MockTimes()) // 1
	fmt.Println(mocker.Times())     // 2
	
	// 提示：重新mock或者释放mock时，相关的计数都会重置为0

	// 重新 mock `Foo` 返回 "MOCKED2!"
	mocker.Return("MOCKED2!")
	fmt.Println(Foo("anything")) // MOCKED2!
	fmt.Println(mocker.MockTimes()) // 1 | 重新mock时被重置为0

	// 释放 `Foo` mock
	mocker.Release()
	fmt.Println(Foo("anything")) // anything | mock 不起作用，因为 mock 已释放
	fmt.Println(mocker.MockTimes()) // 0 | 释放时被重置为0
}
```

## 常见问题
### 如何禁用内联和编译优化？
1. 命令行：使用 `go build -gcflags="all=-l -N"`，测试时使用 `go test -gcflags="all=-l -N" ./...` 。
2. Goland：使用 「Debug 模式」或在 **Run/Debug Configurations > Go tool arguments** 对话框中填写 `-gcflags="all=-l -N"`。
3. VSCode: 使用 「Debug 模式」或在 `settings.json` 中添加 `"go.buildFlags": ["-gcflags=\'all=-N -l\'"]`。

### mock 不起作用？
1. 未禁用内联或编译优化。请检查是否已打印此日志并参考[FAQ](#如何禁用内联和编译优化)。
    ```
    Mockey check failed, please add -gcflags="all=-N -l".
    ```
2. 检查是否未调用`Build()`方法，或者既没有调用`Return(xxx)`也没有调用`To(xxx)`，这将导致没有实际效果。如果目标函数没有返回值，您仍然需要调用空的 `Return()` 或者使用 `To` 自定义钩子函数。
3. mock 目标不完全匹配。检查是否有如下所示的问题，或者请检查是否遇到`GetMethod`说明中的[特定情况](#提供-getmethod-处理特殊情况)
    ```go
    package main_test
    
    import (
    	"fmt"
    	"testing"
    
    	. "github.com/bytedance/mockey"
    )
    
    type A struct{}
    
    func (a A) Foo(in string) string { return in }
    
    func TestXXX(t *testing.T) {
    	Mock((*A).Foo).Return("MOCKED!").Build()
    	fmt.Println(A{}.Foo("anything")) // 不起作用，因为 mock 目标应该是 `A.Foo`
    
    	a := A{}
    	Mock(a.Foo).Return("MOCKED!").Build()
    	fmt.Println(a.Foo("anything")) // 不起作用，因为 mock 目标应该是 `A.Foo`
    }
    ```
4. 目标函数在 mock 释放时在其他goroutine中执行：
    ```go
    package main_test
    
    import (
    	"fmt"
    	"testing"
    	"time"
    
    	. "github.com/bytedance/mockey"
    )
    
    func Foo(in string) string {
    	return in
    }
    
    func TestXXX(t *testing.T) {
    	PatchConvey("TestXXX", t, func() {
    		Mock(Foo).Return("MOCKED!").Build()
    		go func() { fmt.Println(Foo("anything")) }() // 执行 'Foo' 的时机不确定
    	})
    	// 当主goroutine来到这里时，相关 mock 已被 'PatchConvey' 释放。如果 'Foo' 在这之前执行， mock 成功，否则失败
    	fmt.Println("over")
    	time.Sleep(time.Second)
    }
    ```
5. 调用先于 mock 执行。请尝试打断点到原函数第一行，如果执行到第一行时堆栈不在单测的 mock 代码后，则是该问题，常见于 `init()` 函数里的初始化代码。如何 mock `init()`中的函数见 [FAQ](#如何-mock-依赖包-init-里的函数)。
6. 对泛型函数使用了非泛型的 mock。详见[泛型函数/方法](#泛型函数方法)小节。
7. arm64 架构下偶发的 mock 失效或者无法恢复 mock。请升级到最新版本，详见[#90](https://github.com/bytedance/mockey/issues/90)。

### 如何 mock interface 类型？
方法一：使用 `GetMethod` 从实例获取相应方法
```go
package main

import (
	"fmt"

	. "github.com/bytedance/mockey"
)

type FooI interface {
	Foo(string) string
}

func NewFoo() FooI {
	return &foo{}
}

// foo 是 'FooI' 的原始实现
type foo struct{}

func (f *foo) Foo(in string) string {
	return in
}

func main() {
	// 获取原始实现并进行mock
	instance := NewFoo()
	Mock(GetMethod(instance, "Foo")).Return("MOCKED!").Build()

	fmt.Println(instance.Foo("anything")) // MOCKED!
}
```

方法二：创建一个dummy实现类型，并 mock 对应的构造函数返回该类型
```go
package main

import (
	"fmt"

	. "github.com/bytedance/mockey"
)

type FooI interface {
	Foo(string) string
}

func NewFoo() FooI {
	return &foo{}
}

// foo 是 'FooI' 的原始实现
type foo struct{}

func (f *foo) Foo(in string) string {
	return in
}

func main() {
	// 生成 'FooI' 的dummy实现并mock它
	type foo struct{FooI}
	Mock((*foo).Foo).Return("MOCKED!").Build()
	// mock 'FooI' 的构造函数返回dummy实现
	Mock(NewFoo).Return(new(foo)).Build()

	fmt.Println(NewFoo().Foo("anything"))
}
```

方法三：我们正在考虑实现一个 interface mock 功能，详见[#3](https://github.com/bytedance/mockey/issues/3)

### 如何 mock 依赖包 init() 里的函数？
经常我们会遇到这个问题：依赖包中存在 init 函数，init 函数在本地或者 CI 环境执行会 panic，导致单元测试会直接失败。这时候我们会希望将 panic 的函数 mock 掉，但是由于 init 函数的执行会早于单元测试，故一般的方法无法成功。

假设，a 包里引用了 b 包，b 的 init 里运行了 c 包的函数 FunC，然后我们希望在 a 包的单元测试开始前把 FunC mock 掉。由于 golang 的 init 顺序是字典序，我们只需要在 c 包 init 之前将带有 mock 函数的 d 包 init 即可，这里提供一个方案：

1. 新建一个包 d，然后在这个包创建 init 函数，在 init 函数里对 FunC 进行 mock；特别注意，这里需要对 CI 环境进行判断（比如 `os.Getenv("ENV") == "CI"`），否则生产环境也会被 mock
2. 在 a 包「字典序第一个 go 文件」里「额外引用」 d 包，并使得 d 包的引用在所有引用的最前面
3. 运行单元测试时注入 `ENV == "CI"`，使得 mock 生效

## 故障排除
### 错误 "function is too short to patch"？
1. 未禁用内联或编译优化。请检查是否已打印此日志并参考[FAQ](#如何禁用内联和编译优化)。
    ```
    Mockey check failed, please add -gcflags="all=-N -l".
    ```
2. 函数确实太短导致编译后的机器码不够长。通常两行或更多行的函数不会有此问题。如果有这样的需求，可以使用 `MockUnsafe` 来 mock 它，但可能会导致未知问题。
3. 函数已被其他工具 mock 过。检查下是否使用了如 [monkey](https://github.com/bouk/monkey) / [gomonkey](https://github.com/agiledragon/gomonkey) 等 mock 过该函数。

### 错误 "invalid memory address or nil pointer dereference"？
大概率是业务代码或单测代码的问题，建议 debug 单步调试或检查是否有未初始化的对象，一般常见于以下情况：
```go
type Loader interface{ Foo() }
var loader Loader
loader.Foo() // invalid memory address or nil pointer dereference
```

### 错误 "re-mock <xxx>, previous mock at: xxx"
函数在最小单元中被重复 mock，如下所示：
```go
package main

import (
	"fmt"

	. "github.com/bytedance/mockey"
)

func Foo() string { return "" }

func main() {
	Mock(Foo).Return("MOCKED!").Build() // 第一次 mock
	Mock(Foo).Return("MOCKED2!").Build() // 第二次 mock，会 panic!
	fmt.Println(Foo())
}
```
对于一个函数而言，在一个 PatchConvey 中（没有也是一样）只能 mock 一次，请参考 [PatchConvey](#支持patchconvey和patchrun) 说明小节来组织您的测试用例。如果确实有多次 mock 的需求，请获取 Mocker 来重新 mock，参考[获取 Mocker](#获取-mocker)

如果在 mock 不同类型实参的同一泛型函数时出现这个错误，则可能是不同实参的 gcshape 相同导致的，详见[泛型函数/方法](#泛型函数方法)小节。

### 错误 "args not match" / "Return Num of Func a does not match" / "Return value idx of rets can not convertible to"？
- 如果是使用了 `Return`，检查是否 return 参数和目标函数的返回值一致
- 如果是使用了 `To`，检查是否入参和出参和目标函数一致
- 如果使用了 `When`，检查是否入参和目标函数一致
- 如果 target 和 hook 报错中函数签名看起来完全一样，检查单测代码里和目标函数代码里的 import 包是否一致

### 崩溃 "signal SIGBUS: bus error"？
Mac M 系列电脑 (darwin/arm64) 有较大的概率碰到该问题，可以多次重试。在 v1.4.0 版本中，我们修复了该问题（一定程度上），相关讨论见[此处](https://github.com/bytedance/mockey/issues/68)。
```
fatal error: unexpected signal during runtime execution
[signal SIGBUS: bus error code=0x1 addr=0x10509aec0 pc=0x10509aec0]
```

### 崩溃 "signal SIGSEGV: segmentation violation"？
低版本 MacOS（10.x / 11.x）可能会有如下报错，目前可以采用 `go env -w CGO_ENABLED=0` 关闭 cgo 临时解决：
```
fatal error: unexpected signal during runtime execution
[signal SIGSEGV: segmentation violation code=0x1 addr=0xb01dfacedebac1e pc=0x7fff709a070a]
```

### 崩溃 "semacquire not on the G stack"？
最好不要直接 mock 系统函数，因为这可能会导致概率性崩溃问题，例如 mock time.Now 会导致：
```
fatal error: semacquire not on the G stack
```

### M 系列 Mac + Go 1.18 走入错误的分支？
该问题是 golang 在 arm64 架构下特定版本的 bug，请检查 golang 版本是否为 1.18.1～1.18.5，如果是升级 golang 版本即可。

Golang fix MR:
- [go/reflect: Incorrect behavior on arm64 when using MakeFunc / Call [1.18 backport] · Issue #53397](https://github.com/golang/go/issues/53397)
- https://go-review.googlesource.com/c/go/+/405114/2/src/cmd/compile/internal/ssa/rewriteARM64.go#28709

### 错误 "mappedReady and other memstats are not equal" / "index out of range" / "invalid reference to runtime.sysAlloc"？
版本太老，请升级到 mockey 最新版。

## 许可证
Mockey 根据 [Apache License](https://github.com/bytedance/mockey/blob/main/LICENSE-APACHE) 2.0 版本分发。Mockey 的第三方依赖项的许可证在此处[说明](https://github.com/bytedance/mockey/blob/main/licenses)。