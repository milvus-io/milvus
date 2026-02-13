# Mockey
English | [中文](README_cn.md)

[![Release](https://img.shields.io/github/v/release/bytedance/mockey)](https://github.com/bytedance/mockey/releases)
[![License](https://img.shields.io/github/license/bytedance/mockey)](https://github.com/bytedance/mockey/blob/main/LICENSE-APACHE)
[![Go Report Card](https://goreportcard.com/badge/github.com/bytedance/mockey)](https://goreportcard.com/report/github.com/bytedance/mockey)
[![codecov](https://codecov.io/github/bytedance/mockey/graph/badge.svg?token=JKL3WSE3I3)](https://codecov.io/github/bytedance/mockey)
[![OpenIssue](https://img.shields.io/github/issues/bytedance/mockey)](https://github.com/bytedance/mockey/issues)
[![ClosedIssue](https://img.shields.io/github/issues-closed/bytedance/mockey)](https://github.com/bytedance/mockey/issues?q=is%3Aissue+is%3Aclosed)

Mockey is a simple and easy-to-use golang mock library, which can quickly and conveniently mock functions and variables. At present, it is widely used in the unit test writing of ByteDance services (7k+ repos) and is actively maintained. In essence, it rewrites function instructions at runtime similarly to [monkey](https://github.com/bouk/monkey) or [gomonkey](https://github.com/agiledragon/gomonkey).

Mockey makes it easy to replace functions, methods and variables with mocks reducing the need to specify all dependencies as interfaces.
> 1. Mockey requires **inlining and compilation optimization to be disabled** during compilation, or it won't work. See the [FAQs](#how-to-disable-inline-and-compile-optimization) for details.
> 2. It is strongly recommended to use it together with the [goconvey](https://github.com/smartystreets/goconvey) library in unit tests.

## Install
```
go get github.com/bytedance/mockey@latest
```

## Quick Guide
### Simplest example
```go
package main

import (
	"fmt"
	"math/rand"

	. "github.com/bytedance/mockey"
)

func main() {
	Mock(rand.Int).Return(1).Build() // mock `rand.Int` to return 1
	
	fmt.Printf("rand.Int() always return: %v\n", rand.Int()) // Try if it's still random?
}
```

### Unit test example
```go
package main_test

import (
	"math/rand"
	"testing"

	. "github.com/bytedance/mockey"
	. "github.com/smartystreets/goconvey/convey"
)

// Win function to be tested, input a number, win if it's greater than random number, otherwise lose
func Win(in int) bool {
	return in > rand.Int()
}

func TestWin(t *testing.T) {
	PatchConvey("TestWin", t, func() {
		Mock(rand.Int).Return(100).Build() // mock
		
		res1 := Win(101)                   // execute
		So(res1, ShouldBeTrue)             // assert
		
		res2 := Win(99)         // execute
		So(res2, ShouldBeFalse) // assert
	})
}
```

## Features
- Mock functions and methods
    - Basic
        - Simple / generic / variadic function or method (value or pointer receiver)
        - Supporting hook function
        - Supporting `PatchConvey` and `PatchRun` (automatically release mocks after each test case)
        - Providing `GetMethod` to handle special cases (e.g., unexported types, unexported method, and methods in nested structs)
    - Advanced
        - Conditional mocking
        - Sequence returning
        - Decorator pattern (execute the original function after mocking)
        - Goroutine filtering (inclusion, exclusion, targeting)
        - Acquire `Mocker` for advanced usage (e.g., getting the execution times of target/mock function)
- Mock variable
    - Common variable
    - Function variable

## Compatibility
### OS Support
- Mac OS(Darwin)
- Linux
- Windows

### Arch Support 
- AMD64
- ARM64

### Version Support 
- Go 1.13+

## Basic Features
### Simple function/method
Use `Mock` to mock function/method, use `Return` to specify the return value, and use `Build` to make the mock effective:
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
	// mock function
	Mock(Foo).Return("MOCKED!").Build()
	fmt.Println(Foo("anything")) // MOCKED!

	// mock method (value receiver)
	Mock(A.Foo).Return("MOCKED!").Build()
	fmt.Println(A{}.Foo("anything")) // MOCKED!

	// mock method (pointer receiver)
	Mock((*B).Foo).Return("MOCKED!").Build()
	fmt.Println(new(B).Foo("anything")) // MOCKED!
    
    // Tips: if the target has no return value, you still need to call the empty `Return()` or use `To` to customize the hook function.
}
```

### Generic function/method
> Starting from v1.3.0, `Mock` experimentally adds the ability to automatically identify generics (for go1.20+), you can use `Mock` to directly replace `MockGeneric`

Use `MockGeneric` to mock generic function/method:
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
	// mock generic function 
	MockGeneric(FooGeneric[string]).Return("MOCKED!").Build() // `Mock(FooGeneric[string], OptGeneric)` also works
	fmt.Println(FooGeneric("anything"))                       // MOCKED!
	fmt.Println(FooGeneric(1))                                // 1 | Not working because of type mismatch!

	// mock generic method 
	MockGeneric((*GenericClass[string]).Foo).Return("MOCKED!").Build()
	fmt.Println(new(GenericClass[string]).Foo("anything")) // MOCKED!
}
```

Additionally, Golang generics share implementation for different types with the same underlying type. For example, in `type MyString string`, `MyString` and `string` share one implementation. Therefore, mocking one type will interfere with the other.
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
	fmt.Println(FooGeneric[MyString]("anything")) // MOCKED! | This is due to interference after mocking the string type
}
```

In v1.3.1, this issue was resolved. We now support mocking generic functions/methods with the same gcshape but different actual types. The example above will behave more as expected:
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
	fmt.Println(FooGeneric[MyString]("anything")) // anything | No longer interferes
	mocker2 := MockGeneric(FooGeneric[MyString]).Return("MOCKED2!").Build()
	fmt.Println(FooGeneric("anything"))           // MOCKED!
	fmt.Println(FooGeneric[MyString]("anything")) // MOCKED2! | No longer interferes

	// Note: If you need to manually release mockers, be sure to follow the "last-in-first-out" order, otherwise unexpected results such as crashes may occur
	mocker2.UnPatch()
	fmt.Println(FooGeneric("anything"))           // MOCKED!
	fmt.Println(FooGeneric[MyString]("anything")) // anything
	mocker1.UnPatch()
	fmt.Println(FooGeneric("anything"))           // anything
	fmt.Println(FooGeneric[MyString]("anything")) // anything
}
```

### Variadic function/method
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
	// mock variadic function
	Mock(FooVariadic).Return("MOCKED!").Build()
	fmt.Println(FooVariadic("anything")) // MOCKED!

	// mock variadic method
	Mock(A.FooVariadic).Return("MOCKED!").Build()
	fmt.Println(A{}.FooVariadic("anything")) // MOCKED!
}
```

### Supporting hook function
Use `To` to specify the hook function:
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
	// NOTE: hook function must have the same function signature as the original function!
	Mock(Foo).To(func(in string) string { return "MOCKED!" }).Build()
	fmt.Println(Foo("anything")) // MOCKED!

	// NOTE: for method mocking, the receiver can be added to the signature of the hook function on your need (if the receiver is not used, it can be omitted, and mockey is compatible).
	Mock(A.Foo).To(func(a A, in string) string { return a.prefix + ":inner:" + "MOCKED!" }).Build()
	fmt.Println(A{prefix: "prefix"}.Foo("anything")) // prefix:inner:MOCKED!
}
```

### Supporting `PatchConvey` and `PatchRun`
> Starting from v1.4.1, `PatchRun` is supported

`PatchConvey` and `PatchRun` are tools for managing mock lifecycles. They automatically release mocks after test cases or functions are executed, eliminating the need for `defer`. Both support nested usage, and each layer only releases its own internal mocks.

Applicable scenarios comparison:
- When you need to use the assertion features and test organization capabilities of the goconvey framework, it is recommended to use `PatchConvey`; the execution order of nested `PatchConvey` is the same as `Convey`, please refer to the goconvey related [documentation](https://github.com/smartystreets/goconvey/wiki/Execution-order)
- When you don't need goconvey integration, it is recommended to use the more lightweight `PatchRun`

`PatchConvey` example as follows:
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
			res := Foo("anything")                // invoke
			So(res, ShouldEqual, "MOCKED-1!")     // assert
		})

		// mock released
		PatchConvey("mock released", func() {
			res := Foo("anything")               // invoke
			So(res, ShouldEqual, "ori:anything") // assert
		})

		// mock again
		PatchConvey("mock 2", func() {
			Mock(Foo).Return("MOCKED-2!").Build() // mock
			res := Foo("anything")                // invoke
			So(res, ShouldEqual, "MOCKED-2!")     // assert
		})
	})

	// Tips: Like `Convey`, `PatchConvey` can be nested; each layer of `PatchConvey` will only release its own internal mocks
}

```

`PatchRun` example as follows:
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
		res := Foo("anything")                // call
		if res != "MOCKED-1!" {
			t.Errorf("expected 'MOCKED-1!', got '%s'", res)
		}
	})

	// mock released
	res := Foo("anything") // call
	if res != "ori:anything" {
		t.Errorf("expected 'ori:anything', got '%s'", res)
	}

	// mock again
	PatchRun(func() {
		Mock(Foo).Return("MOCKED-2!").Build() // mock
		res := Foo("anything")                // call
		if res != "MOCKED-2!" {
			t.Errorf("expected 'MOCKED-2!', got '%s'", res)
		}
	})

	// mock released
	res = Foo("anything") // call
	if res != "ori:anything" {
		t.Errorf("expected 'ori:anything', got '%s'", res)
	}
}
```

### Providing `GetMethod` to handle special cases
In special cases where direct mocking is not possible or not effective, you can use `GetMethod` to get the corresponding method before mocking. Please ensure that the passed object is not nil.

Mock method through an instance (including interface type instances):
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

	// Mock(a.Foo) won't work, because `a` is an instance of `A`, not the type `A`
	// Tips: if the instance is an interface type, you can use it the same way
	// var ia interface{ Foo(string) string } = new(A)
	// Mock(GetMethod(ia, "Foo")).Return("MOCKED!").Build()
	Mock(GetMethod(a, "Foo")).Return("MOCKED!").Build()
	fmt.Println(a.Foo("anything")) // MOCKED!
}
```

Mock method of unexported types:
```go
package main

import (
	"crypto/sha256"
	"fmt"

	. "github.com/bytedance/mockey"
)

func main() {
	// `sha256.New()` returns an unexported `*digest`, whose `Sum` method is the one we want to mock
	Mock(GetMethod(sha256.New(), "Sum")).Return([]byte{0}).Build()

	fmt.Println(sha256.New().Sum([]byte("anything"))) // [0]
	
	// Tips: this is a special case of "mocking methods through instances", where the type corresponding to the instance is unexported
}
```

Mock unexported method:
```go
package main

import (
	"bytes"
	"fmt"

	. "github.com/bytedance/mockey"
)

func main() {
	// `*bytes.Buffer` has an unexported `empty` method, which is the one we want to mock
	Mock(GetMethod(new(bytes.Buffer), "empty")).Return(true).Build()

	buf := bytes.NewBuffer([]byte{1, 2, 3, 4})
	b, err := buf.ReadByte()
	fmt.Println(b, err)      // 0 EOF | `ReadByte` calls `empty` method inside to check if the buffer is empty and return io.EOF
}
```
The Go compiler may directly erase the type information of unexported methods, in which case `GetMethod` will fail to retrieve the method. In such cases, you can use `OptUnexportedTargetType` to explicitly specify its type:
```go
package main

import (
	"bytes"
	"fmt"

	. "github.com/bytedance/mockey"
)

func main() {
	var targetType func() bool // Signature of the unexported `empty` method (excluding receiver)
	target := GetMethod(new(bytes.Buffer), "empty", OptUnexportedTargetType(targetType))
	Mock(target).Return(true).Build()

	buf := bytes.NewBuffer([]byte{1, 2, 3, 4})
	b, err := buf.ReadByte()
	fmt.Println(b, err) // 0 EOF | `ReadByte` internally calls the `empty` method to check if the buffer is empty and returns io.EOF
}
```

Mock methods in nested structs:
```go
package main

import (
	"fmt"

	. "github.com/bytedance/mockey"
)

type Wrapper struct {
	inner // nested struct
}

type inner struct{}

func (i inner) Foo(in string) string {
	return in
}

func main() {
	// Mock(Wrapper.Foo) won't work, because the target should be `inner.Foo`
	Mock(GetMethod(Wrapper{}, "Foo")).Return("MOCKED!").Build() // or Mock(inner.Foo).Return("MOCKED!").Build()

	fmt.Println(Wrapper{}.Foo("anything")) // MOCKED! 
}
```

## Advanced features
### Conditional mocking
Use `When` to define multiple conditions:
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
	// NOTE: condition function must have the same input signature as the original function!
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

### Sequence returning
Use `Sequence` to mock multiple return values:
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

### Decorator pattern
Use `Origin` to keep the original logic of the target after mock:
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
	// `origin` will carry the logic of the `Foo` function
	origin := Foo

	// `decorator` will do some AOP around `origin`
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

### Goroutine filtering
By default, mocks take effect in all goroutines. You can use the following APIs to specify in which goroutines the mock takes effect:
- `IncludeCurrentGoRoutine`: Only takes effect in the current goroutine
- `ExcludeCurrentGoRoutine`: Takes effect in all goroutines except the current one
- `FilterGoRoutine`: Include or exclude specified goroutines (by goroutine id)
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
	// use `ExcludeCurrentGoRoutine` to exclude current goroutine
	Mock(Foo).ExcludeCurrentGoRoutine().Return("MOCKED!").Build()
	fmt.Println(Foo("anything")) // anything | mock won't work in current goroutine

	go func() {
		fmt.Println(Foo("anything")) // MOCKED! | mock works in other goroutines
	}()

	time.Sleep(time.Second) // wait for goroutine to finish
	
	// Tips: You can use `GetGoroutineId` to get the current goroutine ID
}
```

### Acquire `Mocker`
Acquire `Mocker` to use advanced features:
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

	// use `MockTimes` and `Times` to track the number of times mock worked and `Foo` is called
	fmt.Println(mocker.MockTimes()) // 1
	fmt.Println(mocker.Times())     // 2

	// Tips: When remocking or releasing mock, the related counters will be reset to 0.

	// remock `Foo` to return "MOCKED2!"
	mocker.Return("MOCKED2!")
	fmt.Println(Foo("anything")) // MOCKED2!
	fmt.Println(mocker.MockTimes()) // 1 | Reset to 0 when remocking

	// release `Foo` mock
	mocker.Release()
	fmt.Println(Foo("anything")) // anything | mock won't work, because mock released
	fmt.Println(mocker.MockTimes()) // 0 | Reset to 0 when releasing
}
```

## FAQ
### How to disable inline and compile optimization?
1. Command line：`go test -gcflags="all=-l -N" -v ./...` in tests or `go build -gcflags="all=-l -N"` in main packages.
2. Goland：use Debug or fill `-gcflags="all=-l -N"` in the **Run/Debug Configurations > Go tool arguments** dialog box.
3. VSCode: use Debug or add `"go.buildFlags": ["-gcflags=\'all=-N -l\'"]` in `settings.json`.

### Mock doesn't work?
1. Inline or compilation optimizations are not disabled. Please check if this log has been printed and refer to [relevant section](#how-to-disable-inline-and-compile-optimization) of FAQ.
    ```
    Mockey check failed, please add -gcflags="all=-N -l".
    ```
2. Check if `Build()` was not called, or that neither `Return(xxx)` nor `To(xxx)` was called, resulting in no actual effect. If the target function has no return value, you still need to call the empty `Return()` or use `To` to customize the hook function.
3. Mock targets do not match exactly, as below:
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
    	fmt.Println(A{}.Foo("anything")) // won't work, because the mock target should be `A.Foo`
    
    	a := A{}
    	Mock(a.Foo).Return("MOCKED!").Build()
    	fmt.Println(a.Foo("anything")) // won't work, because the mock target should be `A.Foo`
    }
    ```
   Please refer to [Generic function/method](#generic-functionmethod) if the target is generic. Otherwise, try to check if it hits specific cases in [GetMethod](#providing-getmethod-to-handle-special-cases).
4. The target function is executed in other goroutines when mock released:
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
    		go func() { fmt.Println(Foo("anything")) }() // the timing of executing 'Foo' is uncertain
    	})
    	// when the main goroutine comes here, the relevant mock has been released by 'PatchConvey'. If 'Foo' is executed before this, the mock succeeds, otherwise it fails
    	fmt.Println("over")
    	time.Sleep(time.Second)
    }
    ```
5. The function call happens before mock execution. Try setting a breakpoint at the first line of the original function. If the execution reaches the first line when the stack is not after the mock code in the unit test, this is the issue. Common in `init()` functions. See [relevant section](#how-to-mock-functions-in-dependency-package-init) for how to mock functions in `init()`.
6. Using non-generic mock for generic functions. See [Generic function/method](#generic-functionmethod) section for details.
7. Occasional mock failures or inability to restore mocks under the arm64 architecture. Please upgrade to the latest version, see [#90](https://github.com/bytedance/mockey/issues/90).

### How to mock interface types?
Method 1: Use `GetMethod` to get the corresponding method from the instance
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

// foo the original implementation of 'FooI'
type foo struct{}

func (f *foo) Foo(in string) string {
   return in
}

func main() {
   // get the original implementation and mock it
   instance := NewFoo()
   Mock(GetMethod(instance, "Foo")).Return("MOCKED!").Build()

   fmt.Println(instance.Foo("anything")) // MOCKED!
}
```

Method 2: Create a dummy implementation type and mock the corresponding constructor to return that type
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

// foo the original implementation of 'FooI'
type foo struct{}

func (f *foo) Foo(in string) string {
   return in
}

func main() {
   // generate a dummy implementation of 'FooI' and mock it
   type foo struct{ FooI }
   Mock((*foo).Foo).Return("MOCKED!").Build()
   // mock the constructor of 'FooI' to return the dummy implementation
   Mock(NewFoo).Return(new(foo)).Build()

   fmt.Println(NewFoo().Foo("anything")) // MOCKED!
}
```
Method 3: We are working on a interface mocking feature, see [#3](https://github.com/bytedance/mockey/issues/3)

### How to mock functions in dependency package init()?
We often encounter this problem: there is an init function in the dependency package that panics when executed in local or CI environment, causing unit tests to fail directly. We hope to mock the panicking function, but since init functions execute before unit tests, general methods cannot succeed.

Suppose package a references package b, and package b's init runs a function FunC from package c. We hope to mock FunC before package a's unit test starts. Since golang's init order is dictionary order, we just need to initialize a package d with mock functions before package c's init. Here's a solution:

1. Create a new package d, then create an init function in this package to mock FunC; Special attention: you need to check for CI environment (such as `os.Getenv("ENV") == "CI"`), otherwise the production environment will also be mocked
2. In package a's "first go file in dictionary order", "additionally reference" package d, and make package d's reference at the front of all imports
3. Inject `ENV == "CI"` when running unit tests to make the mock effective

## Troubleshooting
### Error "function is too short to patch"？
1. Inline or compilation optimizations are not disabled. Please check if this log has been printed and refer to [relevant section](#how-to-disable-inline-and-compile-optimization) of FAQ.
    ```
    Mockey check failed, please add -gcflags="all=-N -l".
    ```
2. The function is really too short resulting in the compiled machine code is not long enough. Generally, two or more lines will not cause this problem. If there is such a need, you may use `MockUnsafe` to mock it causing unknown issues.
3. The function has been mocked by other tools (such as [monkey](https://github.com/bouk/monkey) or [gomonkey](https://github.com/agiledragon/gomonkey) etc.)

### Error "invalid memory address or nil pointer dereference"？
This is most likely an issue with your business code or test code. It is recommended to debug step by step or check for uninitialized objects. It is generally common in the following situations:
```go
type Loader interface{ Foo() }
var loader Loader
loader.Foo() // invalid memory address or nil pointer dereference
```

### Error "re-mock <xxx>, previous mock at: xxx"
The function has been mocked repeatedly in the smallest unit, as below:
```go
package main

import (
	"fmt"

	. "github.com/bytedance/mockey"
)

func Foo() string { return "" }

func main() {
	Mock(Foo).Return("MOCKED!").Build() // mock for the first time
	Mock(Foo).Return("MOCKED2!").Build() // mock for the second time, will panic!
	fmt.Println(Foo())
}
```
For a function, it can only be mocked once in a PatchConvey (even without PatchConvey). Please refer to [PatchConvey](#supporting-patchconvey-and-patchrun) to organize your test cases. If there is such a need, please refer to [Acquire Mocker](#acquire-mocker) to remock.

If you encounter this error when mocking the same generic function with different type arguments, it may be caused by the fact that the gcshape of different arguments is the same. For details, see the [Generic function/method](#generic-functionmethod) section.

### Error "args not match" / "Return Num of Func a does not match" / "Return value idx of rets can not convertible to"?
- If using `Return`, check if the return parameters are consistent with the target function's return values
- If using `To`, check if the input and output parameters are consistent with the target function
- If using `When`, check if the input parameters are consistent with the target function
- If the target and hook function signatures appear identical in the error, check if the import packages in the test code and target function code are consistent

### Crash "signal SIGBUS: bus error"?
Mac M series computers (darwin/arm64) have a higher probability of encountering this issue. You can retry multiple times. In v1.4.0, we fixed this issue (to some extent). Related discussion [here](https://github.com/bytedance/mockey/issues/68).
```
fatal error: unexpected signal during runtime execution
[signal SIGBUS: bus error code=0x1 addr=0x10509aec0 pc=0x10509aec0]
```

### Crash "signal SIGSEGV: segmentation violation"?
Lower version MacOS (10.x / 11.x) may have the following error. Currently, you can temporarily resolve it by disabling cgo with `go env -w CGO_ENABLED=0`:
```
fatal error: unexpected signal during runtime execution
[signal SIGSEGV: segmentation violation code=0x1 addr=0xb01dfacedebac1e pc=0x7fff709a070a]
```

### Crash "semacquire not on the G stack"?
It is best not to directly mock system functions as it may cause probabilistic crash issues. For example, mocking `time.Now` will cause:
```
fatal error: semacquire not on the G stack
```

### M series Mac + Go 1.18 goes to wrong branch?
This is a bug in golang under arm64 architecture for specific versions. Please check if the golang version is 1.18.1～1.18.5. If so, upgrade the golang version.

Golang fix MR:
- [go/reflect: Incorrect behavior on arm64 when using MakeFunc / Call [1.18 backport] · Issue #53397](https://github.com/golang/go/issues/53397)
- https://go-review.googlesource.com/c/go/+/405114/2/src/cmd/compile/internal/ssa/rewriteARM64.go#28709

### Error "mappedReady and other memstats are not equal" / "index out of range" / "invalid reference to runtime.sysAlloc"？
The version is too old, please upgrade to the latest version of mockey.

## License
Mockey is distributed under the [Apache License](https://github.com/bytedance/mockey/blob/main/LICENSE-APACHE), version 2.0. The licenses of third party dependencies of Mockey are explained [here](https://github.com/bytedance/mockey/blob/main/licenses).
