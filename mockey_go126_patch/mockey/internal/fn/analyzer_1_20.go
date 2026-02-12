//go:build go1.20 && !go1.27
// +build go1.20,!go1.27

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
	"reflect"

	"github.com/bytedance/mockey/internal/tool"
)

func NewAnalyzer(target interface{}, generic *bool, method *bool) Analyzer {
	a := &AnalyzerImpl{
		target:    target,
		genericIn: generic,
		methodIn:  method,
	}
	return a.init()
}

type AnalyzerImpl struct {
	target    interface{}
	genericIn *bool
	methodIn  *bool

	targetValue        reflect.Value
	targetType         reflect.Type
	nameAnalyzer       *NameAnalyzer
	generic            bool
	method             bool // DO NOT use it unless absolutely necessary
	runtimeTargetType  reflect.Type
	runtimeTargetValue reflect.Value
	runtimeGenericInfo GenericInfo
}

// init initializes the AnalyzerImpl. If `a.genericIn` or `a.methodIn` is set, it will be used directly, else it will be
// determined by the NameAnalyzer.
func (a *AnalyzerImpl) init() *AnalyzerImpl {
	tool.DebugPrintf("[Analyzer.init] start analyze, genericIn: %v, methodIn: %v\n", a.genericIn, a.methodIn)
	a.targetValue, a.targetType = reflect.ValueOf(a.target), reflect.TypeOf(a.target)
	tool.DebugPrintf("[Analyzer.init] targetType: %v, targetValue: 0x%x\n", a.targetType, a.targetValue.Pointer())
	a.generic, a.method = a.isGeneric0(), a.isMethod0()
	a.runtimeTargetType = a.runtimeTargetType0()
	a.runtimeTargetValue, a.runtimeGenericInfo = a.runtimeTargetValueAndGenericInfo0()
	tool.DebugPrintf("[Analyzer.init] analyze finish, generic: %v, method: %v, runtimeTargetType: %v, runtimeTargetValue: 0x%x, runtimeGenericInfo: 0x%x\n", a.generic, a.method, a.runtimeTargetType, a.runtimeTargetValue.Pointer(), a.runtimeGenericInfo)
	return a
}

func (a *AnalyzerImpl) isGeneric0() bool {
	if a.genericIn != nil {
		return *a.genericIn
	}
	if a.nameAnalyzer == nil {
		a.nameAnalyzer = NewNameAnalyzerByValue(a.targetValue)
	}
	return a.nameAnalyzer.IsGeneric()
}

func (a *AnalyzerImpl) isMethod0() bool {
	if a.methodIn != nil {
		return *a.methodIn
	}
	if a.nameAnalyzer == nil {
		a.nameAnalyzer = NewNameAnalyzerByValue(a.targetValue)
	}

	// Analyze whether the function is a method.
	// NOTE: For methods named 'func\d+', misjudgment may occur, and they will not be regarded as methods.
	// ```
	// type foo struct {}
	// func (f *foo) func1() {}
	// ```
	// The fullname of 'f.func1' is 'main.foo.func1' which is regarded as an anonymous function in function 'main.foo'.

	// A function without an argument or middleName is definitely not a method.
	if a.targetType.NumIn() == 0 || !a.nameAnalyzer.HasMiddleName() {
		return false
	}

	// If the receiver is a pointer type, it must be a method
	if a.nameAnalyzer.IsPtrReceiver() {
		return true
	}

	// For exported functions, it is sufficient to iterate through all methods of the receiver and look for a method
	// with the same type and pointer.
	if a.nameAnalyzer.IsExported() {
		recvType := a.targetType.In(0)
		for i := 0; i < recvType.NumMethod(); i++ {
			method := recvType.Method(i)
			if method.Type == a.targetType && method.Func.Pointer() == a.targetValue.Pointer() {
				return true
			}
		}
		return false
	}

	// For unexported functions, it is highly challenging to determine whether they are methods. We can rule out some
	// cases and the rest are regarded as methods.
	return !a.nameAnalyzer.IsGlobal() && !a.nameAnalyzer.IsAnonymousFormat()
}

func (a *AnalyzerImpl) runtimeTargetType0() reflect.Type {
	if !a.generic {
		return a.targetType
	}
	var (
		targetIn      []reflect.Type
		targetInShift int
		targetOut     []reflect.Type
	)
	if a.method {
		// for methods, generic information needs to be inserted at position 1 after go1.20
		targetIn = []reflect.Type{a.targetType.In(0), genericInfoType}
		targetInShift = 1
	} else {
		// for functions, generic information needs to be inserted at position 0
		targetIn = []reflect.Type{genericInfoType}
		targetInShift = 0
	}
	for i := targetInShift; i < a.targetType.NumIn(); i++ {
		targetIn = append(targetIn, a.targetType.In(i))
	}
	for i := 0; i < a.targetType.NumOut(); i++ {
		targetOut = append(targetOut, a.targetType.Out(i))
	}
	return reflect.FuncOf(targetIn, targetOut, a.targetType.IsVariadic())
}

func (a *AnalyzerImpl) InputAdapter(inputName string, inputType reflect.Type) func([]reflect.Value) []reflect.Value {
	tool.Assert(inputType.Kind() == reflect.Func, "'%v' is not a function", inputType.Kind())
	targetType := a.RuntimeTargetType()
	tool.Assert(targetType.IsVariadic() == inputType.IsVariadic(), "args not match: target: %v, %s: %v", a.TargetType(), inputName, inputType)

	// check:
	// 1. function:
	//     a. non-generic function: func(inArgs) outArgs
	//     b. generic function: func(info GenericInfo, inArgs) outArgs
	// 2. method:
	//     a. non-generic method: func(self *struct, inArgs) outArgs
	if tool.CheckFuncArgs(targetType, inputType, 0, 0) {
		return func(targetArgs []reflect.Value) []reflect.Value { return targetArgs }
	}
	// need to adapt the arguments.
	if a.IsGeneric() {
		f, _ := a.genericAnalyzer(inputName, inputType)
		return f
	}
	f, _ := a.nonGenericAnalyzer(inputName, inputType)
	return f
}

func (a *AnalyzerImpl) ReversedInputAdapter(inputName string, inputType reflect.Type) func(inputArgs, extraArgs []reflect.Value) []reflect.Value {
	tool.Assert(inputType.Kind() == reflect.Func, "'%v' is not a function", inputType.Kind())
	targetType := a.RuntimeTargetType()
	tool.Assert(targetType.IsVariadic() == inputType.IsVariadic(), "args not match: target: %v, %s: %v", a.TargetType(), inputName, inputType)

	// check:
	// 1. function:
	//     a. non-generic function: func(inArgs) outArgs
	//     b. generic function: func(info GenericInfo, inArgs) outArgs
	// 2. method:
	//     a. non-generic method: func(self *struct, inArgs) outArgs
	if tool.CheckFuncArgs(targetType, inputType, 0, 0) {
		return func(inputArgs, extraArgs []reflect.Value) []reflect.Value { return inputArgs }
	}
	// need to adapt the arguments.
	if a.IsGeneric() {
		_, rf := a.genericAnalyzer(inputName, inputType)
		return rf
	}
	_, rf := a.nonGenericAnalyzer(inputName, inputType)
	return rf
}

type (
	fn         = func(targetArgs []reflect.Value) []reflect.Value
	reversedFn = func(inputArgs, extraArgs []reflect.Value) []reflect.Value
)

func (a *AnalyzerImpl) nonGenericAnalyzer(inputName string, inputType reflect.Type) (fn, reversedFn) {
	// check:
	// 1. method:
	//     a. non-generic method: func(inArgs) outArgs
	res := tool.CheckFuncArgs(a.TargetType(), inputType, 1, 0)
	tool.Assert(res, "args not match: target: %v, %s: %v", a.TargetType(), inputName, inputType)
	f := func(targetArgs []reflect.Value) []reflect.Value {
		return targetArgs[1:]
	}
	rf := func(inputArgs, extraArgs []reflect.Value) []reflect.Value {
		return append([]reflect.Value{extraArgs[0]}, inputArgs...)
	}
	return f, rf
}

func (a *AnalyzerImpl) genericAnalyzer(inputName string, inputType reflect.Type) (fn, reversedFn) {
	targetType := a.RuntimeTargetType()

	// check function:
	// 		a. generic function: func(inArgs) outArgs
	if !a.method {
		res := tool.CheckFuncArgs(targetType, inputType, 1, 0)
		tool.Assert(res, "args not match: target: %v, %s: %v", a.TargetType(), inputName, inputType)
		f := func(targetArgs []reflect.Value) []reflect.Value {
			return targetArgs[1:]
		}
		rf := func(inputArgs, extraArgs []reflect.Value) []reflect.Value {
			return append([]reflect.Value{extraArgs[0]}, inputArgs...)
		}
		return f, rf
	}

	// check method:
	// 		a. generic method: func(GenericInfo, self *struct, inArgs)
	if inputType.NumIn() >= 2 && inputType.In(0) == genericInfoType {
		tool.Assert(inputType.In(1) == targetType.In(0), "args not match: target: %v, %s: %v", a.TargetType(), inputName, inputType)
		res := tool.CheckFuncArgs(targetType, inputType, 2, 2)
		tool.Assert(res, "args not match: target: %v, %s: %v", a.TargetType(), inputName, inputType)
		f := func(targetArgs []reflect.Value) []reflect.Value {
			return append([]reflect.Value{targetArgs[1], targetArgs[0]}, targetArgs[2:]...)
		}
		rf := func(inputArgs, extraArgs []reflect.Value) []reflect.Value {
			return append([]reflect.Value{inputArgs[1], inputArgs[0]}, inputArgs[2:]...)
		}
		return f, rf
	}

	// check method:
	// 		a. generic method: func(self *struct, inArgs)
	if inputType.NumIn() >= 1 && inputType.In(0) == targetType.In(0) {
		res := tool.CheckFuncArgs(targetType, inputType, 2, 1)
		tool.Assert(res, "args not match: target: %v, %s: %v", a.TargetType(), inputName, inputType)
		f := func(targetArgs []reflect.Value) []reflect.Value {
			return append([]reflect.Value{targetArgs[0]}, targetArgs[2:]...)
		}
		rf := func(inputArgs, extraArgs []reflect.Value) []reflect.Value {
			return append([]reflect.Value{inputArgs[0], extraArgs[1]}, inputArgs[1:]...)
		}
		return f, rf
	}

	// check method:
	// 		a. generic method: func(inArgs)
	res := tool.CheckFuncArgs(targetType, inputType, 2, 0)
	tool.Assert(res, "args not match: target: %v, %s: %v", a.TargetType(), inputName, inputType)
	f := func(targetArgs []reflect.Value) []reflect.Value {
		return targetArgs[2:]
	}
	rf := func(inputArgs, extraArgs []reflect.Value) []reflect.Value {
		return append([]reflect.Value{extraArgs[0], extraArgs[1]}, inputArgs...)
	}
	return f, rf
}
