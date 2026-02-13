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
	"reflect"
	"sync"
	"sync/atomic"

	"github.com/bytedance/mockey/internal/fn"
	"github.com/bytedance/mockey/internal/monkey"
	"github.com/bytedance/mockey/internal/tool"
)

type FilterGoroutineType int64

const (
	Disable FilterGoroutineType = 0
	Include FilterGoroutineType = 1
	Exclude FilterGoroutineType = 2
)

type Mocker struct {
	target    reflect.Value // mock target value
	hook      reflect.Value // mock hook value
	proxy     reflect.Value // proxy pointer value
	times     int64
	mockTimes int64
	patch     *monkey.Patch
	lock      sync.Mutex
	isPatched bool
	builder   *MockBuilder

	outerCaller tool.CallerInfo
}

type MockBuilder struct {
	target          interface{}      // mock target
	originPtr       interface{}      // origin caller
	conditions      []*mockCondition // mock conditions
	filterGoroutine FilterGoroutineType
	gId             int64
	unsafe          bool
	analyzer        fn.Analyzer
}

// Mock mocks target function.
// From go1.20, Mock can automatically judge whether the target is generic or not. Before go1.20, you need to use
// MockGeneric to mock generic function.
func Mock(target interface{}, opt ...mockOptionFn) *MockBuilder {
	tool.AssertFunc(target)

	opts := resolveMockOpt(opt...)

	builder := &MockBuilder{
		target:   target,
		unsafe:   opts.unsafe,
		analyzer: fn.NewAnalyzer(target, opts.generic, opts.method),
	}
	builder.resetCondition()
	return builder
}

// MockUnsafe has the full ability of the Mock function and removes some security restrictions. This is an alternative
// when the Mock function fails. It may cause some unknown problems, so we recommend using Mock under normal conditions.
func MockUnsafe(target interface{}, opt ...mockOptionFn) *MockBuilder {
	return Mock(target, append(opt, OptUnsafe)...)
}

// runtimeTargetType returns the type of the target function with generic type info if it's generic.
func (builder *MockBuilder) runtimeTargetType() reflect.Type {
	return builder.analyzer.RuntimeTargetType()
}

func (builder *MockBuilder) resetCondition() *MockBuilder {
	builder.conditions = []*mockCondition{builder.newCondition()} // at least 1 condition is needed
	return builder
}

// Origin add an origin hook which can be used to call un-mocked origin function
//
// For example:
//
//	 origin := Fun // only need the same type
//	 mock := func(p string) string {
//		 return origin(p + "mocked")
//	 }
//	 mock2 := Mock(Fun).To(mock).Origin(&origin).Build()
//
// Origin only works when call origin hook directly, target will still be mocked in recursive call
func (builder *MockBuilder) Origin(funcPtr interface{}) *MockBuilder {
	tool.Assert(builder.originPtr == nil, "re-set builder origin")
	return builder.origin(funcPtr)
}

func (builder *MockBuilder) origin(funcPtr interface{}) *MockBuilder {
	tool.AssertPtr(funcPtr)
	builder.originPtr = funcPtr
	return builder
}

func (builder *MockBuilder) lastCondition() *mockCondition {
	cond := builder.conditions[len(builder.conditions)-1]
	if cond.Complete() {
		cond = builder.newCondition()
		builder.conditions = append(builder.conditions, cond)
	}
	return cond
}

func (builder *MockBuilder) newCondition() *mockCondition {
	return &mockCondition{builder: builder}
}

// When declares the condition hook that's called to determine whether the mock should be executed.
//
// The condition hook function must have the same parameters as the target function.
//
// The following example would execute the mock when input int is negative
//
//	func Fun(input int) string {
//		return strconv.Itoa(input)
//	}
//	Mock(Fun).When(func(input int) bool { return input < 0 }).Return("0").Build()
//
// Note that if the target function is a struct method, you may optionally include
// the receiver as the first argument of the condition hook function. For example,
//
//	type Foo struct {
//		Age int
//	}
//	func (f *Foo) GetAge(younger int) string {
//		return strconv.Itoa(f.Age - younger)
//	}
//	Mock((*Foo).GetAge).When(func(f *Foo, younger int) bool { return younger < 0 }).Return("0").Build()
func (builder *MockBuilder) When(when interface{}) *MockBuilder {
	builder.lastCondition().SetWhen(when)
	return builder
}

// To declares the hook function that's called to replace the target function.
//
// The hook function must have the same signature as the target function.
//
// The following example would make Fun always return true
//
//	func Fun(input string) bool {
//		return input == "fun"
//	}
//
//	Mock(Fun).To(func(_ string) bool {return true}).Build()
//
// Note that if the target function is a struct method, you may optionally include
// the receiver as the first argument of the hook function. For example,
//
//	type Foo struct {
//		Name string
//	}
//	func (f *Foo) Bar(other string) bool {
//		return other == f.Name
//	}
//	Mock((*Foo).Bar).To(func(f *Foo, other string) bool {return true}).Build()
func (builder *MockBuilder) To(hook interface{}) *MockBuilder {
	builder.lastCondition().SetTo(hook)
	return builder
}

func (builder *MockBuilder) Return(results ...interface{}) *MockBuilder {
	builder.lastCondition().SetReturn(results...)
	return builder
}

func (builder *MockBuilder) IncludeCurrentGoRoutine() *MockBuilder {
	return builder.FilterGoRoutine(Include, tool.GetGoroutineID())
}

func (builder *MockBuilder) ExcludeCurrentGoRoutine() *MockBuilder {
	return builder.FilterGoRoutine(Exclude, tool.GetGoroutineID())
}

func (builder *MockBuilder) FilterGoRoutine(filter FilterGoroutineType, gId int64) *MockBuilder {
	builder.filterGoroutine = filter
	builder.gId = gId
	return builder
}

func (builder *MockBuilder) Build() *Mocker {
	mocker := Mocker{builder: builder}
	mocker.build()
	mocker.Patch()
	return &mocker
}

func (mocker *Mocker) build() {
	mocker.target = reflect.ValueOf(mocker.builder.target)

	var (
		extraArgsGetter func() []reflect.Value
		originExec      func(args []reflect.Value) []reflect.Value
		match           []func(args []reflect.Value) bool
		exec            []func(args []reflect.Value) []reflect.Value
	)

	mocker.proxy = reflect.New(mocker.builder.runtimeTargetType())

	originExec = func(args []reflect.Value) []reflect.Value {
		return tool.ReflectCall(mocker.proxy.Elem(), args)
	}

	if originPtr := mocker.builder.originPtr; originPtr != nil {
		origin := reflect.ValueOf(originPtr).Elem()
		originType := reflect.TypeOf(originPtr).Elem()
		adapter := mocker.builder.analyzer.ReversedInputAdapter("origin", originType)
		origin.Set(reflect.MakeFunc(originType, func(args []reflect.Value) []reflect.Value {
			return tool.ReflectCall(mocker.proxy.Elem(), adapter(args, extraArgsGetter()))
		}))
	}

	for i := range mocker.builder.conditions {
		condition := mocker.builder.conditions[i]
		if condition.when == nil {
			// when condition is not set, just go into hook exec
			match = append(match, func(args []reflect.Value) bool { return true })
		} else {
			match = append(match, func(args []reflect.Value) bool {
				return tool.ReflectCall(reflect.ValueOf(condition.when), args)[0].Bool()
			})
		}

		if condition.hook == nil {
			// hook condition is not set, just go into original exec
			exec = append(exec, originExec)
		} else {
			exec = append(exec, func(args []reflect.Value) []reflect.Value {
				mocker.mock()
				return tool.ReflectCall(reflect.ValueOf(condition.hook), args)
			})
		}
	}

	mockerHook := reflect.MakeFunc(mocker.builder.runtimeTargetType(), func(args []reflect.Value) []reflect.Value {
		if mocker.builder.originPtr != nil {
			// Origin call need extra args, which only can be obtained during the execution of mockerHook.
			extraArgsGetter = func() []reflect.Value { return args }
		}

		// Check if the currently called generic function instance matches the mocked generic function
		if analyzer := mocker.builder.analyzer; analyzer.IsGeneric() {
			genericInfoHook := tool.NewFuncTypeByInsertIn(analyzer.TargetType(), reflect.TypeOf(GenericInfo(0)))
			genericInfoAdapter := analyzer.InputAdapter("getGenericInfo", genericInfoHook)
			genericInfo, targetGenericInfo := genericInfoAdapter(args)[0].Interface().(GenericInfo), analyzer.GenericInfo()
			if genericInfo != targetGenericInfo {
				tool.DebugPrintf("genericInfo mismatch: genericInfo: 0x%x, targetGenericInfo: 0x%x\n", genericInfo, targetGenericInfo)
				return originExec(args)
			}
		}

		mocker.access()
		switch mocker.builder.filterGoroutine {
		case Disable:
			break
		case Include:
			if tool.GetGoroutineID() != mocker.builder.gId {
				return originExec(args)
			}
		case Exclude:
			if tool.GetGoroutineID() == mocker.builder.gId {
				return originExec(args)
			}
		}

		for i, matchFn := range match {
			execFn := exec[i]
			if matchFn(args) {
				return execFn(args)
			}
		}

		return originExec(args)
	})
	mocker.hook = mockerHook
}

func (mocker *Mocker) Patch() *Mocker {
	mocker.lock.Lock()
	defer mocker.lock.Unlock()
	if mocker.isPatched {
		return mocker
	}
	runtimeTarget := mocker.builder.analyzer.RuntimeTargetValue()
	mocker.patch = monkey.PatchValue(runtimeTarget, mocker.hook, mocker.proxy, mocker.builder.unsafe)
	mocker.isPatched = true
	addToGlobal(mocker)

	mocker.outerCaller = tool.OuterCaller()
	return mocker
}

func (mocker *Mocker) UnPatch() *Mocker {
	mocker.lock.Lock()
	defer mocker.lock.Unlock()
	if !mocker.isPatched {
		return mocker
	}
	mocker.patch.Unpatch()
	mocker.isPatched = false
	removeFromGlobal(mocker)
	atomic.StoreInt64(&mocker.times, 0)
	atomic.StoreInt64(&mocker.mockTimes, 0)

	return mocker
}

func (mocker *Mocker) Release() *MockBuilder {
	mocker.UnPatch()
	mocker.builder.resetCondition()
	return mocker.builder
}

func (mocker *Mocker) ExcludeCurrentGoRoutine() *Mocker {
	return mocker.rePatch(func() {
		mocker.builder.ExcludeCurrentGoRoutine()
	})
}

func (mocker *Mocker) FilterGoRoutine(filter FilterGoroutineType, gId int64) *Mocker {
	return mocker.rePatch(func() {
		mocker.builder.FilterGoRoutine(filter, gId)
	})
}

func (mocker *Mocker) IncludeCurrentGoRoutine() *Mocker {
	return mocker.rePatch(func() {
		mocker.builder.IncludeCurrentGoRoutine()
	})
}

func (mocker *Mocker) When(when interface{}) *Mocker {
	tool.Assert(len(mocker.builder.conditions) == 1, "only one-condition mocker could reset when (You can call Release first, then rebuild mocker)")

	return mocker.rePatch(func() {
		mocker.builder.conditions[0].SetWhenForce(when)
	})
}

func (mocker *Mocker) To(to interface{}) *Mocker {
	tool.Assert(len(mocker.builder.conditions) == 1, "only one-condition mocker could reset to  (You can call Release first, then rebuild mocker)")

	return mocker.rePatch(func() {
		mocker.builder.conditions[0].SetToForce(to)
	})
}

func (mocker *Mocker) Return(results ...interface{}) *Mocker {
	tool.Assert(len(mocker.builder.conditions) == 1, "only one-condition mocker could reset return  (You can call Release first, then rebuild mocker)")

	return mocker.rePatch(func() {
		mocker.builder.conditions[0].SetReturnForce(results...)
	})
}

func (mocker *Mocker) Origin(funcPtr interface{}) *Mocker {
	return mocker.rePatch(func() {
		mocker.builder.origin(funcPtr)
	})
}

func (mocker *Mocker) rePatch(do func()) *Mocker {
	mocker.UnPatch()
	do()
	mocker.build()
	mocker.Patch()
	return mocker
}

func (mocker *Mocker) access() {
	atomic.AddInt64(&mocker.times, 1)
}

func (mocker *Mocker) mock() {
	atomic.AddInt64(&mocker.mockTimes, 1)
}

func (mocker *Mocker) Times() int {
	return int(atomic.LoadInt64(&mocker.times))
}

func (mocker *Mocker) MockTimes() int {
	return int(atomic.LoadInt64(&mocker.mockTimes))
}

func (mocker *Mocker) key() uintptr {
	return mocker.target.Pointer()
}

func (mocker *Mocker) name() string {
	return mocker.target.String()
}

func (mocker *Mocker) unPatch() {
	mocker.UnPatch()
}

func (mocker *Mocker) caller() tool.CallerInfo {
	return mocker.outerCaller
}
