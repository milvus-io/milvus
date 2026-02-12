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
	"sync/atomic"

	"github.com/bytedance/mockey/internal/monkey"
	monkeyFn "github.com/bytedance/mockey/internal/monkey/fn"
	"github.com/bytedance/mockey/internal/monkey/inst"
	"github.com/bytedance/mockey/internal/tool"
)

type Analyzer interface {
	// TargetType returns the type of the target function or method.
	TargetType() reflect.Type

	// TargetValue returns the value of the target function or method.
	TargetValue() reflect.Value

	// RuntimeTargetType returns the actual type of the target function at runtime. It contains generic type info if the
	// target is generic:
	//  1. function:
	//     a. non-generic function: func(inArgs) outArgs
	//     b. generic function: func(GenericInfo, inArgs) outArgs
	//  2. method:
	//     a. non-generic method: func(self *struct, inArgs) outArgs
	//     b. generic method:
	//     - BEFORE go1.20: func(GenericInfo, self *struct, inArgs) outArgs
	//     - AFTER go1.20: func(self *struct, GenericInfo, inArgs) outArgs
	RuntimeTargetType() reflect.Type

	// RuntimeTargetValue returns the actual value of the target function or method. If the target is generic, the returned
	// value is the gcshape function. Otherwise, it is the same as the target.
	RuntimeTargetValue() reflect.Value

	// IsGeneric returns true if the target is a generic function or method.
	IsGeneric() bool

	// GenericInfo returns the type info of the generic target.
	GenericInfo() GenericInfo

	// InputAdapter generates an adapter function to adapt the input arguments of the RuntimeTargetType() to the inputType.
	// These inputTypes are valid:
	//  1. function:
	//     a. non-generic function: func(inArgs) outArgs
	//     b. generic function: func(info GenericInfo, inArgs) outArgs OR func(inArgs) outArgs
	//  2. method:
	//     a. non-generic method: func(self *struct, inArgs) outArgs OR func(inArgs) outArgs
	//     b. generic method: func(GenericInfo, self *struct, inArgs) outArgs OR func(self *struct, inArgs) outArgs OR func(inArgs) outArgs
	InputAdapter(inputName string, inputType reflect.Type) func([]reflect.Value) []reflect.Value

	// ReversedInputAdapter generates an adapter function to adapt the input arguments of the inputType to the RuntimeTargetType().
	// These inputTypes are valid:
	//  1. function:
	//     a. non-generic function: func(inArgs) outArgs
	//     b. generic function: func(info GenericInfo, inArgs) outArgs OR func(inArgs) outArgs
	//  2. method:
	//     a. non-generic method: func(self *struct, inArgs) outArgs OR func(inArgs) outArgs
	//     b. generic method: func(GenericInfo, self *struct, inArgs) outArgs OR func(self *struct, inArgs) outArgs OR func(inArgs) outArgs
	ReversedInputAdapter(inputName string, inputType reflect.Type) func(inputArgs, extraArgs []reflect.Value) []reflect.Value
}

var (
	genericAnalyzedCount int64
	genericFallbackCount int64
)

func (a *AnalyzerImpl) TargetValue() reflect.Value {
	return a.targetValue
}

func (a *AnalyzerImpl) TargetType() reflect.Type {
	return a.targetType
}

func (a *AnalyzerImpl) RuntimeTargetType() reflect.Type {
	return a.runtimeTargetType
}

func (a *AnalyzerImpl) RuntimeTargetValue() reflect.Value {
	return a.runtimeTargetValue
}

func (a *AnalyzerImpl) IsGeneric() bool {
	return a.generic
}

func (a *AnalyzerImpl) GenericInfo() GenericInfo {
	return a.runtimeGenericInfo
}

// runtimeTargetValueAndGenericInfo0 obtains the runtime value of the target and the generic information.
func (a *AnalyzerImpl) runtimeTargetValueAndGenericInfo0() (reflect.Value, GenericInfo) {
	if !a.IsGeneric() {
		return a.TargetValue(), 0
	}
	tool.DebugPrintf("[Analyzer.init] try to analyze generic\n")
	atomic.AddInt64(&genericAnalyzedCount, 1)
	// Obtain the jump address and generic information address of the generic function through instruction analysis
	jumpAddr, genericInfoAddr := inst.GetGenericAddr(a.TargetValue().Pointer(), 10000)
	// Create a function value based on the runtime type and the obtained jump address
	runtimeTarget, genericInfo := monkeyFn.MakeFunc(a.RuntimeTargetType(), jumpAddr), (GenericInfo)(genericInfoAddr)

	// Fallback genericInfo: obtains generic information by means of actual execution
	if genericInfo == 0 {
		tool.DebugPrintf("[Analyzer.init] fallback genericInfo\n")
		atomic.AddInt64(&genericFallbackCount, 1)

		genericInfoHook := tool.NewFuncTypeByInsertIn(a.TargetType(), genericInfoType)
		genericInfoAdapter := a.InputAdapter("fallbackGenericInfo", genericInfoHook)

		hook := reflect.MakeFunc(a.RuntimeTargetType(), func(args []reflect.Value) []reflect.Value {
			genericInfo = genericInfoAdapter(args)[0].Interface().(GenericInfo) // extract genericInfo from args
			return tool.MakeEmptyOutArgs(a.TargetType())
		})

		proxy := reflect.New(a.RuntimeTargetType())

		patch := monkey.PatchValue(runtimeTarget, hook, proxy, true)
		tool.ReflectCall(a.TargetValue(), tool.MakeEmptyInArgs(a.TargetType()))
		patch.Unpatch()
	}
	tool.DebugPrintf("[Analyzer.init] analyze generic finish, fallback statistics: %d/%d\n", genericFallbackCount, genericAnalyzedCount)
	return runtimeTarget, genericInfo
}
