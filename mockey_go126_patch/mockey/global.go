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

	"github.com/bytedance/mockey/internal/tool"
	"github.com/smartystreets/goconvey/convey"
)

var gMocker = make([]map[uintptr]mockerInstance, 0)

func init() {
	gMocker = append(gMocker, make(map[uintptr]mockerInstance))
}

func addToGlobal(mocker mockerInstance) {
	key := mocker.key()
	tool.DebugPrintf("[addToGlobal] 0x%x added\n", key)
	last, ok := gMocker[len(gMocker)-1][key]
	if ok {
		tool.Assert(!ok, "re-mock %v, previous mock at: %v", last.name(), last.caller())
	}
	gMocker[len(gMocker)-1][key] = mocker
}

func removeFromGlobal(mocker mockerInstance) {
	key := mocker.key()
	tool.DebugPrintf("[removeFromGlobal] 0x%x removed\n", key)
	delete(gMocker[len(gMocker)-1], key)
}

// PatchConvey creates a test context that automatically manages mock lifecycles.
// It wraps around the `convey.Convey` function and adds automatic mock cleanup functionality.
//
// Benefits:
// - No need to manually manage mock cleanup with defer statements
// - Supports nested contexts, where each level only cleans up its own mocks
// - Ensures proper mock isolation between test cases
//
// Usage examples:
//
// Basic usage:
//
//	PatchConvey("Test case description", t, func() {
//	    // Create mocks here
//	    Mock(someFunction).Return(42).Build()
//	    // Test code that uses the mocks
//	})
//	// All mocks are automatically cleaned up here
//
// Nested usage:
//
//	PatchConvey("Outer test", t, func() {
//	    Mock(outerFunc).Return(1).Build()
//	    PatchConvey("Inner test", func() {
//	        Mock(innerFunc).Return(2).Build()
//	        // Both outerFunc and innerFunc are mocked here
//	    })
//	    // Only innerFunc is cleaned up, outerFunc remains mocked
//	})
//	// All mocks are cleaned up
func PatchConvey(items ...interface{}) {
	for i, item := range items {
		if reflect.TypeOf(item).Kind() == reflect.Func {
			items[i] = reflect.MakeFunc(reflect.TypeOf(item), func(args []reflect.Value) []reflect.Value {
				gMocker = append(gMocker, make(map[uintptr]mockerInstance))
				defer func() {
					for _, mocker := range gMocker[len(gMocker)-1] {
						mocker.unPatch()
					}
					gMocker = gMocker[:len(gMocker)-1]
				}()
				return tool.ReflectCall(reflect.ValueOf(item), args)
			}).Interface()
		}
	}

	convey.Convey(items...)
}

// PatchRun creates a test context that automatically manages mock lifecycles.
//
// Benefits:
// - No need to manually manage mock cleanup with defer statements
// - Supports nested contexts, where each level only cleans up its own mocks
// - More lightweight than PatchConvey when goconvey integration is not needed
//
// Usage example:
//
//	PatchRun(func() {
//	    // Create mocks here
//	    Mock(someFunction).Return(42).Build()
//	    // Test code that uses the mocks
//	    result := someFunction() // Returns 42
//	})
//	// All mocks are automatically cleaned up here
//	result := someFunction() // Returns original value
//
// Nested usage example:
//
//	PatchRun(func() {
//	    Mock(functionA).Return("outer").Build()
//
//	    // Inner PatchRun inherits outer mocks but can override them
//	    PatchRun(func() {
//	        Mock(functionB).Return("inner").Build()
//	        Mock(functionA).Return("overridden").Build()
//
//	        resultA := functionA() // Returns "overridden"
//	        resultB := functionB() // Returns "inner"
//	    })
//	    // Inner mocks are cleaned up, outer mocks remain
//
//	    resultA := functionA() // Returns "outer"
//	    resultB := functionB() // Returns original value
//	})
//	// All mocks are cleaned up
//	resultA := functionA() // Returns original value
func PatchRun(f func()) {
	gMocker = append(gMocker, make(map[uintptr]mockerInstance))
	defer func() {
		for _, mocker := range gMocker[len(gMocker)-1] {
			mocker.unPatch()
		}
		gMocker = gMocker[:len(gMocker)-1]
	}()
	f()
}

// UnPatchAll unpatch all mocks in current `PatchConvey` or `PatchRun` context. If the caller is out of `PatchConvey`
// or `PatchRun`, it will unpatch all mocks.
//
// For example:
//
//	Test1(t) {
//	    Mock(a).Build()
//		Mock(b).Build()
//
//	    // a and b will be unpatched
//		UnpatchAll()
//	}
//
//	Test2(t) {
//		Mock(a).Build()
//		PatchConvey(t,func(){
//		    Mock(b).Build()
//
//			// only b will be unpatched
//			UnpatchAll()
//		}
//	}
func UnPatchAll() {
	for _, mocker := range gMocker[len(gMocker)-1] {
		mocker.unPatch()
	}
}
