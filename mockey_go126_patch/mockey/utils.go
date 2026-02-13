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

	"github.com/bytedance/mockey/internal/monkey/fn"
	"github.com/bytedance/mockey/internal/tool"
	"github.com/bytedance/mockey/internal/unsafereflect"
)

// GetMethod resolves a method with the specified name from the given instance.
// Supports finding:
// - Exported and unexported methods
// - Methods for value types and pointer types
// - Methods in nested anonymous fields
// - Method fields of structs
// Parameters:
// - instance: The instance to find the method on
// - methodName: The name of the method to find
// Return value:
// - The interface of the found method, triggers assertion failure if not found
func GetMethod(instance interface{}, methodName string, opt ...methodOptionFn) (res interface{}) {
	opts := resolveMethodOpt(opt...)
	if m, ok := getMethod(reflect.ValueOf(instance), methodName, opts); ok {
		return m.Interface()
	}
	tool.Assert(false, "can't reflect instance method: %v", methodName)
	return nil
}

func getMethod(val reflect.Value, methodName string, opts *methodOption) (method reflect.Value, ok bool) {
	if !val.IsValid() {
		return
	}
	typ := val.Type()
	kind := typ.Kind()

	if kind == reflect.Ptr || kind == reflect.Interface {
		val = val.Elem()
		if !val.IsValid() {
			return
		}
		typ = val.Type()
		kind = typ.Kind()
	}

	// check nested if it has anonymous fields
	if kind == reflect.Struct {
		for i := 0; i < typ.NumField(); i++ {
			if !typ.Field(i).Anonymous {
				continue
			}
			if m, ok := getMethod(val.Field(i), methodName, opts); ok {
				tool.DebugPrintf("[GetMethod] found in nested field, type: %v, field: %v\n", typ, typ.Field(i).Name)
				return m, true
			}
		}
	}

	// check elem type for exported method
	if m, ok := typ.MethodByName(methodName); ok {
		return m.Func, true
	}
	// check elem type for field method
	if m, ok := getFieldMethod(val, methodName); ok {
		return m, true
	}
	// check elem type for unexported method
	if m, ok := unexportedMethodByName(typ, methodName, opts); ok {
		return m, true
	}

	// check ptr type for exported or unexported method
	ptrType := reflect.PtrTo(typ)
	if m, ok := ptrType.MethodByName(methodName); ok {
		return m.Func, true
	}
	if m, ok := unexportedMethodByName(ptrType, methodName, opts); ok {
		return m, true
	}
	return
}

// getFieldMethod gets a functional field's value as an instance
// The return instance is not original field but a new function object points to
// the same function.
// for example:
//
//	  type Fn func()
//	  type Foo struct {
//			privateField Fn
//	  }
//	  func NewFoo() Foo { return Foo{ privateField: func() { /*do nothing*/ } }}
//
// getFieldMethod(NewFoo(),"privateField") will return a function object which
// points to the anonymous function in NewFoo
func getFieldMethod(v reflect.Value, fieldName string) (res reflect.Value, ok bool) {
	if v.Kind() != reflect.Struct {
		return
	}

	field := v.FieldByName(fieldName)
	if !field.IsValid() || field.Kind() != reflect.Func {
		return
	}
	return fn.MakeFunc(field.Type(), field.Pointer()), true
}

// GetPrivateMethod resolve a certain public method from an instance.
// Deprecated, this is an old API in mockito. Please use GetMethod instead.
func GetPrivateMethod(instance interface{}, methodName string) interface{} {
	m, ok := reflect.TypeOf(instance).MethodByName(methodName)
	if ok {
		return m.Func.Interface()
	}
	tool.Assert(false, "can't reflect instance method: %v", methodName)
	return nil
}

// GetNestedMethod resolves a certain public method in anonymous structs, it will
// look for the specific method in every anonymous struct field recursively.
// Deprecated, this is an old API in mockito. Please use GetMethod instead.
func GetNestedMethod(instance interface{}, methodName string) interface{} {
	if typ := reflect.TypeOf(instance); typ != nil {
		if m, ok := getNestedMethod(reflect.ValueOf(instance), methodName); ok {
			return m.Func.Interface()
		}
	}
	tool.Assert(false, "can't reflect instance method: %v", methodName)
	return nil
}

func getNestedMethod(val reflect.Value, methodName string) (reflect.Method, bool) {
	typ := val.Type()
	kind := typ.Kind()
	if kind == reflect.Ptr || kind == reflect.Interface {
		val = val.Elem()
	}
	if !val.IsValid() {
		return reflect.Method{}, false
	}

	typ = val.Type()
	kind = typ.Kind()
	if kind == reflect.Struct {
		for i := 0; i < typ.NumField(); i++ {
			if !typ.Field(i).Anonymous {
				// there is no need to acquire non-anonymous method
				continue
			}
			if m, ok := getNestedMethod(val.Field(i), methodName); ok {
				return m, true
			}
		}
	}
	// a struct receiver is prior to the corresponding pointer receiver
	if m, ok := typ.MethodByName(methodName); ok {
		return m, true
	}
	return reflect.PtrTo(typ).MethodByName(methodName)
}

// unexportedMethodByName resolve an unexported method from an instance
func unexportedMethodByName(instanceType reflect.Type, methodName string, opts *methodOption) (res reflect.Value, ok bool) {
	if ch0 := methodName[0]; ch0 >= 'A' && ch0 <= 'Z' {
		return
	}
	typ, tfn, ok := unsafereflect.MethodByName(instanceType, methodName)
	if !ok {
		return
	}
	tool.Assert(typ != nil || opts.unexportedTargetType != nil, "failed to determine %v's type, please use `OptUnexportedTargetType` to specify", methodName)

	if opts.unexportedTargetType != nil {
		tool.DebugPrintf("[GetMethod] force determine unexported type: %v\n", typ)
		typ = opts.unexportedTargetType
	}
	newType := tool.NewFuncTypeByInsertIn(typ, instanceType)
	return fn.MakeFunc(newType, tfn), true
}

// GetGoroutineId gets the current goroutine ID
func GetGoroutineId() int64 {
	return tool.GetGoroutineID()
}
