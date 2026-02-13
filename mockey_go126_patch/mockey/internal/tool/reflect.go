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

package tool

import (
	"reflect"
)

func ReflectCall(f reflect.Value, args []reflect.Value) []reflect.Value {
	if f.Type().IsVariadic() {
		newArgs := make([]reflect.Value, 0)
		lastArg := args[len(args)-1]
		for i := 0; i < len(args)-1; i++ {
			newArgs = append(newArgs, args[i])
		}

		for i := 0; i < lastArg.Len(); i++ {
			newArgs = append(newArgs, lastArg.Index(i))
		}
		return f.Call(newArgs)
	} else {
		return f.Call(args)
	}
}

func NewFuncTypeByOut(ft reflect.Type, newOutTypes ...reflect.Type) reflect.Type {
	var inTypes []reflect.Type
	for i := 0; i < ft.NumIn(); i++ {
		inTypes = append(inTypes, ft.In(i))
	}
	return reflect.FuncOf(inTypes, newOutTypes, ft.IsVariadic())
}

func NewFuncTypeByInsertIn(ft reflect.Type, newInTypes ...reflect.Type) reflect.Type {
	inTypes := newInTypes
	for i := 0; i < ft.NumIn(); i++ {
		inTypes = append(inTypes, ft.In(i))
	}
	outTypes := make([]reflect.Type, ft.NumOut())
	for i := range outTypes {
		outTypes[i] = ft.Out(i)
	}
	return reflect.FuncOf(inTypes, outTypes, ft.IsVariadic())
}

func NewFuncTypeByReplaceIn(ft reflect.Type, newInType reflect.Type, newInIndex int) reflect.Type {
	inTypes := make([]reflect.Type, ft.NumIn())
	for i := 0; i < ft.NumIn(); i++ {
		if i == newInIndex {
			inTypes[i] = newInType
		} else {
			inTypes[i] = ft.In(i)
		}
	}
	outTypes := make([]reflect.Type, ft.NumOut())
	for i := range outTypes {
		outTypes[i] = ft.Out(i)
	}
	return reflect.FuncOf(inTypes, outTypes, ft.IsVariadic())
}

func MakeEmptyInArgs(ft reflect.Type) []reflect.Value {
	args := make([]reflect.Value, ft.NumIn())
	for i := range args {
		args[i] = MakeEmtpy(ft.In(i))
	}
	return args
}

func MakeEmptyOutArgs(ft reflect.Type) []reflect.Value {
	args := make([]reflect.Value, ft.NumOut())
	for i := range args {
		args[i] = MakeEmtpy(ft.Out(i))
	}
	return args
}

func MakeEmtpy(typ reflect.Type) reflect.Value {
	switch typ.Kind() {
	case reflect.Ptr:
		return reflect.New(typ.Elem())
	default:
		return reflect.Zero(typ)
	}
}

func MakeReturnValues(ft reflect.Type, results ...interface{}) []reflect.Value {
	var retValues []reflect.Value
	for i, result := range results {
		retValue := reflect.Zero(ft.Out(i))
		if result != nil {
			retValue = reflect.ValueOf(result).Convert(ft.Out(i))
		}
		retValues = append(retValues, retValue)
	}
	return retValues
}
