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

package internal

import (
	"reflect"
	"runtime"
	"unsafe"

	"github.com/bytedance/mockey/internal/fn"
	fn2 "github.com/bytedance/mockey/internal/monkey/fn"
	"github.com/bytedance/mockey/internal/monkey/linkname"
	"github.com/bytedance/mockey/internal/tool"
)

func FindImplementTargets(i interface{}, selector Selector) []interface{} {
	iType := reflect.TypeOf(i)
	tool.Assert(iType.Kind() == reflect.Func, "'%v' is not a function", iType.Kind())
	tool.Assert(iType.NumIn() >= 1, "'%v' must have receiver", iType)
	tool.Assert(iType.In(0).Kind() == reflect.Interface, "'%v' must have interface receiver", iType)

	iPC := reflect.ValueOf(i).Pointer()
	iFun := runtime.FuncForPC(iPC)
	iAnalyzer := fn.NewNameAnalyzer(iFun.Name(), false)
	iName := iAnalyzer.FuncName()
	iArgSizeWithoutReceiver := totalArgSize(iFun) - int32(iType.In(0).Size())

	var res []interface{}
	for _, fi := range funcInfoMap[iName] {
		// Due to the lack of type information, our methods for finding targets are very limited.
		pc := fi.Func.Entry()
		// Exclude interface itself
		if pc == iPC {
			continue
		}
		argSizeWithoutReceiver := totalArgSize(fi.Func) - int32(reflect.TypeOf(uintptr(0)).Size())
		// Exclude argument size not match
		if argSizeWithoutReceiver != iArgSizeWithoutReceiver {
			continue
		}
		// Exclude function name not match
		if selector != nil && !selector.Match(fi) {
			continue
		}
		newType := tool.NewFuncTypeByReplaceIn(reflect.TypeOf(i), reflect.TypeOf(unsafe.Pointer(nil)), 0)
		res = append(res, fn2.MakeFunc(newType, pc).Interface())
	}
	return res
}

var funcInfoMap = make(map[string][]*funcInfo)

type funcInfo struct {
	Func     *runtime.Func
	Analyzer *fn.NameAnalyzer
}

func init() {
	for _, fun := range linkname.FuncList() {
		fullName := fun.Name()
		// Exclude internal functions that do not have a name
		if fullName == "" {
			continue
		}
		analyzer := fn.NewNameAnalyzer(fullName, false)
		// Exclude functions that do not have a receiver name
		if !analyzer.HasMiddleName() {
			continue
		}
		// Exclude methods that do not have pointer receivers. If a type implements an interface, all methods of that
		// interface must have pointer receivers.
		if !analyzer.IsPtrReceiver() {
			continue
		}
		name := analyzer.FuncName()
		funcInfoMap[name] = append(funcInfoMap[name], &funcInfo{Func: fun, Analyzer: analyzer})
	}
}

func totalArgSize(f *runtime.Func) int32 {
	const argsOffset = 8
	return *(*int32)(unsafe.Pointer(uintptr(unsafe.Pointer(f)) + argsOffset))
}
