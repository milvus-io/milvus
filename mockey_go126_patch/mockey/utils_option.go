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
)

type methodOption struct {
	unexportedTargetType reflect.Type
}

type methodOptionFn func(*methodOption)

// OptUnexportedTargetType specifies the unexported method type for the GetMethod API. The method type specified by
// this option should not contain the receiver.
// This is useful when the unexported method type is ignored during compilation, see https://github.com/bytedance/mockey/issues/80.
//
// Example:
// var funcType func() [32]byte
// fn := GetMethod(sha256.New(), "checkSum", OptUnexportedTargetType(funcType))
func OptUnexportedTargetType(i interface{}) methodOptionFn {
	tool.AssertFunc(i)
	return func(option *methodOption) {
		option.unexportedTargetType = reflect.TypeOf(i)
	}
}

func resolveMethodOpt(fn ...methodOptionFn) *methodOption {
	opt := &methodOption{
		unexportedTargetType: nil,
	}
	for _, f := range fn {
		f(opt)
	}
	return opt
}
