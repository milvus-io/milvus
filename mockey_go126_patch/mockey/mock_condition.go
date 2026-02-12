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

type mockCondition struct {
	when interface{} // condition
	hook interface{} // mock function

	builder *MockBuilder
}

func (m *mockCondition) Complete() bool {
	return m.when != nil && m.hook != nil
}

func (m *mockCondition) SetWhen(when interface{}) {
	tool.Assert(m.when == nil, "re-set builder when")
	m.SetWhenForce(when)
}

func (m *mockCondition) SetWhenForce(when interface{}) {
	wVal, wTyp := reflect.ValueOf(when), reflect.TypeOf(when)
	tool.Assert(wTyp.NumOut() == 1, "when func ret value not bool")
	out1 := wTyp.Out(0)
	tool.Assert(out1.Kind() == reflect.Bool, "when func ret value not bool")

	adapter := m.builder.analyzer.InputAdapter("when", wTyp)
	runtimeWhenType := tool.NewFuncTypeByOut(m.builder.runtimeTargetType(), out1)

	m.when = reflect.MakeFunc(runtimeWhenType, func(args []reflect.Value) []reflect.Value {
		return tool.ReflectCall(wVal, adapter(args))
	}).Interface()
}

func (m *mockCondition) SetReturn(results ...interface{}) {
	tool.Assert(m.hook == nil, "re-set builder hook")
	m.SetReturnForce(results...)
}

func (m *mockCondition) SetReturnForce(results ...interface{}) {
	getResult := func() []interface{} { return results }
	if len(results) == 1 {
		seq, ok := results[0].(SequenceOpt)
		if ok {
			getResult = seq.GetNext
		}
	}

	hookType := m.builder.runtimeTargetType()
	m.hook = reflect.MakeFunc(hookType, func([]reflect.Value) []reflect.Value {
		current := getResult()
		tool.CheckReturnValues(hookType, current...)
		return tool.MakeReturnValues(hookType, current...)
	}).Interface()
}

func (m *mockCondition) SetTo(to interface{}) {
	tool.Assert(m.hook == nil, "re-set builder hook")
	m.SetToForce(to)
}

func (m *mockCondition) SetToForce(to interface{}) {
	toType := reflect.TypeOf(to)
	tool.CheckFuncReturnValues(m.builder.analyzer.TargetType(), toType)
	adapter := m.builder.analyzer.InputAdapter("hook", toType)
	m.hook = reflect.MakeFunc(m.builder.runtimeTargetType(), func(args []reflect.Value) (results []reflect.Value) {
		return tool.ReflectCall(reflect.ValueOf(to), adapter(args))
	}).Interface()
}
