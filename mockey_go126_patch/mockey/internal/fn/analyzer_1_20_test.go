//go:build go1.20
// +build go1.20

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
	"fmt"
	"reflect"
	"testing"

	"github.com/bytedance/mockey/internal/fn/type4test"
)

func TestAnalyzerImpl_RuntimeTargetType(t *testing.T) {
	type fields struct {
		target interface{}
	}
	tests := []struct {
		name   string
		fields fields
		want   reflect.Type
	}{
		{"type4test.Foo[string]", fields{type4test.Foo[string]}, reflect.TypeOf(func(GenericInfo, string) {})},
		{"type4test.NoArgs[string]}", fields{type4test.NoArgs[string]}, reflect.TypeOf(func(GenericInfo) {})},
		{"(*type4test.A[string]).Foo", fields{(*type4test.A[string]).Foo}, reflect.TypeOf(func(*type4test.A[string], GenericInfo, int) {})},
		{"(*type4test.A[string]).NoArgs", fields{(*type4test.A[string]).NoArgs}, reflect.TypeOf(func(*type4test.A[string], GenericInfo) {})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := NewAnalyzer(tt.fields.target, nil, nil)
			if got := a.RuntimeTargetType(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("RuntimeTargetType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAnalyzerImpl_IsGeneric(t *testing.T) {
	tests := []struct {
		name string
		fn   interface{}
		want bool
	}{
		{"type4test.Foo0", type4test.Foo0, false},
		{"type4test.GlobalFn1", type4test.GlobalFn1, false},
		{"type4test.GlobalFn2", type4test.GlobalFn2, false},
		{"func(int) {}", func(int) {}, false},
		{"func(type4test.A0, int) {}", func(type4test.A0, int) {}, false},
		{"globalFn1", globalFn1, false},
		{"globalFn2", globalFn2, false},
		{"foo", foo, false},
		{"type4test.Foo[string]", type4test.Foo[string], true},
		{"type4test.Foo[type4test.A0]", type4test.Foo[type4test.A0], true},
		{"type4test.Foo[type4test.A[string]]", type4test.Foo[type4test.A[string]], true},
		{"globalFn3(0)(0)", globalFn3(0)(0), false},
		{"newB().Hi", newB().Hi, false},
		{"newB().hi", newB().hi, false},
		{"reflectFn()", reflectFn(), false},
		{"type4test.A0.Foo", type4test.A0.Foo, false},
		{"(*type4test.A0).Foo", (*type4test.A0).Foo, false},
		{"(*type4test.A0).Bar", (*type4test.A0).Bar, false},
		{"a.func1", a.func1, false},
		{"(*a).func1", (*a).func1, false},
		{"a.haha", a.haha, false},
		{"(*a).haha", (*a).haha, false},
		{"(*a).func2", (*a).func2, false},
		{"type4test.A[string].Foo", type4test.A[string].Foo, true},
		{"(*type4test.A[string]).Foo", (*type4test.A[string]).Foo, true},
		{"type4test.A[int].Foo", type4test.A[int].Foo, true},
		{"(*type4test.A[int]).Foo", (*type4test.A[int]).Foo, true},
		{"(*type4test.A[string]).Bar", (*type4test.A[string]).Bar, true},
		{"(*type4test.A[int]).Bar", (*type4test.A[int]).Bar, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := (&AnalyzerImpl{target: tt.fn}).init()
			if got := a.IsGeneric(); got != tt.want {
				t.Errorf("IsGeneric() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAnalyzerImpl_IsMethod(t *testing.T) {
	tests := []struct {
		name string
		fn   interface{}
		want bool
	}{
		{"type4test.Foo0", type4test.Foo0, false},
		{"type4test.GlobalFn1", type4test.GlobalFn1, false},
		{"type4test.GlobalFn2", type4test.GlobalFn2, false},
		{"func(int) {}", func(int) {}, false},
		{"func(type4test.A0, int) {}", func(type4test.A0, int) {}, false},
		{"globalFn1", globalFn1, false},
		{"globalFn2", globalFn2, false},
		{"foo", foo, false},
		{"type4test.Foo[string]", type4test.Foo[string], false},
		{"type4test.Foo[type4test.A0]", type4test.Foo[type4test.A0], false},
		{"type4test.Foo[type4test.A[string]]", type4test.Foo[type4test.A[string]], false},
		{"globalFn3(0)(0)", globalFn3(0)(0), false},
		{"newB().Hi", newB().Hi, false},
		{"newB().hi", newB().hi, false},
		{"reflectFn()", reflectFn(), false},
		{"type4test.A0.Foo", type4test.A0.Foo, true},
		{"(*type4test.A0).Foo", (*type4test.A0).Foo, true},
		{"(*type4test.A0).Bar", (*type4test.A0).Bar, true},
		{"a.func1", a.func1, false}, // FIXME: misjudge as a function
		{"(*a).func1", (*a).func1, true},
		{"a.haha", a.haha, true},
		{"(*a).haha", (*a).haha, true},
		{"(*a).func2", (*a).func2, true},
		{"type4test.A[string].Foo", type4test.A[string].Foo, true},
		{"(*type4test.A[string]).Foo", (*type4test.A[string]).Foo, true},
		{"type4test.A[int].Foo", type4test.A[int].Foo, true},
		{"(*type4test.A[int]).Foo", (*type4test.A[int]).Foo, true},
		{"(*type4test.A[string]).Bar", (*type4test.A[string]).Bar, true},
		{"(*type4test.A[int]).Bar", (*type4test.A[int]).Bar, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := (&AnalyzerImpl{target: tt.fn}).init()
			if got := a.method; got != tt.want {
				t.Errorf("IsMethod() = %v, want %v", got, tt.want)
			}
		})
	}
}

var (
	globalFn1 = func(int) {}
	globalFn2 = func(type4test.A0, int) {}
	globalFn3 = func(int) func(int) func(int) {
		b := func(int) func(int) {
			a := func(int) {}
			return a
		}
		return b
	}
)

func newB() b {
	return b{
		Hi: func(int) {
			fmt.Println("hi")
		},
		hi: func(int) {

		},
	}
}

type b struct {
	Hi func(int)
	hi func(int)
}

type a int

func (g a) func1(i int)  {}
func (g a) haha(i int)   {}
func (g *a) func2(i int) {}

func foo(int) {}

func reflectFn() interface{} {
	typ := reflect.FuncOf([]reflect.Type{reflect.TypeOf(0)}, []reflect.Type{reflect.TypeOf("")}, false)
	return reflect.MakeFunc(typ, func(args []reflect.Value) (results []reflect.Value) { return nil }).Interface()
}
