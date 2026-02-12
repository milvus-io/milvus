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

package type4test

var (
	GlobalFn1 = func(int) {}
	GlobalFn2 = func(A0, int) {}
)

func Foo0(int) {}

type A0 struct {
	Inner string
}

func (f A0) Foo(i int)  {}
func (f *A0) Bar(i int) {}

func Foo[T any](t T) {}

func NoArgs[T any]() {}

type A[T any] struct {
	Inner T
}

func (f A[T]) Foo(i int)       {}
func (f *A[T]) Bar(i int, t T) {}
func (f *A[T]) NoArgs()        {}
