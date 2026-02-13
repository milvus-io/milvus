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

package iface

import (
	"github.com/bytedance/mockey"
	"github.com/bytedance/mockey/exp/iface/internal"
	"github.com/bytedance/mockey/internal/tool"
)

type Mocker struct {
	builder *MockBuilder
	mockers []*mockey.Mocker
}

type MockBuilder struct {
	builders []*mockey.MockBuilder
}

func Mock(target interface{}, opt ...OptionFn) *MockBuilder {
	opts := resolveOpt(opt...)
	targets := internal.FindImplementTargets(target, opts.selector)
	builder := &MockBuilder{}
	tool.DebugPrintf("[InterfaceMock] start to mock for %d targets...\n", len(targets))
	for i, t := range targets {
		builder.builders = append(builder.builders, mockey.Mock(t))
		tool.DebugPrintf("[InterfaceMock] builder generated for index: %d\n", i+1)
	}
	tool.DebugPrintf("[InterfaceMock] mock builder generated for %d targets\n", len(targets))
	return builder
}

func (builder *MockBuilder) When(when interface{}) *MockBuilder {
	for _, b := range builder.builders {
		b.When(when)
	}
	return builder
}

func (builder *MockBuilder) To(hook interface{}) *MockBuilder {
	for _, b := range builder.builders {
		b.To(hook)
	}
	return builder
}

func (builder *MockBuilder) Return(results ...interface{}) *MockBuilder {
	for _, b := range builder.builders {
		b.Return(results...)
	}
	return builder
}

func (builder *MockBuilder) Build() *Mocker {
	tool.DebugPrintf("[InterfaceMock] start to build for %d targets...\n", len(builder.builders))
	mocker := Mocker{builder: builder}
	for i, b := range builder.builders {
		mocker.mockers = append(mocker.mockers, b.Build())
		tool.DebugPrintf("[InterfaceMock] mocker generated for index: %d\n", i+1)
	}
	tool.DebugPrintf("[InterfaceMock] mocker generated for %d targets\n", len(builder.builders))
	return &mocker
}

func (mocker *Mocker) Patch() *Mocker {
	tool.DebugPrintf("[InterfaceMock] start to patch for %d targets...\n", len(mocker.mockers))
	for i, m := range mocker.mockers {
		m.Patch()
		tool.DebugPrintf("[InterfaceMock] mocker patched for index: %d\n", i+1)
	}
	tool.DebugPrintf("[InterfaceMock] mocker patched for %d targets\n", len(mocker.mockers))
	return mocker
}

func (mocker *Mocker) UnPatch() *Mocker {
	tool.DebugPrintf("[InterfaceMock] start to unpatch for %d targets...\n", len(mocker.mockers))
	for i, m := range mocker.mockers {
		m.UnPatch()
		tool.DebugPrintf("[InterfaceMock] mocker unpatched for index: %d\n", i+1)
	}
	tool.DebugPrintf("[InterfaceMock] mocker unpatched for %d targets\n", len(mocker.mockers))
	return mocker
}

func (mocker *Mocker) Times() int {
	var res int
	for _, m := range mocker.mockers {
		res += m.Times()
	}
	return res
}

func (mocker *Mocker) MockTimes() int {
	var res int
	for _, m := range mocker.mockers {
		res += m.MockTimes()
	}
	return res
}
