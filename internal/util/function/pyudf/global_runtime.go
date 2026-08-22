// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pyudf

import (
	"context"
	"sync"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type runtimeProvider struct {
	once       sync.Once
	runtime    Runtime
	initErr    error
	newRuntime func() (Runtime, error)
}

func newRuntimeProvider(newRuntime func() (Runtime, error)) *runtimeProvider {
	return &runtimeProvider{newRuntime: newRuntime}
}

func (p *runtimeProvider) Get() (Runtime, error) {
	if p == nil || p.newRuntime == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: global runtime initializer is nil")
	}
	p.once.Do(func() {
		p.runtime, p.initErr = p.newRuntime()
		if p.initErr != nil {
			p.initErr = merr.Wrap(p.initErr, "py_udf: initialize global runtime")
			return
		}
		if p.runtime == nil {
			p.initErr = merr.WrapErrServiceInternalMsg("py_udf: global runtime initializer returned nil")
		}
	})
	if p.initErr != nil {
		return nil, p.initErr
	}
	return p.runtime, nil
}

var globalRuntimeProvider = newRuntimeProvider(func() (Runtime, error) {
	params := paramtable.Get()
	config, err := NewConfig(params)
	if err != nil {
		return nil, err
	}
	return NewProductionRuntime(context.Background(), config)
})

// GetGlobalRuntime returns the process-wide PyUDF runtime, initializing its Go
// configuration and cache on the first request-level expression construction.
// Embedded CPython remains lazily initialized by ProductionRuntime.Acquire.
func GetGlobalRuntime() (Runtime, error) {
	return globalRuntimeProvider.Get()
}
