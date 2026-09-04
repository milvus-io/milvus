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
)

// ProductionRuntime owns the util-layer PyUDF runtime and FileResource cache.
type ProductionRuntime struct {
	cache       *Cache
	unavailable Runtime

	initializeNative func() error
	initializeOnce   sync.Once
	initializeErr    error

	closeOnce sync.Once
}

// NewProductionRuntime creates the process-lifetime PyUDF runtime. When enabled,
// embedded CPython is initialized lazily on the first acquisition; individual
// user wheels remain lazily loaded by the cache.
func NewProductionRuntime(
	ctx context.Context,
	config Config,
) (*ProductionRuntime, error) {
	return newDefaultProductionRuntime(ctx, config, globalResourceInfo)
}

func newDefaultProductionRuntime(
	ctx context.Context,
	config Config,
	source resourceSource,
) (*ProductionRuntime, error) {
	return newProductionRuntime(
		ctx,
		config,
		source,
		EmbeddedBuildCapability,
		initializeNativeRuntime,
		func() ResourceLoader { return newEmbeddedResourceLoader() },
	)
}

func newProductionRuntime(
	ctx context.Context,
	config Config,
	source resourceSource,
	capability func() BuildCapability,
	initialize func() error,
	newLoader func() ResourceLoader,
) (*ProductionRuntime, error) {
	if ctx == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: runtime context is nil")
	}
	// Keep a backend-independent defensive gate here even though PyUDFExpr also
	// checks the process-wide configuration. ProductionRuntime can be constructed
	// or used directly and must never initialize a backend while PyUDF is disabled.
	if !config.Enabled {
		return &ProductionRuntime{
			unavailable: NewUnavailableRuntime(disabledReason),
		}, nil
	}
	if capability == nil || initialize == nil || newLoader == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: runtime dependencies are nil")
	}
	if err := ValidateConfigCapability(config, capability()); err != nil {
		return nil, err
	}
	if source == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: resource source is nil")
	}
	cache, err := newCache(ctx, source, newLoader())
	if err != nil {
		return nil, err
	}
	return &ProductionRuntime{
		cache:            cache,
		initializeNative: initialize,
	}, nil
}

func (runtime *ProductionRuntime) ensureInitialized() error {
	runtime.initializeOnce.Do(func() {
		if runtime.initializeNative == nil {
			runtime.initializeErr = merr.WrapErrServiceInternalMsg("py_udf: native runtime initializer is nil")
			return
		}
		if err := runtime.initializeNative(); err != nil {
			runtime.initializeErr = merr.Wrap(err, "py_udf: initialize embedded runtime")
		}
	})
	return runtime.initializeErr
}

func (runtime *ProductionRuntime) Acquire(ctx context.Context, resourceName, stage string) (Lease, error) {
	if runtime == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: production runtime is nil")
	}
	// Defense in depth for callers that use ProductionRuntime without PyUDFExpr.
	if runtime.unavailable != nil {
		return runtime.unavailable.Acquire(ctx, resourceName, stage)
	}
	if runtime.cache == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: production runtime cache is nil")
	}

	if err := runtime.ensureInitialized(); err != nil {
		return nil, err
	}
	return runtime.cache.Acquire(ctx, resourceName, stage)
}

// Close prevents new acquisitions and retires all loaded resources. CPython is
// process-lifetime and is intentionally not finalized.
func (runtime *ProductionRuntime) Close() {
	if runtime == nil {
		return
	}
	runtime.closeOnce.Do(func() {
		if runtime.cache != nil {
			runtime.cache.Close()
		}
	})
}
