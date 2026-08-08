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
	"math"
	"sync"

	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ProductionRuntime owns the util-layer PyUDF runtime and FileResource cache.
type ProductionRuntime struct {
	cache       *Cache
	unavailable Runtime

	closeOnce sync.Once
}

// NewProductionRuntime initializes the process-lifetime PyUDF runtime. When
// enabled, embedded CPython is initialized immediately; individual user wheels
// remain lazily loaded by the cache on acquisition. The returned value also
// implements fileresource.Listener for package-global composition.
func NewProductionRuntime(ctx context.Context, config Config) (*ProductionRuntime, error) {
	return newProductionRuntime(
		ctx,
		config,
		EmbeddedBuildCapability,
		initializeNativeRuntime,
		func(instances int32) ResourceLoader { return newEmbeddedResourceLoader(instances) },
	)
}

func newProductionRuntime(
	ctx context.Context,
	config Config,
	capability func() BuildCapability,
	initialize func() error,
	newLoader func(int32) ResourceLoader,
) (*ProductionRuntime, error) {
	if ctx == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: runtime context is nil")
	}
	if !config.Enabled {
		return &ProductionRuntime{
			unavailable: NewUnavailableRuntime("function.pyUDF.enabled is false"),
		}, nil
	}
	if capability == nil || initialize == nil || newLoader == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: runtime dependencies are nil")
	}
	if err := ValidateConfigCapability(config, capability()); err != nil {
		return nil, err
	}
	if config.InstancesPerResource <= 0 || config.InstancesPerResource > math.MaxInt32 {
		return nil, merr.WrapErrServiceInternalMsg(
			"py_udf: instancesPerResource %d exceeds native range",
			config.InstancesPerResource,
		)
	}
	if err := initialize(); err != nil {
		return nil, merr.Wrap(err, "py_udf: initialize embedded runtime")
	}
	cache, err := NewCache(
		ctx,
		newLoader(int32(config.InstancesPerResource)),
		config.LoadTimeout,
	)
	if err != nil {
		return nil, err
	}
	return &ProductionRuntime{cache: cache}, nil
}

func (runtime *ProductionRuntime) Acquire(ctx context.Context, resourceName, stage string) (Lease, error) {
	if runtime == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: production runtime is nil")
	}
	if runtime.unavailable != nil {
		return runtime.unavailable.Acquire(ctx, resourceName, stage)
	}
	if runtime.cache == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: production runtime cache is nil")
	}
	return runtime.cache.Acquire(ctx, resourceName, stage)
}

func (runtime *ProductionRuntime) OnFileResourceSync(event fileresource.SyncEvent) error {
	if runtime == nil || runtime.cache == nil {
		return nil
	}
	return runtime.cache.OnFileResourceSync(event)
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
