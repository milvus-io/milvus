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

	"github.com/apache/arrow/go/v17/arrow"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type nativePyUDFResource interface {
	run(inputs []*arrow.Chunked, serializedParams []byte) ([]*arrow.Chunked, error)
	Close() error
}

type embeddedResourceLoader struct {
	instancesPerResource int32
	loadNative           func([]byte) (nativePyUDFResource, error)
}

type embeddedLoadedResource struct {
	resourceName string
	stage        string
	native       nativePyUDFResource

	closeOnce sync.Once
	closeErr  error
}

func newEmbeddedResourceLoader(instancesPerResource int32) *embeddedResourceLoader {
	return &embeddedResourceLoader{
		instancesPerResource: instancesPerResource,
		loadNative: func(serialized []byte) (nativePyUDFResource, error) {
			return loadNativeResource(serialized)
		},
	}
}

func (loader *embeddedResourceLoader) Load(ctx context.Context, resource fileresource.ResolvedFileResource, stage string) (LoadedResource, error) {
	if ctx == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: load context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if loader == nil || loader.loadNative == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: native resource loader is nil")
	}

	request, err := NewLoadRequest(resource, stage, loader.instancesPerResource)
	if err != nil {
		return nil, err
	}
	serialized, err := MarshalLoadRequest(request)
	if err != nil {
		return nil, err
	}
	native, err := loader.loadNative(serialized)
	if err != nil {
		return nil, err
	}
	if native == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: native loader returned nil resource")
	}
	if err := ctx.Err(); err != nil {
		_ = native.Close()
		return nil, err
	}
	return &embeddedLoadedResource{
		resourceName: resource.Name,
		stage:        stage,
		native:       native,
	}, nil
}

func (resource *embeddedLoadedResource) Run(ctx context.Context, params *schemapb.FunctionParamObject, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	if ctx == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: run context is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if resource == nil || resource.native == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: loaded native resource is nil")
	}

	runParams, err := NewRunParams(resource.resourceName, resource.stage, params)
	if err != nil {
		return nil, err
	}
	serialized, err := MarshalRunParams(runParams)
	if err != nil {
		return nil, err
	}
	outputs, err := resource.native.run(inputs, serialized)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		for _, output := range outputs {
			if output != nil {
				output.Release()
			}
		}
		return nil, err
	}
	return outputs, nil
}

func (resource *embeddedLoadedResource) Close() error {
	if resource == nil {
		return nil
	}
	resource.closeOnce.Do(func() {
		if resource.native != nil {
			resource.closeErr = resource.native.Close()
			resource.native = nil
		}
	})
	return resource.closeErr
}
