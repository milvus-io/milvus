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
	"sync/atomic"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/proto/cgopb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type fakeNativePyUDFResource struct {
	runFunc    func([]*arrow.Chunked, []byte) ([]*arrow.Chunked, error)
	closeCount atomic.Int64
}

func (resource *fakeNativePyUDFResource) run(inputs []*arrow.Chunked, serializedParams []byte) ([]*arrow.Chunked, error) {
	if resource.runFunc == nil {
		return []*arrow.Chunked{}, nil
	}
	return resource.runFunc(inputs, serializedParams)
}

func (resource *fakeNativePyUDFResource) Close() error {
	resource.closeCount.Add(1)
	return nil
}

func TestEmbeddedResourceLoader(t *testing.T) {
	resolved := fileresource.ResolvedFileResource{
		ID:        7,
		Name:      "rank_udf",
		Path:      "/remote/rank_udf.whl",
		LocalPath: "/local/rank_udf.whl",
	}

	t.Run("builds load request", func(t *testing.T) {
		native := &fakeNativePyUDFResource{}
		loader := &embeddedResourceLoader{
			instancesPerResource: 3,
			loadNative: func(serialized []byte) (nativePyUDFResource, error) {
				request := &cgopb.PyUDFLoadRequest{}
				require.NoError(t, proto.Unmarshal(serialized, request))
				assert.Equal(t, resolved.Name, request.GetResourceName())
				assert.Equal(t, resolved.ID, request.GetResourceId())
				assert.Equal(t, resolved.Path, request.GetResourcePath())
				assert.Equal(t, resolved.LocalPath, request.GetLocalPath())
				assert.Equal(t, "L2_rerank", request.GetStage())
				assert.Equal(t, int32(3), request.GetInstanceCount())
				return native, nil
			},
		}

		loaded, err := loader.Load(context.Background(), resolved, "L2_rerank")
		require.NoError(t, err)
		require.NotNil(t, loaded)
		require.NoError(t, loaded.Close())
		require.NoError(t, loaded.Close())
		assert.Equal(t, int64(1), native.closeCount.Load())
	})

	t.Run("pre-canceled context skips native load", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var calls atomic.Int64
		loader := &embeddedResourceLoader{
			instancesPerResource: 1,
			loadNative: func([]byte) (nativePyUDFResource, error) {
				calls.Add(1)
				return &fakeNativePyUDFResource{}, nil
			},
		}
		loaded, err := loader.Load(ctx, resolved, "L2_rerank")
		assert.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, loaded)
		assert.Zero(t, calls.Load())
	})

	t.Run("post-load cancellation closes native resource", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		native := &fakeNativePyUDFResource{}
		loader := &embeddedResourceLoader{
			instancesPerResource: 1,
			loadNative: func([]byte) (nativePyUDFResource, error) {
				cancel()
				return native, nil
			},
		}
		loaded, err := loader.Load(ctx, resolved, "L2_rerank")
		assert.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, loaded)
		assert.Equal(t, int64(1), native.closeCount.Load())
	})
}

func TestEmbeddedLoadedResourceRun(t *testing.T) {
	t.Run("serializes run params", func(t *testing.T) {
		params := &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{
			"factor": intParamValue(4),
		}}
		native := &fakeNativePyUDFResource{runFunc: func(inputs []*arrow.Chunked, serialized []byte) ([]*arrow.Chunked, error) {
			assert.Empty(t, inputs)
			runParams := &cgopb.PyUDFRunParams{}
			require.NoError(t, proto.Unmarshal(serialized, runParams))
			assert.Equal(t, "rank_udf", runParams.GetResourceName())
			assert.Equal(t, "L2_rerank", runParams.GetStage())
			assert.True(t, proto.Equal(params, runParams.GetUdfParams()))
			return []*arrow.Chunked{}, nil
		}}
		resource := &embeddedLoadedResource{
			resourceName: "rank_udf",
			stage:        "L2_rerank",
			native:       native,
		}

		outputs, err := resource.Run(context.Background(), params, nil)
		require.NoError(t, err)
		assert.Empty(t, outputs)
	})

	t.Run("post-run cancellation releases outputs", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		allocator := memory.NewCheckedAllocator(memory.DefaultAllocator)
		builder := array.NewInt64Builder(allocator)
		builder.Append(1)
		value := builder.NewArray()
		builder.Release()
		output := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{value})
		value.Release()

		native := &fakeNativePyUDFResource{runFunc: func([]*arrow.Chunked, []byte) ([]*arrow.Chunked, error) {
			cancel()
			return []*arrow.Chunked{output}, nil
		}}
		resource := &embeddedLoadedResource{
			resourceName: "rank_udf",
			stage:        "L2_rerank",
			native:       native,
		}
		outputs, err := resource.Run(ctx, nil, nil)
		assert.ErrorIs(t, err, context.Canceled)
		assert.Nil(t, outputs)
		allocator.AssertSize(t, 0)
	})

	t.Run("invalid user params remain parameter invalid", func(t *testing.T) {
		resource := &embeddedLoadedResource{
			resourceName: "rank_udf",
			stage:        "L2_rerank",
			native:       &fakeNativePyUDFResource{},
		}
		_, err := resource.Run(context.Background(), &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{
			"invalid": {},
		}}, nil)
		assert.ErrorIs(t, err, merr.ErrParameterInvalid)
	})
}
