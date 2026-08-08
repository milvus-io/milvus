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

//go:build cgo

package pyudf

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/arrow/memory/mallocator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/fileresource"
)

func TestProductionRuntimeTransformQuery(t *testing.T) {
	if !EmbeddedBuildCapability().Available {
		t.Skip("requires MILVUS_ENABLE_PY_UDF=ON")
	}

	wheel := writeNativeRuntimeTestWheel(t, "production_runtime_transform", `
import pyarrow as pa

class UDF:
    def transform_query(self, params, columns):
        values = [value.as_py() * params["factor"] for value in columns[0]]
        return [pa.array(values, type=pa.int64()), columns[0]]

def create_udf(context):
    return UDF()
`)
	runtime, err := NewProductionRuntime(context.Background(), Config{
		Enabled:              true,
		LoadTimeout:          time.Second,
		ExecutorThreads:      1,
		MaxQueueSize:         64,
		InstancesPerResource: 1,
	})
	require.NoError(t, err)
	defer runtime.Close()

	resource := &fileresource.ResolvedFileResource{
		ID:        41,
		Name:      "production_runtime_transform",
		Path:      "/remote/" + filepath.Base(wheel),
		LocalPath: wheel,
	}
	require.NoError(t, runtime.OnFileResourceSync(fileresource.SyncEvent{
		Version:   1,
		Resources: []*fileresource.ResolvedFileResource{resource},
	}))
	lease, err := runtime.Acquire(context.Background(), resource.Name, "L2_rerank")
	require.NoError(t, err)
	defer lease.Release()

	allocator := memory.NewCheckedAllocator(mallocator.NewMallocator())
	defer allocator.AssertSize(t, 0)
	inputArray := newPyUDFTestInt64Values(allocator, []int64{2, 3}, nil)
	input := newPyUDFTestChunked(arrow.PrimitiveTypes.Int64, inputArray)
	inputArray.Release()
	outputs, err := lease.Run(context.Background(), &schemapb.FunctionParamObject{Fields: map[string]*schemapb.FunctionParamValue{
		"factor": intParamValue(5),
	}}, []*arrow.Chunked{input})
	require.NoError(t, err)
	input.Release()
	require.Len(t, outputs, 2)
	assert.Equal(t, "10", outputs[0].Chunk(0).ValueStr(0))
	assert.Equal(t, "15", outputs[0].Chunk(0).ValueStr(1))
	assert.Equal(t, "2", outputs[1].Chunk(0).ValueStr(0))
	releasePyUDFChunked(outputs)
}
