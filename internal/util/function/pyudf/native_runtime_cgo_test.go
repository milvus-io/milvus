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
	"archive/zip"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/proto/cgopb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestNativeRuntimeBuildCapability(t *testing.T) {
	capability := EmbeddedBuildCapability()
	if capability.Available {
		assert.Empty(t, capability.Reason)
	} else {
		assert.NotEmpty(t, capability.Reason)
	}
}

func TestNativeRuntimeDisabledBoundary(t *testing.T) {
	if EmbeddedBuildCapability().Available {
		t.Skip("requires MILVUS_ENABLE_PY_UDF=OFF")
	}

	err := initializeNativeRuntime()
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrSegcoreUnsupported)

	resource, err := loadNativeResource(nil)
	require.Error(t, err)
	assert.Nil(t, resource)
	assert.ErrorIs(t, err, merr.ErrSegcoreUnsupported)
}

func TestNativeLoadRequestWireValidation(t *testing.T) {
	if !EmbeddedBuildCapability().Available {
		t.Skip("requires MILVUS_ENABLE_PY_UDF=ON")
	}

	require.NoError(t, initializeNativeRuntime())
	require.NoError(t, initializeNativeRuntime())

	valid := &cgopb.PyUDFLoadRequest{
		ResourceName:  "rank_udf",
		ResourceId:    7,
		ResourcePath:  "/remote/rank_udf.whl",
		LocalPath:     "/missing/rank_udf.whl",
		Stage:         "L2_rerank",
		InstanceCount: 1,
	}
	tests := []struct {
		name    string
		request *cgopb.PyUDFLoadRequest
		match   string
	}{
		{name: "blank resource name", request: cloneLoadRequest(valid, func(request *cgopb.PyUDFLoadRequest) { request.ResourceName = "  " }), match: "blank or invalid UTF-8"},
		{name: "blank resource path", request: cloneLoadRequest(valid, func(request *cgopb.PyUDFLoadRequest) { request.ResourcePath = "" }), match: "blank or invalid UTF-8"},
		{name: "blank local path", request: cloneLoadRequest(valid, func(request *cgopb.PyUDFLoadRequest) { request.LocalPath = "\t" }), match: "blank or invalid UTF-8"},
		{name: "blank stage", request: cloneLoadRequest(valid, func(request *cgopb.PyUDFLoadRequest) { request.Stage = "\n" }), match: "blank or invalid UTF-8"},
		{name: "zero instances", request: cloneLoadRequest(valid, func(request *cgopb.PyUDFLoadRequest) { request.InstanceCount = 0 }), match: "instance_count must be positive"},
		{name: "negative instances", request: cloneLoadRequest(valid, func(request *cgopb.PyUDFLoadRequest) { request.InstanceCount = -1 }), match: "instance_count must be positive"},
		{name: "wrong extension", request: cloneLoadRequest(valid, func(request *cgopb.PyUDFLoadRequest) { request.LocalPath = "/missing/rank_udf.zip" }), match: "local wheel"},
		{name: "missing wheel", request: valid, match: "local wheel"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			serialized, err := MarshalLoadRequest(test.request)
			require.NoError(t, err)

			resource, err := loadNativeResource(serialized)
			require.Error(t, err)
			assert.Nil(t, resource)
			assert.ErrorIs(t, err, merr.ErrSegcore)
			assert.ErrorContains(t, err, test.match)
		})
	}

	t.Run("no protocol fields", func(t *testing.T) {
		// Encode resource_id=0 explicitly. Protobuf parses this successfully but
		// normalizes it to the default value, leaving no protocol fields.
		resource, err := loadNativeResource([]byte{0x10, 0x00})
		require.Error(t, err)
		assert.Nil(t, resource)
		assert.ErrorContains(t, err, "no protocol fields")
	})

	t.Run("empty request", func(t *testing.T) {
		resource, err := loadNativeResource(nil)
		require.Error(t, err)
		assert.Nil(t, resource)
		assert.ErrorContains(t, err, "serialized load request is empty")
	})

	t.Run("malformed protobuf", func(t *testing.T) {
		resource, err := loadNativeResource([]byte{0xff})
		require.Error(t, err)
		assert.Nil(t, resource)
		assert.ErrorContains(t, err, "serialized load request is malformed")
	})
}

func TestNativeLoadRequestFileOpenClassification(t *testing.T) {
	if !EmbeddedBuildCapability().Available {
		t.Skip("requires MILVUS_ENABLE_PY_UDF=ON")
	}

	require.NoError(t, initializeNativeRuntime())
	request, err := NewLoadRequest(fileresource.ResolvedFileResource{
		ID:        7,
		Name:      "rank_udf",
		Path:      "/remote/rank_udf.whl",
		LocalPath: "/missing/rank_udf.whl",
	}, "L2_rerank", 1)
	require.NoError(t, err)
	serialized, err := MarshalLoadRequest(request)
	require.NoError(t, err)

	resource, err := loadNativeResource(serialized)
	require.Error(t, err)
	assert.Nil(t, resource)
	assert.ErrorIs(t, err, merr.ErrSegcore)
	assert.ErrorContains(t, err, "segcoreCode=2012")
	assert.True(t, merr.IsRetryableErr(err))
}

func TestNativeLoadResourceRoundTrip(t *testing.T) {
	if !EmbeddedBuildCapability().Available {
		t.Skip("requires MILVUS_ENABLE_PY_UDF=ON")
	}

	require.NoError(t, initializeNativeRuntime())
	wheelPath := writeNativeRuntimeTestWheel(t, "native_round_trip", `
class UDF:
    def transform(self, params, columns):
        raise AssertionError("Slice 9 must not run transform")
    def close(self):
        return None

def create_udf(context):
    assert context.resource_name == "native_round_trip"
    assert context.stage == "L2_rerank"
    return UDF()
`)
	resource, err := loadNativeRuntimeTestResource("native_round_trip", wheelPath, 2)
	require.NoError(t, err)
	require.NotNil(t, resource)
	require.NoError(t, resource.Close())
	require.NoError(t, resource.Close())
}

func TestNativeLoadResourceReportsFactoryFailure(t *testing.T) {
	if !EmbeddedBuildCapability().Available {
		t.Skip("requires MILVUS_ENABLE_PY_UDF=ON")
	}

	require.NoError(t, initializeNativeRuntime())
	wheelPath := writeNativeRuntimeTestWheel(t, "native_factory_failure", `
def create_udf(context):
    raise ValueError("native factory boom")
`)
	resource, err := loadNativeRuntimeTestResource("native_factory_failure", wheelPath, 1)
	require.Error(t, err)
	assert.Nil(t, resource)
	assert.ErrorIs(t, err, merr.ErrFunctionFailed)
	assert.ErrorContains(t, err, "native factory boom")
	assert.ErrorContains(t, err, "Python UDF load failed")
}

func TestNativeLoadResourceReportsCloseFailure(t *testing.T) {
	if !EmbeddedBuildCapability().Available {
		t.Skip("requires MILVUS_ENABLE_PY_UDF=ON")
	}

	require.NoError(t, initializeNativeRuntime())
	wheelPath := writeNativeRuntimeTestWheel(t, "native_close_failure", `
class UDF:
    def transform(self, params, columns):
        return None
    def close(self):
        raise RuntimeError("native close boom")

def create_udf(context):
    return UDF()
`)
	resource, err := loadNativeRuntimeTestResource("native_close_failure", wheelPath, 1)
	require.NoError(t, err)
	require.NotNil(t, resource)

	err = resource.Close()
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrFunctionFailed)
	assert.ErrorContains(t, err, "native close boom")
	assert.ErrorContains(t, err, "Python resource close failed")
	assert.Nil(t, resource.handle)
	require.NoError(t, resource.Close())
}

func cloneLoadRequest(request *cgopb.PyUDFLoadRequest, mutate func(*cgopb.PyUDFLoadRequest)) *cgopb.PyUDFLoadRequest {
	cloned := proto.Clone(request).(*cgopb.PyUDFLoadRequest)
	mutate(cloned)
	return cloned
}

func loadNativeRuntimeTestResource(resourceName, wheelPath string, instanceCount int32) (*nativeResource, error) {
	request, err := NewLoadRequest(fileresource.ResolvedFileResource{
		ID:        11,
		Name:      resourceName,
		Path:      "/remote/" + filepath.Base(wheelPath),
		LocalPath: wheelPath,
	}, "L2_rerank", instanceCount)
	if err != nil {
		return nil, err
	}
	serialized, err := MarshalLoadRequest(request)
	if err != nil {
		return nil, err
	}
	return loadNativeResource(serialized)
}

func writeNativeRuntimeTestWheel(t *testing.T, packageName, module string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), packageName+"-0.0.1-py3-none-any.whl")
	file, err := os.Create(path)
	require.NoError(t, err)
	writer := zip.NewWriter(file)
	writeFile := func(name, content string) {
		entry, createErr := writer.Create(name)
		require.NoError(t, createErr)
		_, writeErr := entry.Write([]byte(content))
		require.NoError(t, writeErr)
	}
	writeFile(packageName+"/__init__.py", module)
	writeFile(packageName+"-0.0.1.dist-info/entry_points.txt", "[milvus.pudf]\nmain = "+packageName+":create_udf\n")
	writeFile(packageName+"-0.0.1.dist-info/METADATA", "Metadata-Version: 2.1\nName: "+packageName+"\nVersion: 0.0.1\n")
	require.NoError(t, writer.Close())
	require.NoError(t, file.Close())
	return path
}
