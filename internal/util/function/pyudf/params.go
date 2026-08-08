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
	"fmt"
	"strings"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/fileresource"
	"github.com/milvus-io/milvus/pkg/v3/proto/cgopb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const maxParamNestingDepth = 64

var deterministicMarshal = proto.MarshalOptions{Deterministic: true}

// NewLoadRequest builds the native control request for one resolved PyUDF resource.
func NewLoadRequest(resource fileresource.ResolvedFileResource, stage string, instanceCount int32) (*cgopb.PyUDFLoadRequest, error) {
	if strings.TrimSpace(resource.Name) == "" {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: load resource name is empty")
	}
	if !utf8.ValidString(resource.Name) {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: load resource name is not valid UTF-8")
	}
	if resource.Path == "" {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: load resource path is empty for %q", resource.Name)
	}
	if !utf8.ValidString(resource.Path) {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: load resource path is not valid UTF-8 for %q", resource.Name)
	}
	if resource.LocalPath == "" {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: load local path is empty for %q", resource.Name)
	}
	if !utf8.ValidString(resource.LocalPath) {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: load local path is not valid UTF-8 for %q", resource.Name)
	}
	stage = strings.TrimSpace(stage)
	if stage == "" {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: load stage is empty for %q", resource.Name)
	}
	if !utf8.ValidString(stage) {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: load stage is not valid UTF-8 for %q", resource.Name)
	}
	if instanceCount <= 0 {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: instance count must be positive for %q", resource.Name)
	}

	return &cgopb.PyUDFLoadRequest{
		ResourceName:  resource.Name,
		ResourceId:    resource.ID,
		ResourcePath:  resource.Path,
		LocalPath:     resource.LocalPath,
		Stage:         stage,
		InstanceCount: instanceCount,
	}, nil
}

// NewRunParams builds the native control parameters for one PyUDF invocation.
func NewRunParams(resourceName, stage string, params *schemapb.FunctionParamObject) (*cgopb.PyUDFRunParams, error) {
	resourceName = strings.TrimSpace(resourceName)
	if resourceName == "" {
		return nil, merr.WrapErrParameterInvalidMsg("py_udf: resource_name is empty")
	}
	if !utf8.ValidString(resourceName) {
		return nil, merr.WrapErrParameterInvalidMsg("py_udf: resource_name is not valid UTF-8")
	}
	stage = strings.TrimSpace(stage)
	if stage == "" {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: run stage is empty for resource %q", resourceName)
	}
	if !utf8.ValidString(stage) {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: run stage is not valid UTF-8 for resource %q", resourceName)
	}

	if params == nil {
		params = &schemapb.FunctionParamObject{}
	}
	if err := validateParamObject(params, "udf_params", 0); err != nil {
		return nil, err
	}
	return &cgopb.PyUDFRunParams{
		ResourceName: resourceName,
		Stage:        stage,
		UdfParams:    proto.Clone(params).(*schemapb.FunctionParamObject),
	}, nil
}

// MarshalLoadRequest serializes a PyUDF load request for the native boundary.
func MarshalLoadRequest(request *cgopb.PyUDFLoadRequest) ([]byte, error) {
	if request == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: load request is nil")
	}
	data, err := deterministicMarshal.Marshal(request)
	if err != nil {
		return nil, merr.Wrap(err, "py_udf: marshal load request")
	}
	return data, nil
}

// MarshalRunParams serializes PyUDF invocation parameters for the native boundary.
func MarshalRunParams(params *cgopb.PyUDFRunParams) ([]byte, error) {
	if params == nil {
		return nil, merr.WrapErrServiceInternalMsg("py_udf: run params are nil")
	}
	data, err := deterministicMarshal.Marshal(params)
	if err != nil {
		return nil, merr.Wrap(err, "py_udf: marshal run params")
	}
	return data, nil
}

func validateParamObject(object *schemapb.FunctionParamObject, path string, depth int) error {
	if depth > maxParamNestingDepth {
		return paramDepthError(path)
	}
	if object == nil {
		return merr.WrapErrParameterInvalidMsg("py_udf: parameter %s contains a nil object", path)
	}

	for key, value := range object.GetFields() {
		if !utf8.ValidString(key) {
			return merr.WrapErrParameterInvalidMsg("py_udf: parameter %s contains an object key that is not valid UTF-8", path)
		}
		if err := validateParamValue(value, fmt.Sprintf("%s[%q]", path, key), depth+1); err != nil {
			return err
		}
	}
	return nil
}

func validateParamArray(array *schemapb.FunctionParamArray, path string, depth int) error {
	if depth > maxParamNestingDepth {
		return paramDepthError(path)
	}
	if array == nil {
		return merr.WrapErrParameterInvalidMsg("py_udf: parameter %s contains a nil array", path)
	}

	for index, value := range array.GetValues() {
		if err := validateParamValue(value, fmt.Sprintf("%s[%d]", path, index), depth+1); err != nil {
			return err
		}
	}
	return nil
}

func validateParamValue(value *schemapb.FunctionParamValue, path string, depth int) error {
	if depth > maxParamNestingDepth {
		return paramDepthError(path)
	}
	if value == nil || value.GetValue() == nil {
		return merr.WrapErrParameterInvalidMsg("py_udf: parameter %s is unset", path)
	}

	switch typed := value.GetValue().(type) {
	case *schemapb.FunctionParamValue_BoolValue,
		*schemapb.FunctionParamValue_Int64Value,
		*schemapb.FunctionParamValue_DoubleValue:
		return nil
	case *schemapb.FunctionParamValue_StringValue:
		if !utf8.ValidString(typed.StringValue) {
			return merr.WrapErrParameterInvalidMsg("py_udf: parameter %s is not valid UTF-8", path)
		}
		return nil
	case *schemapb.FunctionParamValue_BytesValue:
		return nil
	case *schemapb.FunctionParamValue_ArrayValue:
		return validateParamArray(typed.ArrayValue, path, depth)
	case *schemapb.FunctionParamValue_ObjectValue:
		return validateParamObject(typed.ObjectValue, path, depth)
	default:
		return merr.WrapErrParameterInvalidMsg("py_udf: parameter %s has an unsupported value type", path)
	}
}

func paramDepthError(path string) error {
	return merr.WrapErrParameterInvalidMsg(
		"py_udf: parameter %s exceeds the maximum nesting depth of %d",
		path,
		maxParamNestingDepth,
	)
}
