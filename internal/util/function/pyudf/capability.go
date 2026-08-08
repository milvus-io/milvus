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

import "github.com/milvus-io/milvus/pkg/v3/util/merr"

// BuildCapability describes whether this binary includes the embedded PyUDF runtime.
type BuildCapability struct {
	Available bool
	Reason    string
}

// EmbeddedBuildCapability reports the embedded PyUDF capability of this binary.
func EmbeddedBuildCapability() BuildCapability {
	return embeddedBuildCapability()
}

// ValidateConfigCapability verifies that enabled configuration is supported by the binary.
func ValidateConfigCapability(config Config, capability BuildCapability) error {
	if !config.Enabled || capability.Available {
		return nil
	}
	if capability.Reason == "" {
		capability.Reason = "embedded PyUDF runtime is unavailable"
	}
	return merr.WrapErrServiceInternalMsg(
		"py_udf: function.pyUDF.enabled is true, but this binary does not support embedded PyUDF: %s",
		capability.Reason,
	)
}
