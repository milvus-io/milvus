// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package metricsinfo

import (
	"encoding/json"
)

// ComponentInfos defines the interface of all component infos
type ComponentInfos interface {
}

// MarshalComponentInfos returns the json string of ComponentInfos
func MarshalComponentInfos(infos ComponentInfos) (string, error) {
	binary, err := json.Marshal(infos)
	return string(binary), err
}

// UnmarshalComponentInfos constructs a ComponentInfos object using a json string
func UnmarshalComponentInfos(s string, infos ComponentInfos) error {
	return json.Unmarshal([]byte(s), infos)
}

// BaseComponentInfos contains basic information that all components should have.
type BaseComponentInfos struct {
	HasError    bool   `json:"has_error"`
	ErrorReason string `json:"error_reason"`
	Name        string `json:"name"`
	// TODO(dragondriver): more required information
}

// QueryNodeInfos implements ComponentInfos
type QueryNodeInfos struct {
	BaseComponentInfos
	// TODO(dragondriver): add more detail metrics
}

// QueryCoordInfos implements ComponentInfos
type QueryCoordInfos struct {
	BaseComponentInfos
	// TODO(dragondriver): add more detail metrics
}

// ProxyInfos implements ComponentInfos
type ProxyInfos struct {
	BaseComponentInfos
	// TODO(dragondriver): add more detail metrics
}

// IndexNodeInfos implements ComponentInfos
type IndexNodeInfos struct {
	BaseComponentInfos
	// TODO(dragondriver): add more detail metrics
}

// IndexCoordInfos implements ComponentInfos
type IndexCoordInfos struct {
	BaseComponentInfos
	// TODO(dragondriver): add more detail metrics
}
