/*
 *
 * Copyright 2017 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modified by github.com/milvus-io/milvus, @chyezh
 * - Only keep modified struct `PickerBuildInfo`, `PickerBuilder`,  remove unmodified struct.
 *
 */

package balancer

import (
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

type PickerBuildInfo struct {
	// ReadySCs is a map from all ready SubConns to the Addresses can be used.
	ReadySCs map[balancer.SubConn]base.SubConnInfo

	// UnReadySCs is a map from all unready SubConns to the Addresses can be used.
	UnReadySCs map[balancer.SubConn]base.SubConnInfo
}

// PickerBuilder creates balancer.Picker.
type PickerBuilder interface {
	// Build returns a picker that will be used by gRPC to pick a SubConn.
	Build(info PickerBuildInfo) balancer.Picker
}

// NewBalancerBuilder returns a base balancer builder configured by the provided config.
func NewBalancerBuilder(name string, pb PickerBuilder, config base.Config) balancer.Builder {
	return &baseBuilder{
		name:          name,
		pickerBuilder: pb,
		config:        config,
	}
}
