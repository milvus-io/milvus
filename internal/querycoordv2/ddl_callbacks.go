// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querycoordv2

import "github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"

// RegisterDDLCallbacks registers the ddl callbacks.
func RegisterDDLCallbacks(s *Server) {
	ddlCallback := &DDLCallbacks{
		Server: s,
	}
	ddlCallback.registerLoadConfigCallbacks()
	ddlCallback.registerResourceGroupCallbacks()
}

type DDLCallbacks struct {
	*Server
}

// registerLoadConfigCallbacks registers the load config callbacks.
func (c *DDLCallbacks) registerLoadConfigCallbacks() {
	registry.RegisterAlterLoadConfigV2AckCallback(c.alterLoadConfigV2AckCallback)
	registry.RegisterDropLoadConfigV2AckCallback(c.dropLoadConfigV2AckCallback)
}

func (c *DDLCallbacks) registerResourceGroupCallbacks() {
	registry.RegisterAlterResourceGroupV2AckCallback(c.alterResourceGroupV2AckCallback)
	registry.RegisterDropResourceGroupV2AckCallback(c.dropResourceGroupV2AckCallback)
}
