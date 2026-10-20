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

package datacoord

import (
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/registry"
)

// RegisterDDLCallbacks registers the ddl callbacks of s.ddl. It builds s.ddl from s first when s.ddl is not set.
func RegisterDDLCallbacks(s *Server) {
	if s.ddl == nil {
		s.ddl = newDDLCallbacks(s)
	}
	ddlCallback := s.ddl
	ddlCallback.registerIndexCallbacks()
	registry.RegisterFlushAllV2AckCallback(ddlCallback.flushAllV2AckCallback)
	ddlCallback.registerSnapshotCallbacks()
	ddlCallback.registerExternalCollectionCallbacks()
	ddlCallback.registerImportCallbacks()
	ddlCallback.registerBatchUpdateManifestCallbacks()
}

// newDDLCallbacks builds the DDL callbacks from the current fields of s.
// Later changes to s fields are not seen; rebuild after changing them.
func newDDLCallbacks(s *Server) *DDLCallbacks {
	return &DDLCallbacks{DDLDeps: DDLDeps{
		Meta:                             s.meta,
		ImportMeta:                       s.importMeta,
		IndexMeta:                        s.indexMeta,
		SnapshotMeta:                     s.snapshotMeta,
		SnapshotManager:                  s.snapshotManager,
		ExternalCollectionRefreshManager: s.externalCollectionRefreshManager,
		Broker:                           s.broker,
		NotifyIndexChan:                  s.notifyIndexChan,
		CreateImportJobFromAck:           s.createImportJobFromAck,
	}}
}
