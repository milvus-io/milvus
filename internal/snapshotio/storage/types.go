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

package storage

import (
	milvusstorage "github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

type Direction int

const (
	DirectionExport Direction = iota
	DirectionRestore
	DirectionCopySource
)

type ResolvedForeignStorage struct {
	ForeignBucket        string
	ForeignCM            milvusstorage.ChunkManager
	ForeignStorageConfig *indexpb.StorageConfig
	Copier               milvusstorage.CrossBucketCopier
}

const unsupportedServerSideCopyMessage = "server-side cross-bucket copy is unsupported for this provider/endpoint pair; cross-service copy would require streaming (unsupported)"
