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

package format

import "github.com/milvus-io/milvus/pkg/v3/storagev2"

type LobFileInfo struct {
	Path          string // relative path to the LOB file
	FieldID       int64  // field ID this LOB file belongs to
	TotalRows     int64  // total number of rows in the LOB file
	ValidRows     int64  // number of valid (non-deleted) rows
	FileSizeBytes int64  // size of the LOB file in bytes
}
type NativeManifestFiles struct {
	ManifestFile string
	ColumnFiles  []string
	DeltaFiles   []string
	StatsFiles   []string
	LobFiles     []LobFileInfo
	Indexes      []storagev2.ManifestIndexInfo
}
