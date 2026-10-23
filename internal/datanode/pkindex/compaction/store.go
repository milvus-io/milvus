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

package compaction

import "context"

// Store moves SST files between the local disk and object storage. The
// object store of package internal/pkindex/objstore implements it.
type Store interface {
	// UploadFile writes the local file at path to key.
	UploadFile(ctx context.Context, key, path string) error
	// DownloadFile writes the object at key to the local file at path.
	DownloadFile(ctx context.Context, key, path string) error
}
