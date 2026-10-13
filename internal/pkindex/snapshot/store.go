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

package snapshot

import "context"

// TableStore moves SST files between local disk and object storage.
type TableStore interface {
	// UploadFile writes the local file at path to key.
	UploadFile(ctx context.Context, key, path string) error

	// DownloadFile writes the object at key to the local file at path. It
	// writes a temporary file in the same directory and renames it, so path
	// either does not exist or holds the whole object.
	//
	// A missing object returns an error marked pkerr.ErrCorrupted. A manifest
	// references the table, so a table that is gone is data corruption and
	// not an IO failure that a retry can clear.
	DownloadFile(ctx context.Context, key, path string) error
}
