// Copyright 2023 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package packed

const (
	// ColumnGroupSizeThreshold is the threshold of column group size per row.
	ColumnGroupSizeThreshold = 1024 // 1KB
	// DefaultBufferSize is the default buffer size for writing data to storage.
	DefaultWriteBufferSize = 32 * 1024 * 1024 // 32MB
	// DefaultMultiPartUploadSize is the default size of each part of a multipart upload.
	DefaultMultiPartUploadSize = 10 * 1024 * 1024 // 10MB
)
