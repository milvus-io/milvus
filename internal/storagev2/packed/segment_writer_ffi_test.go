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

package packed

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/stretchr/testify/assert"
)

// TestSegmentWriterPropertiesMultiPartUploadSize guards the P1 regression for
// the TEXT writer path (FFISegmentWriter).
// Why: the pinned milvus-storage consumes the fs-scoped key
// (PROPERTY_FS_MULTI_PART_UPLOAD_SIZE) in both the filesystem producers and
// the Parquet writer. Not a "writer.*" key because undefined property keys
// are silently ignored, so a writer-scoped key would leave uploads on the
// 10 MiB default without any error.
func TestSegmentWriterPropertiesMultiPartUploadSize(t *testing.T) {
	assert.Equal(t, "fs.multi_part_upload_size", PropertyFSMultiPartUploadSize)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "f0", Type: arrow.PrimitiveTypes.Int64},
	}, nil)

	extra := segmentWriterProperties(schema, &SegmentWriterConfig{
		MultiPartUploadSize: 209715200, // 200 MiB
	})
	assert.Equal(t, "209715200", extra[PropertyFSMultiPartUploadSize])

	// Non-positive size means unset: the key must be absent so the storage
	// library falls back to its own default.
	extra = segmentWriterProperties(schema, &SegmentWriterConfig{})
	assert.NotContains(t, extra, PropertyFSMultiPartUploadSize)

	extra = segmentWriterProperties(schema, nil)
	assert.NotContains(t, extra, PropertyFSMultiPartUploadSize)
}
