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

package lance

import (
	"context"
	"os"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/importutilv2/parquet"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// innerReader is the minimal subset of the Parquet reader that the Lance
// reader needs to delegate to after Lance-to-Parquet conversion.
type innerReader interface {
	Size() (int64, error)
	Read() (*storage.InsertData, error)
	Close()
}

type reader struct {
	ctx        context.Context
	cm         storage.ChunkManager
	schema     *schemapb.CollectionSchema
	bufferSize int
	version    uint64

	inner    innerReader
	tempPath string
}

// NewReader opens a Lance snapshot at the given path and version, converts it
// to a temporary Parquet file, and returns a reader that delegates to the
// existing Parquet import path.
func NewReader(
	ctx context.Context,
	cm storage.ChunkManager,
	schema *schemapb.CollectionSchema,
	path string,
	version uint64,
	bufferSize int,
) (*reader, error) {
	tempPath, err := convertLanceToParquet(ctx, path, version)
	if err != nil {
		return nil, err
	}

	pqReader, err := parquet.NewReader(ctx, cm, schema, tempPath, bufferSize)
	if err != nil {
		os.Remove(tempPath)
		return nil, err
	}

	return &reader{
		ctx:        ctx,
		cm:         cm,
		schema:     schema,
		bufferSize: bufferSize,
		version:    version,
		inner:      pqReader,
		tempPath:   tempPath,
	}, nil
}

// convertLanceToParquet reads a Lance snapshot at the given version and
// materializes it into a temporary Parquet file. It returns the temp file path.
//
// The Lance file format requires the Lance client library to read. The
// conversion is intentionally a thin adapter: read the fixed Lance snapshot,
// materialize it into a temporary Parquet representation, and hand it off to
// the existing Parquet import path.
//
// NOTE: In production the import chunk manager may be a RemoteChunkManager that
// resolves paths as object keys in a storage bucket. A local filesystem temp
// path returned here cannot be read back through parquet.NewReader in that case.
// When the Lance client dependency is added and the conversion is implemented,
// the temp Parquet must be written into the chunk manager (e.g. via cm) or read
// through a local FileReader/ChunkManager rather than passed as a local path.
//
// TODO: Implement Lance reading using a Lance Go client (e.g.
// github.com/lancedb/lance). The Lance client is not yet available as a Go
// module dependency in this repo, and requires CGO/Rust tooling to build.
// Once the dependency is added, this function should:
//  1. Open the Lance dataset at `path` pinned to `version`.
//  2. Read the Lance table into Arrow record batches.
//  3. Write the record batches to a temporary Parquet file.
//  4. Return the temp file path.
//
// Until then, return a clear error so import fails fast instead of
// silently producing no data.
func convertLanceToParquet(ctx context.Context, path string, version uint64) (string, error) {
	tempFile, err := os.CreateTemp("", "lance-import-*.parquet")
	if err != nil {
		return "", err
	}
	tempPath := tempFile.Name()
	tempFile.Close()
	// The conversion is not yet implemented; remove the empty temp file so
	// a failed import attempt does not leak a file into /tmp.
	os.Remove(tempPath)
	return tempPath, merr.WrapErrImportSysFailedMsg(
		"lance reader requires Lance Go client dependency to open %s@%d", path, version)
}

func (r *reader) Size() (int64, error) {
	if r.inner != nil {
		return r.inner.Size()
	}
	return 0, nil
}

func (r *reader) Read() (*storage.InsertData, error) {
	if r.inner != nil {
		return r.inner.Read()
	}
	return nil, nil
}

func (r *reader) Close() {
	if r.inner != nil {
		r.inner.Close()
	}
	if r.tempPath != "" {
		os.Remove(r.tempPath)
	}
}
