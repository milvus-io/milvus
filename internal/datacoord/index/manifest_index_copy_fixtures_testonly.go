// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build test

package index

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
)

func MigrationServerCopyFixture(t *testing.T, root string) storage.ChunkManager {
	t.Helper()
	local := storage.NewLocalChunkManager(objectstorage.RootPath(root))
	fixture, err := os.OpenRoot(root)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, fixture.Close()) })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dst := strings.TrimPrefix(r.URL.Path, "/fixture/")
		if !strings.HasPrefix(dst, root+"/") {
			http.Error(w, "outside fixture", 400)
			return
		}
		dst = strings.TrimPrefix(dst, root+"/")
		if r.Method == http.MethodHead {
			info, err := fixture.Stat(dst)
			if err != nil {
				http.Error(w, "missing", 404)
				return
			}
			w.Header().Set("Content-Length", fmt.Sprint(info.Size()))
			w.Header().Set("ETag", `"fixture"`)
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			return
		}
		if r.Method != http.MethodPut || r.Header.Get("X-Amz-Copy-Source") == "" {
			t.Errorf("artifact migration used non-copy request: %s %s", r.Method, r.URL.Path)
			http.Error(w, "only CopyObject is supported", 400)
			return
		}
		src, err := url.PathUnescape(strings.TrimPrefix(strings.TrimPrefix(r.Header.Get("X-Amz-Copy-Source"), "/"), "fixture/"))
		if err != nil || !strings.HasPrefix(src, root+"/") {
			http.Error(w, "invalid source", 400)
			return
		}
		data, err := fixture.ReadFile(strings.TrimPrefix(src, root+"/"))
		if err != nil {
			http.Error(w, "missing source", 404)
			return
		}
		if err := fixture.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
			t.Error(err)
			http.Error(w, "mkdir", 500)
			return
		}
		if err := fixture.WriteFile(dst, data, 0o600); err != nil {
			t.Error(err)
			http.Error(w, "write", 500)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprint(w, `<CopyObjectResult><LastModified>2026-10-19T00:00:00.000Z</LastModified><ETag>"fixture"</ETag></CopyObjectResult>`)
	}))
	t.Cleanup(server.Close)
	client, err := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{Region: "us-east-1", BucketLookup: minio.BucketLookupPath})
	require.NoError(t, err)
	remote := storage.NewRemoteChunkManagerForTesting(client, "fixture", root)
	return migrationCopyManager{ChunkManager: local, copy: remote.Copy}
}

type migrationCopyManager struct {
	storage.ChunkManager
	copy func(context.Context, string, string) error
}

func (m migrationCopyManager) Copy(ctx context.Context, src, dst string) error {
	return m.copy(ctx, src, dst)
}
