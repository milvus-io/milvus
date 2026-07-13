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
	"context"

	"golang.org/x/exp/mmap"

	"github.com/milvus-io/milvus/internal/storageprofile"
)

type attributedChunkManager struct {
	inner       ChunkManager
	attribution storageprofile.Attribution
}

func WithAttribution(inner ChunkManager, attribution storageprofile.Attribution) ChunkManager {
	if inner == nil {
		return nil
	}
	return &attributedChunkManager{inner: inner, attribution: attribution.Bounded()}
}

func (m *attributedChunkManager) context(ctx context.Context) context.Context {
	return storageprofile.WithDefaultAttribution(ctx, m.attribution)
}

func (m *attributedChunkManager) RootPath() string { return m.inner.RootPath() }
func (m *attributedChunkManager) Path(ctx context.Context, path string) (string, error) {
	return m.inner.Path(m.context(ctx), path)
}
func (m *attributedChunkManager) Size(ctx context.Context, path string) (int64, error) {
	return m.inner.Size(m.context(ctx), path)
}
func (m *attributedChunkManager) Write(ctx context.Context, path string, content []byte) error {
	return m.inner.Write(m.context(ctx), path, content)
}
func (m *attributedChunkManager) MultiWrite(ctx context.Context, contents map[string][]byte) error {
	return m.inner.MultiWrite(m.context(ctx), contents)
}
func (m *attributedChunkManager) Exist(ctx context.Context, path string) (bool, error) {
	return m.inner.Exist(m.context(ctx), path)
}
func (m *attributedChunkManager) Read(ctx context.Context, path string) ([]byte, error) {
	return m.inner.Read(m.context(ctx), path)
}
func (m *attributedChunkManager) Reader(ctx context.Context, path string) (FileReader, error) {
	return m.inner.Reader(m.context(ctx), path)
}
func (m *attributedChunkManager) MultiRead(ctx context.Context, paths []string) ([][]byte, error) {
	return m.inner.MultiRead(m.context(ctx), paths)
}
func (m *attributedChunkManager) WalkWithPrefix(ctx context.Context, prefix string, recursive bool, walkFunc ChunkObjectWalkFunc) error {
	return m.inner.WalkWithPrefix(m.context(ctx), prefix, recursive, walkFunc)
}
func (m *attributedChunkManager) Mmap(ctx context.Context, path string) (*mmap.ReaderAt, error) {
	return m.inner.Mmap(m.context(ctx), path)
}
func (m *attributedChunkManager) ReadAt(ctx context.Context, path string, offset, length int64) ([]byte, error) {
	return m.inner.ReadAt(m.context(ctx), path, offset, length)
}
func (m *attributedChunkManager) Remove(ctx context.Context, path string) error {
	return m.inner.Remove(m.context(ctx), path)
}
func (m *attributedChunkManager) MultiRemove(ctx context.Context, paths []string) error {
	return m.inner.MultiRemove(m.context(ctx), paths)
}
func (m *attributedChunkManager) RemoveWithPrefix(ctx context.Context, prefix string) error {
	return m.inner.RemoveWithPrefix(m.context(ctx), prefix)
}
func (m *attributedChunkManager) Copy(ctx context.Context, source, destination string) error {
	return m.inner.Copy(m.context(ctx), source, destination)
}
