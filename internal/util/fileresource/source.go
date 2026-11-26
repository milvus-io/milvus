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

package fileresource

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// RestrictSource applies the operator boundary to new resource admission.
// Local paths may be relative to the allowed directory; object keys stay complete.
func RestrictSource(cm storage.ChunkManager) *restrictedSource {
	root := paramtable.Get().CommonCfg.FileResourceAllowedPath.GetValue()
	if root == "" {
		if _, local := cm.(*storage.LocalChunkManager); local {
			root = filepath.Join(paramtable.Get().LocalStorageCfg.Path.GetValue(), "file_resource")
		} else {
			root = path.Join(cm.RootPath(), "file_resource")
		}
	}
	return &restrictedSource{ChunkManager: cm, root: strings.TrimRight(root, "/")}
}

type restrictedSource struct {
	storage.ChunkManager
	root string
}

func (s *restrictedSource) relativePath(name string) (string, error) {
	if s.root == "" {
		return "", merr.WrapErrParameterInvalidMsg("common.fileResource.allowedPath must be configured before using file resources")
	}
	relative := name
	if _, local := s.ChunkManager.(*storage.LocalChunkManager); !local || filepath.IsAbs(name) {
		prefix := s.root + "/"
		if !strings.HasPrefix(name, prefix) {
			return "", merr.WrapErrParameterInvalidMsg("file resource path is outside the configured directory")
		}
		relative = strings.TrimPrefix(name, prefix)
	}
	for _, component := range strings.Split(relative, "/") {
		if component == "" || component == "." || component == ".." || strings.ContainsAny(component, "\\\x00") {
			return "", merr.WrapErrParameterInvalidMsg("invalid file resource path")
		}
	}
	return relative, nil
}

// ResolvePath fixes local relative paths before the resource is persisted.
func (s *restrictedSource) ResolvePath(name string) (string, error) {
	relative, err := s.relativePath(name)
	if err != nil {
		return "", err
	}
	if _, local := s.ChunkManager.(*storage.LocalChunkManager); local {
		return filepath.Join(s.root, relative), nil
	}
	return name, nil
}

func (s *restrictedSource) openLocal(relative string) (*storage.LocalReader, error) {
	// Open each component relative to a pinned directory; never follow a link
	// or block on a FIFO supplied in place of a dictionary.
	if !filepath.IsAbs(s.root) {
		return nil, merr.WrapErrParameterInvalidMsg("local file resource directory must be absolute")
	}
	fd, err := unix.Open(s.root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, merr.WrapErrIoFailedMsg("cannot open file resource directory")
	}
	parts := strings.Split(relative, "/")
	for i, part := range parts {
		flags := unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK
		if i < len(parts)-1 {
			flags |= unix.O_DIRECTORY
		}
		next, openErr := unix.Openat(fd, part, flags, 0)
		unix.Close(fd)
		if openErr != nil {
			if openErr == unix.ELOOP || openErr == unix.ENOTDIR {
				return nil, merr.WrapErrParameterInvalidMsg("file resource path contains a symbolic link or non-directory component")
			}
			if openErr == unix.ENOENT {
				return nil, merr.WrapErrIoKeyNotFound("file resource")
			}
			return nil, merr.WrapErrIoFailedMsg("cannot open file resource")
		}
		fd = next
	}
	file := os.NewFile(uintptr(fd), "file resource")
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, merr.WrapErrIoFailedMsg("cannot stat file resource")
	}
	if !info.Mode().IsRegular() {
		file.Close()
		return nil, merr.WrapErrParameterInvalidMsg("file resource must be a regular file")
	}
	return &storage.LocalReader{File: file}, nil
}

func (s *restrictedSource) Reader(ctx context.Context, name string) (storage.FileReader, error) {
	relative, err := s.relativePath(name)
	if err != nil {
		return nil, err
	}
	if _, local := s.ChunkManager.(*storage.LocalChunkManager); local {
		return s.openLocal(relative)
	}
	return s.ChunkManager.Reader(ctx, name)
}

func (s *restrictedSource) Size(ctx context.Context, name string) (int64, error) {
	relative, err := s.relativePath(name)
	if err != nil {
		return 0, err
	}
	if _, local := s.ChunkManager.(*storage.LocalChunkManager); !local {
		return s.ChunkManager.Size(ctx, name)
	}
	reader, err := s.openLocal(relative)
	if err != nil {
		return 0, err
	}
	defer reader.Close()
	return reader.Size()
}

func (s *restrictedSource) Exist(ctx context.Context, name string) (bool, error) {
	if _, err := s.relativePath(name); err != nil {
		return false, err
	}
	if _, local := s.ChunkManager.(*storage.LocalChunkManager); !local {
		return s.ChunkManager.Exist(ctx, name)
	}
	_, err := s.Size(ctx, name)
	return err == nil, err
}
