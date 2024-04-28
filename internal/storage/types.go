// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package storage

import (
	"context"
	"fmt"
	"io"
	"time"

	"golang.org/x/exp/mmap"
)

type StatsLogType int64

const (
	DefaultStatsType StatsLogType = iota + 0

	// CompundStatsType log save multiple stats
	// and bloom filters to one file
	CompoundStatsType
)

func (s StatsLogType) LogIdx() string {
	return fmt.Sprintf("%d", s)
}

type FileReader interface {
	io.Reader
	io.Closer
	io.ReaderAt
	io.Seeker
}

// ChunkObjectInfo is to store object info.
type ChunkObjectInfo struct {
	FilePath   string
	ModifyTime time.Time
}

// ChunkManager is to manager chunks.
// Include Read, Write, Remove chunks.
type ChunkManager interface {
	// RootPath returns current root path.
	RootPath() string
	// Path returns path of @filePath.
	Path(ctx context.Context, filePath string) (string, error)
	// Size returns path of @filePath.
	Size(ctx context.Context, filePath string) (int64, error)
	// Write writes @content to @filePath.
	Write(ctx context.Context, filePath string, content []byte) error
	// MultiWrite writes multi @content to @filePath.
	MultiWrite(ctx context.Context, contents map[string][]byte) error
	// Exist returns true if @filePath exists.
	Exist(ctx context.Context, filePath string) (bool, error)
	// Read reads @filePath and returns content.
	Read(ctx context.Context, filePath string) ([]byte, error)
	// Reader return a reader for @filePath
	Reader(ctx context.Context, filePath string) (FileReader, error)
	// MultiRead reads @filePath and returns content.
	MultiRead(ctx context.Context, filePaths []string) ([][]byte, error)
	// WalkWithPrefix list files with same @prefix and call @walkFunc for each file.
	// 1. walkFunc return false or reach the last object, WalkWithPrefix will stop and return nil.
	// 2. underlying walking failed or context canceled, WalkWithPrefix will stop and return a error.
	WalkWithPrefix(ctx context.Context, prefix string, recursive bool, walkFunc ChunkObjectWalkFunc) error
	Mmap(ctx context.Context, filePath string) (*mmap.ReaderAt, error)
	// ReadAt reads @filePath by offset @off, content stored in @p, return @n as the number of bytes read.
	// if all bytes are read, @err is io.EOF.
	// return other error if read failed.
	ReadAt(ctx context.Context, filePath string, off int64, length int64) (p []byte, err error)
	// Remove delete @filePath.
	Remove(ctx context.Context, filePath string) error
	// MultiRemove delete @filePaths.
	MultiRemove(ctx context.Context, filePaths []string) error
	// RemoveWithPrefix remove files with same @prefix.
	RemoveWithPrefix(ctx context.Context, prefix string) error
}

// ListAllChunkWithPrefix is a helper function to list all objects with same @prefix by using `ListWithPrefix`.
// `ListWithPrefix` is more efficient way to call if you don't need all chunk at same time.
func ListAllChunkWithPrefix(ctx context.Context, manager ChunkManager, prefix string, recursive bool) ([]string, []time.Time, error) {
	var dirs []string
	var mods []time.Time
	if err := manager.WalkWithPrefix(ctx, prefix, recursive, func(chunkInfo *ChunkObjectInfo) bool {
		dirs = append(dirs, chunkInfo.FilePath)
		mods = append(mods, chunkInfo.ModifyTime)
		return true
	}); err != nil {
		return nil, nil, err
	}
	return dirs, mods, nil
}
