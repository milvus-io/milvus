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
	"io"

	"golang.org/x/exp/mmap"
)

type FileReader interface {
	io.Reader
	io.Closer
}

// ChunkManager is to manager chunks.
// Include Read, Write, Remove chunks.
type ChunkManager interface {
	// Path returns path of @filePath.
	Path(filePath string) (string, error)
	// Size returns path of @filePath.
	Size(filePath string) (int64, error)
	// Write writes @content to @filePath.
	Write(filePath string, content []byte) error
	// MultiWrite writes multi @content to @filePath.
	MultiWrite(contents map[string][]byte) error
	// Exist returns true if @filePath exists.
	Exist(filePath string) bool
	// Read reads @filePath and returns content.
	Read(filePath string) ([]byte, error)
	// Reader return a reader for @filePath
	Reader(filePath string) (FileReader, error)
	// MultiRead reads @filePath and returns content.
	MultiRead(filePaths []string) ([][]byte, error)
	ListWithPrefix(prefix string) ([]string, error)
	// ReadWithPrefix reads files with same @prefix and returns contents.
	ReadWithPrefix(prefix string) ([]string, [][]byte, error)
	Mmap(filePath string) (*mmap.ReaderAt, error)
	// ReadAt reads @filePath by offset @off, content stored in @p, return @n as the number of bytes read.
	// if all bytes are read, @err is io.EOF.
	// return other error if read failed.
	ReadAt(filePath string, off int64, length int64) (p []byte, err error)
	// Remove delete @filePath.
	Remove(filePath string) error
	// MultiRemove delete @filePaths.
	MultiRemove(filePaths []string) error
	// RemoveWithPrefix remove files with same @prefix.
	RemoveWithPrefix(prefix string) error
}
