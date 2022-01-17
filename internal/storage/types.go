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

// ChunkManager is to manager chunks.
// Include Read, Write, Remove chunks.
type ChunkManager interface {
	// GetPath returns path of @key
	GetPath(key string) (string, error)
	// Write writes @content to @key
	Write(key string, content []byte) error
	// Exist returns true if @key exists
	Exist(key string) bool
	// Read reads @key and returns content
	Read(key string) ([]byte, error)
	// ReadAt reads @key by offset @off, content stored in @p, return @n as the number of bytes read
	// if all bytes are read, @err is io.EOF
	// return other error if read failed
	ReadAt(key string, p []byte, off int64) (n int, err error)
}
