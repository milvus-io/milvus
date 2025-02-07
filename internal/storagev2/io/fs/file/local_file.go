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

package file

import (
	"io"
	"os"
)

var EOF = io.EOF

type LocalFile struct {
	file os.File
}

func (l *LocalFile) Read(p []byte) (n int, err error) {
	return l.file.Read(p)
}

func (l *LocalFile) Write(p []byte) (n int, err error) {
	return l.file.Write(p)
}

func (l *LocalFile) ReadAt(p []byte, off int64) (n int, err error) {
	return l.file.ReadAt(p, off)
}

func (l *LocalFile) Seek(offset int64, whence int) (int64, error) {
	return l.file.Seek(offset, whence)
}

func (l *LocalFile) Close() error {
	return l.file.Close()
}

func NewLocalFile(f *os.File) *LocalFile {
	return &LocalFile{
		file: *f,
	}
}
