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
	"errors"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	"golang.org/x/exp/mmap"

	"github.com/milvus-io/milvus/internal/util/errorutil"
)

// LocalChunkManager is responsible for read and write local file.
type LocalChunkManager struct {
	localPath string
}

var _ ChunkManager = (*LocalChunkManager)(nil)

// NewLocalChunkManager create a new local manager object.
func NewLocalChunkManager(opts ...Option) *LocalChunkManager {
	c := newDefaultConfig()
	for _, opt := range opts {
		opt(c)
	}
	return &LocalChunkManager{
		localPath: c.rootPath,
	}
}

// GetPath returns the path of local data if exists.
func (lcm *LocalChunkManager) GetPath(filePath string) (string, error) {
	if !lcm.Exist(filePath) {
		return "", errors.New("local file cannot be found with filePath:" + filePath)
	}
	absPath := path.Join(lcm.localPath, filePath)
	return absPath, nil
}

// Write writes the data to local storage.
func (lcm *LocalChunkManager) Write(filePath string, content []byte) error {
	absPath := path.Join(lcm.localPath, filePath)
	dir := path.Dir(absPath)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return err
		}
	}
	err := ioutil.WriteFile(absPath, content, os.ModePerm)
	if err != nil {
		return err
	}
	return nil
}

// MultiWrite writes the data to local storage.
func (lcm *LocalChunkManager) MultiWrite(contents map[string][]byte) error {
	var el errorutil.ErrorList
	for filePath, content := range contents {
		err := lcm.Write(filePath, content)
		if err != nil {
			el = append(el, err)
		}
	}
	if len(el) == 0 {
		return nil
	}
	return el
}

// Exist checks whether chunk is saved to local storage.
func (lcm *LocalChunkManager) Exist(filePath string) bool {
	absPath := path.Join(lcm.localPath, filePath)
	if _, err := os.Stat(absPath); errors.Is(err, os.ErrNotExist) {
		return false
	}
	return true
}

// Read reads the local storage data if exists.
func (lcm *LocalChunkManager) Read(filePath string) ([]byte, error) {
	if !lcm.Exist(filePath) {
		return nil, errors.New("file not exist" + filePath)
	}
	absPath := path.Join(lcm.localPath, filePath)
	file, err := os.Open(path.Clean(absPath))
	if err != nil {
		return nil, err
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}
	return content, nil
}

// MultiRead reads the local storage data if exists.
func (lcm *LocalChunkManager) MultiRead(filePaths []string) ([][]byte, error) {
	results := make([][]byte, len(filePaths))
	var el errorutil.ErrorList
	for i, filePath := range filePaths {
		content, err := lcm.Read(filePath)
		if err != nil {
			el = append(el, err)
		}
		results[i] = content
	}
	if len(el) == 0 {
		return results, nil
	}
	return results, el
}

func (lcm *LocalChunkManager) ListWithPrefix(prefix string) ([]string, error) {
	var filePaths []string
	absPrefix := path.Join(lcm.localPath, prefix)
	dir := filepath.Dir(absPrefix)
	err := filepath.Walk(dir, func(filePath string, f os.FileInfo, err error) error {
		if strings.HasPrefix(filePath, absPrefix) && !f.IsDir() {
			filePaths = append(filePaths, strings.TrimPrefix(filePath, lcm.localPath))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return filePaths, nil
}

func (lcm *LocalChunkManager) ReadWithPrefix(prefix string) ([]string, [][]byte, error) {
	filePaths, err := lcm.ListWithPrefix(prefix)
	if err != nil {
		return nil, nil, err
	}
	result, err := lcm.MultiRead(filePaths)
	return filePaths, result, err
}

// ReadAt reads specific position data of local storage if exists.
func (lcm *LocalChunkManager) ReadAt(filePath string, off int64, length int64) ([]byte, error) {
	if off < 0 || length < 0 {
		return nil, io.EOF
	}
	absPath := path.Join(lcm.localPath, filePath)
	file, err := os.Open(path.Clean(absPath))
	if err != nil {
		return nil, err
	}
	defer file.Close()
	res := make([]byte, length)
	if _, err := file.ReadAt(res, off); err != nil {
		return nil, err
	}
	return res, nil
}

func (lcm *LocalChunkManager) Mmap(filePath string) (*mmap.ReaderAt, error) {
	absPath := path.Join(lcm.localPath, filePath)
	return mmap.Open(path.Clean(absPath))
}

func (lcm *LocalChunkManager) GetSize(filePath string) (int64, error) {
	absPath := path.Join(lcm.localPath, filePath)
	fi, err := os.Stat(absPath)
	if err != nil {
		return 0, err
	}
	// get the size
	size := fi.Size()
	return size, nil
}

func (lcm *LocalChunkManager) Remove(filePath string) error {
	if lcm.Exist(filePath) {
		absPath := path.Join(lcm.localPath, filePath)
		err := os.RemoveAll(absPath)
		if err != nil {
			return err
		}
	}
	return nil
}

func (lcm *LocalChunkManager) MultiRemove(filePaths []string) error {
	var el errorutil.ErrorList
	for _, filePath := range filePaths {
		err := lcm.Remove(filePath)
		if err != nil {
			el = append(el, err)
		}
	}
	if len(el) == 0 {
		return nil
	}
	return el
}

func (lcm *LocalChunkManager) RemoveWithPrefix(prefix string) error {
	filePaths, err := lcm.ListWithPrefix(prefix)
	if err != nil {
		return err
	}
	return lcm.MultiRemove(filePaths)
}
