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
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"
	"golang.org/x/exp/mmap"

	"github.com/milvus-io/milvus/internal/log"
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

// Path returns the path of local data if exists.
func (lcm *LocalChunkManager) Path(filePath string) (string, error) {
	exist, err := lcm.Exist(filePath)
	if err != nil {
		return "", err
	}

	if !exist {
		return "", fmt.Errorf("local file cannot be found with filePath: %s", filePath)
	}
	absPath := path.Join(lcm.localPath, filePath)
	return absPath, nil
}

func (lcm *LocalChunkManager) Reader(filePath string) (FileReader, error) {
	exist, err := lcm.Exist(filePath)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, errors.New("local file cannot be found with filePath:" + filePath)
	}
	absPath := path.Join(lcm.localPath, filePath)
	return os.Open(absPath)
}

// Write writes the data to local storage.
func (lcm *LocalChunkManager) Write(filePath string, content []byte) error {
	absPath := path.Join(lcm.localPath, filePath)
	dir := path.Dir(absPath)
	exist, err := lcm.Exist(dir)
	if err != nil {
		return err
	}
	if !exist {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return err
		}
	}
	return ioutil.WriteFile(absPath, content, os.ModePerm)
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
func (lcm *LocalChunkManager) Exist(filePath string) (bool, error) {
	absPath := path.Join(lcm.localPath, filePath)
	_, err := os.Stat(absPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// Read reads the local storage data if exists.
func (lcm *LocalChunkManager) Read(filePath string) ([]byte, error) {
	exist, err := lcm.Exist(filePath)
	if err != nil {
		return nil, err
	}
	if !exist {
		return nil, fmt.Errorf("file not exist: %s", filePath)
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

func (lcm *LocalChunkManager) ListWithPrefix(prefix string, recursive bool) ([]string, []time.Time, error) {
	var filePaths []string
	var modTimes []time.Time
	if recursive {
		absPrefix := path.Join(lcm.localPath, prefix)
		dir := filepath.Dir(absPrefix)
		err := filepath.Walk(dir, func(filePath string, f os.FileInfo, err error) error {
			if strings.HasPrefix(filePath, absPrefix) && !f.IsDir() {
				filePaths = append(filePaths, strings.TrimPrefix(filePath, lcm.localPath))
			}
			return nil
		})
		if err != nil {
			return nil, nil, err
		}
		for _, filePath := range filePaths {
			modTime, err2 := lcm.getModTime(filePath)
			if err2 != nil {
				return filePaths, nil, err2
			}
			modTimes = append(modTimes, modTime)
		}
		return filePaths, modTimes, nil
	}
	absPrefix := path.Join(lcm.localPath, prefix+"*")
	absPaths, err := filepath.Glob(absPrefix)
	if err != nil {
		return nil, nil, err
	}
	for _, absPath := range absPaths {
		filePaths = append(filePaths, strings.TrimPrefix(absPath, lcm.localPath))
	}
	for _, filePath := range filePaths {
		modTime, err2 := lcm.getModTime(filePath)
		if err2 != nil {
			return filePaths, nil, err2
		}
		modTimes = append(modTimes, modTime)
	}
	return filePaths, modTimes, nil
}

func (lcm *LocalChunkManager) ReadWithPrefix(prefix string) ([]string, [][]byte, error) {
	filePaths, _, err := lcm.ListWithPrefix(prefix, true)
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

func (lcm *LocalChunkManager) Size(filePath string) (int64, error) {
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
	exist, err := lcm.Exist(filePath)
	if err != nil {
		return err
	}
	if exist {
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
	filePaths, _, err := lcm.ListWithPrefix(prefix, true)
	if err != nil {
		return err
	}
	return lcm.MultiRemove(filePaths)
}

func (lcm *LocalChunkManager) getModTime(filepath string) (time.Time, error) {
	absPath := path.Join(lcm.localPath, filepath)
	fi, err := os.Stat(absPath)
	if err != nil {
		log.Error("stat fileinfo error", zap.String("relative filepath", filepath))
		return time.Time{}, err
	}

	return fi.ModTime(), nil
}
