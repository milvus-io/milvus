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
	"errors"
	"io/ioutil"
	"os"
	"path"

	"golang.org/x/exp/mmap"
)

type LocalFileManager struct {
	localPath string
}

func NewLocalFileManager(localPath string) *LocalFileManager {
	return &LocalFileManager{
		localPath: localPath,
	}
}

func (lfm *LocalFileManager) GetFile(key string) (string, error) {
	if !lfm.Exist(key) {
		return "", errors.New("local file cannot be found with key:" + key)
	}
	path := path.Join(lfm.localPath, key)
	return path, nil
}

func (lfm *LocalFileManager) PutFile(key string, content []byte) error {
	path := path.Join(lfm.localPath, key)
	err := ioutil.WriteFile(path, content, 0644)
	if err != nil {
		return err
	}
	return nil
}

func (lfm *LocalFileManager) Exist(key string) bool {
	path := path.Join(lfm.localPath, key)
	_, err := os.Stat(path)
	if err != nil {
		if os.IsExist(err) {
			return true
		}
		return false
	}
	return true
}

func (lfm *LocalFileManager) ReadAll(key string) ([]byte, error) {
	path := path.Join(lfm.localPath, key)
	file, err := os.Open(path)
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

func (lfm *LocalFileManager) ReadAt(key string, p []byte, off int64) (n int, err error) {
	path := path.Join(lfm.localPath, key)
	at, err := mmap.Open(path)
	defer at.Close()
	if err != nil {
		return 0, err
	}

	return at.ReadAt(p, off)
}
