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

package manifest

import (
	"fmt"
	"path/filepath"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/storagev2/common/constant"
	"github.com/milvus-io/milvus/internal/storagev2/common/log"
	"github.com/milvus-io/milvus/internal/storagev2/common/utils"
	"github.com/milvus-io/milvus/internal/storagev2/io/fs"
)

var ErrManifestNotFound = errors.New("manifest not found")

type ManifestReaderWriter struct {
	fs   fs.Fs
	root string
}

func findAllManifest(fs fs.Fs, path string) ([]fs.FileEntry, error) {
	files, err := fs.List(path)
	log.Debug("list all manifest:", log.Any("files", files))
	if err != nil {
		return nil, err
	}
	return files, nil
}

func (rw ManifestReaderWriter) Read(version int64) (*Manifest, error) {
	manifests, err := findAllManifest(rw.fs, utils.GetManifestDir(rw.root))
	if err != nil {
		return nil, err
	}

	var maxVersionManifest string
	var maxVersion int64 = -1
	for _, m := range manifests {
		ver := utils.ParseVersionFromFileName(filepath.Base(m.Path))
		if ver == -1 {
			continue
		}

		if version != constant.LatestManifestVersion {
			if ver == version {
				return ParseFromFile(rw.fs, m.Path)
			}
		} else if ver > maxVersion {
			maxVersion = ver
			maxVersionManifest = m.Path
		}
	}

	if maxVersion != -1 {
		return ParseFromFile(rw.fs, maxVersionManifest)
	}
	return nil, ErrManifestNotFound
}

func (rw ManifestReaderWriter) MaxVersion() (int64, error) {
	manifests, err := findAllManifest(rw.fs, utils.GetManifestDir(rw.root))
	if err != nil {
		return -1, err
	}
	var max int64 = -1
	for _, m := range manifests {
		ver := utils.ParseVersionFromFileName(filepath.Base(m.Path))
		if ver == -1 {
			continue
		}

		if ver > max {
			max = ver
		}
	}

	if max == -1 {
		return -1, ErrManifestNotFound
	}
	return max, nil
}

func (rw ManifestReaderWriter) Write(m *Manifest) error {
	tmpManifestFilePath := utils.GetManifestTmpFilePath(rw.root, m.Version())
	manifestFilePath := utils.GetManifestFilePath(rw.root, m.Version())
	log.Debug("path", log.String("tmpManifestFilePath", tmpManifestFilePath), log.String("manifestFilePath", manifestFilePath))
	output, err := rw.fs.OpenFile(tmpManifestFilePath)
	if err != nil {
		return fmt.Errorf("open file error: %w", err)
	}
	if err = WriteManifestFile(m, output); err != nil {
		return err
	}
	err = rw.fs.Rename(tmpManifestFilePath, manifestFilePath)
	if err != nil {
		return fmt.Errorf("rename file error: %w", err)
	}
	log.Debug("save manifest file success", log.String("path", manifestFilePath))
	return nil
}

func NewManifestReaderWriter(fs fs.Fs, root string) ManifestReaderWriter {
	return ManifestReaderWriter{fs, root}
}
