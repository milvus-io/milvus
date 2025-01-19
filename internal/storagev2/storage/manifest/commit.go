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
	"github.com/milvus-io/milvus/internal/storagev2/common/constant"
	"github.com/milvus-io/milvus/internal/storagev2/storage/lock"
)

type ManifestCommit struct {
	ops  []ManifestCommitOp
	lock lock.LockManager
	rw   ManifestReaderWriter
}

func (m *ManifestCommit) AddOp(op ...ManifestCommitOp) {
	m.ops = append(m.ops, op...)
}

func (m ManifestCommit) Commit() (manifest *Manifest, err error) {
	ver, latest, err := m.lock.Acquire()
	if err != nil {
		return nil, err
	}
	var version int64
	defer func() {
		if err != nil {
			if err2 := m.lock.Release(-1, false); err2 != nil {
				err = err2
			}
		} else {
			err = m.lock.Release(version, true)
		}
	}()
	var base *Manifest
	if latest {
		base, err = m.rw.Read(constant.LatestManifestVersion)
		if err != nil {
			return nil, err
		}
		base.version++
	} else {
		base, err = m.rw.Read(ver)
		if err != nil {
			return nil, err
		}
		maxVersion, err := m.rw.MaxVersion()
		if err != nil {
			return nil, err
		}
		base.version = maxVersion + 1
	}

	for _, op := range m.ops {
		op.commit(base)
	}
	version = base.version

	err = m.rw.Write(base)
	if err != nil {
		return nil, err
	}
	return base, nil
}

func NewManifestCommit(lock lock.LockManager, rw ManifestReaderWriter) ManifestCommit {
	return ManifestCommit{nil, lock, rw}
}
