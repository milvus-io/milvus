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

package packed

/*
#cgo pkg-config: milvus_core milvus-storage
#include "milvus-storage/ffi_c.h"
*/
import "C"

import (
	"context"
	"sync"
	"unsafe"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// LoadManifest owns the manifest and its Go projections for a physical load.
// It is shared only by operations using one segment's primary storage.
type LoadManifest struct {
	path       string
	native     *C.LoonManifest
	stats      map[string]ManifestStat
	deltaPaths []string
	indexOnce  sync.Once
	indexes    []ManifestIndexInfo
	indexErr   error
}

func OpenLoadManifest(ctx context.Context, path string, config *indexpb.StorageConfig) (*LoadManifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	native, err := getManifestHandle(ctx, path, config, ExternalSpecContext{}, nil, common.TalonStageManifest)
	if err != nil {
		return nil, merr.Wrap(err, "open segment load manifest")
	}
	if native == nil {
		return nil, merr.WrapErrDataIntegrityMsg("manifest %s returned a nil handle", path)
	}
	manifest := &LoadManifest{path: path, native: native}
	if err := manifest.decode(ctx); err != nil {
		manifest.Close()
		return nil, err
	}
	return manifest, nil
}

func (m *LoadManifest) decode(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	stats, err := decodeManifestStats(m.native, m.path)
	if err != nil {
		return err
	}
	deltas, err := deltaLogsFromManifest(m.path, m.native)
	if err != nil {
		return err
	}
	m.stats = stats
	m.deltaPaths = deltaLogPaths(deltas)
	return ctx.Err()
}

func (m *LoadManifest) Close() {
	if m != nil {
		C.loon_manifest_destroy(m.native)
		m.native = nil
	}
}

// Native is borrowed by segcore while importing native column groups.
func (m *LoadManifest) Native() unsafe.Pointer {
	if m == nil {
		return nil
	}
	return unsafe.Pointer(m.native)
}

// Indexes projects the index section of the already-open revision on demand.
func (m *LoadManifest) Indexes() ([]ManifestIndexInfo, error) {
	m.indexOnce.Do(func() {
		m.indexes, m.indexErr = manifestIndexInfos(m.native, m.path)
	})
	return m.indexes, m.indexErr
}

func (m *LoadManifest) DeltaPaths() []string {
	return m.deltaPaths
}

func (m *LoadManifest) Matches(path string) bool {
	return m != nil && m.native != nil && SameManifestPath(m.path, path)
}

type loadManifestKey struct{}

func WithLoadManifest(ctx context.Context, manifest *LoadManifest) context.Context {
	return context.WithValue(ctx, loadManifestKey{}, manifest)
}

func LoadManifestFromContext(ctx context.Context, path string) *LoadManifest {
	manifest, _ := ctx.Value(loadManifestKey{}).(*LoadManifest)
	if manifest.Matches(path) {
		return manifest
	}
	return nil
}
