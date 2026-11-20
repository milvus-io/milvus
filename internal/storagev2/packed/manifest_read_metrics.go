// Copyright 2026 Zilliz
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

import (
	"time"

	"github.com/milvus-io/milvus/pkg/v3/util/stage"
)

// ManifestReadOrigin is a bounded instrumentation site, never a request label.
type ManifestReadOrigin uint8

const (
	ManifestReadOther ManifestReadOrigin = iota
	ManifestReadCreate
	ManifestReadPreload
	ManifestReadPostSync
	ManifestReadReopen
	manifestReadOriginCount
)

const (
	manifestReadTotal = iota
	manifestReadProperties
	manifestReadBegin
	manifestReadGet
	manifestReadExtract
	manifestReadPhaseCount
)

var manifestReadPhaseNames = [manifestReadPhaseCount]string{
	"total", "properties", "transaction_begin", "get_manifest", "extract_stats",
}

type manifestReadTiming struct {
	durations [manifestReadPhaseCount]time.Duration
}

var manifestReadMetrics = func() [manifestReadOriginCount][manifestReadPhaseCount]*stage.Recorder {
	var result [manifestReadOriginCount][manifestReadPhaseCount]*stage.Recorder
	for origin, operation := range [...]string{
		"manifest_other", "manifest_create", "manifest_preload", "manifest_postsync", "manifest_reopen",
	} {
		for phase, name := range manifestReadPhaseNames {
			result[origin][phase] = stage.New("storage", operation, name)
		}
	}
	return result
}()

// Observe once per GetManifestStats attempt with a single final outcome and
// zero for skipped stages. The existing FFI exposes transaction begin as one
// interval; its cache, I/O, decode and retry work cannot be separated here.
func (t *manifestReadTiming) observe(origin ManifestReadOrigin, err error) {
	if origin >= manifestReadOriginCount {
		origin = ManifestReadOther
	}
	result := stage.Outcome(err)
	recorders := &manifestReadMetrics[origin]
	for i, duration := range t.durations {
		recorders[i].Observe(duration, result)
	}
}
