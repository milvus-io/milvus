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

package datacoord

import (
	"context"
	"errors"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type incrementalPrewarmCommit struct{ objects []packed.PrewarmObject }

type talonIncrementalPrewarm struct {
	ctx    context.Context
	cancel context.CancelFunc
	queue  chan incrementalPrewarmCommit
	done   chan struct{}
	client TalonPrewarmClient
	prefix string
}

func (s *Server) initTalonIncrementalPrewarm(p *paramtable.ComponentParam, factory func(*indexpb.StorageConfig, int) (TalonPrewarmClient, error)) error {
	if !p.DataCoordCfg.TalonIncrementalPrewarmEnabled.GetAsBool() || p.CommonCfg.StorageType.GetValue() == "local" {
		return nil
	}
	config := talonPrewarmStorageConfig(p)
	if strings.TrimSpace(config.GetTalonCoordinator()) == "" {
		return merr.WrapErrServiceInternalMsg("incremental prewarm requires common.storage.talon.coordinator")
	}
	resolve, err := manifestPrewarmResolver(config.GetCloudProvider(), config.GetBucketName())
	if err != nil {
		return err
	}
	descriptor := packed.MarshalManifestPath("prewarm-prefix", 1)
	uri, err := resolve(descriptor)
	if err != nil {
		return err
	}
	key, err := packed.ManifestFilePath(descriptor)
	if err != nil {
		return err
	}
	client, err := factory(config, 1)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(s.ctx)
	worker := &talonIncrementalPrewarm{ctx: ctx, cancel: cancel, queue: make(chan incrementalPrewarmCommit, 1024), done: make(chan struct{}), client: client, prefix: strings.TrimSuffix(uri, key)}
	s.meta.incrementalPrewarm = worker
	go worker.run()
	return nil
}

func (w *talonIncrementalPrewarm) stop() { w.cancel(); <-w.done }

func (m *meta) prewarmManifestCommit(before string, segment *SegmentInfo, mutation ManifestMutation, manifestSize int64) {
	w := m.incrementalPrewarm
	if w == nil || segment == nil || !isSegmentHealthy(segment) || packed.SameManifestPath(before, segment.GetManifestPath()) {
		return
	}
	switch segment.GetState() {
	case commonpb.SegmentState_Sealed, commonpb.SegmentState_Flushing, commonpb.SegmentState_Flushed:
	default:
		return
	}
	base, _, err := packed.UnmarshalManifestPath(segment.GetManifestPath())
	if err != nil {
		mlog.RatedWarn(m.ctx, 1, "incremental prewarm descriptor invalid", mlog.Err(err))
		return
	}
	objects, missing, err := mutation.Updates.NewObjects(base)
	if err != nil {
		mlog.RatedWarn(m.ctx, 1, "incremental prewarm object extraction failed", mlog.Err(err))
		return
	}
	objects = append(objects, mutation.NewObjects...)
	manifest, err := packed.ManifestFilePath(segment.GetManifestPath())
	if err != nil {
		mlog.RatedWarn(m.ctx, 1, "incremental prewarm manifest path invalid", mlog.Err(err))
		return
	}
	if manifestSize > 0 {
		objects = append(objects, packed.PrewarmObject{Path: manifest, Size: uint64(manifestSize)})
	} else {
		missing = append(missing, manifest)
	}
	if len(missing) > 0 {
		mlog.RatedWarn(m.ctx, 1, "incremental prewarm missing object sizes", mlog.Int("objects", len(missing)))
	}
	if len(objects) == 0 {
		return
	}
	select {
	case <-w.ctx.Done():
	case w.queue <- incrementalPrewarmCommit{objects: objects}:
	default:
		mlog.RatedWarn(m.ctx, 1, "incremental prewarm queue full; skipping event")
	}
}

func (w *talonIncrementalPrewarm) run() {
	defer close(w.done)
	defer w.client.Close()
	for {
		select {
		case <-w.ctx.Done():
			return
		case commit := <-w.queue:
			if err := w.prewarm(commit); err != nil {
				mlog.RatedWarn(w.ctx, 1, "incremental prewarm failed", mlog.Err(err))
			}
		}
	}
}

func (w *talonIncrementalPrewarm) prewarm(commit incrementalPrewarmCommit) error {
	seen := map[string]struct{}{}
	batch := make([]TalonPrewarmRequest, 0, 1024)
	var firstErr error
	flush := func() {
		if err := w.client.BatchLoad(w.ctx, batch); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
		batch = batch[:0]
	}
	for _, object := range commit.objects {
		if err := w.ctx.Err(); err != nil {
			return errors.Join(firstErr, err)
		}
		if _, exists := seen[object.Path]; exists || object.Path == "" {
			continue
		}
		seen[object.Path] = struct{}{}
		// Empty version selects Talon's UNCHECKED_VERSION cache identity.
		batch = append(batch, TalonPrewarmRequest{URI: w.prefix + object.Path, Size: object.Size})
		if len(batch) == 1024 {
			flush()
		}
	}
	if len(batch) > 0 {
		flush()
	}
	return firstErr
}
