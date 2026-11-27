// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"strconv"

	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type ManifestMutationType int

const (
	ManifestMutationNoop ManifestMutationType = iota // adopt an externally prepared revision
	ManifestMutationCommitUpdates
)

type ManifestMutation struct {
	Type         ManifestMutationType
	ManifestPath string
	Updates      *packed.ManifestUpdates
}
type SegmentManifestCommit struct {
	SegmentID          int64
	StorageConfig      *indexpb.StorageConfig
	ExpectedManifest   string
	InspectedManifest  string
	Mutation           ManifestMutation
	classifiedManifest string
	entry              *BackfillSegment
}

type preparedBackfillManifest struct {
	segmentID     int64
	base          string
	next          string
	droppedFields map[int64]struct{}
}

func staleSegmentManifestError(id int64, expected, actual string) error {
	return merr.WrapErrServiceUnavailableMsg("stale segment manifest for segment %d: expected %s, current %s", id, expected, actual)
}

func (s *Server) prepareBackfillManifest(ctx context.Context, c SegmentManifestCommit) (preparedBackfillManifest, error) {
	p := preparedBackfillManifest{segmentID: c.SegmentID}
	if err := ctx.Err(); err != nil {
		return p, err
	}
	seg := s.meta.GetSegment(ctx, c.SegmentID)
	if seg == nil || seg.GetState() != commonpb.SegmentState_Flushed || seg.GetStorageVersion() != storage.StorageV3 {
		return p, merr.WrapErrServiceUnavailableMsg("segment %d is no longer a flushed V3 segment", c.SegmentID)
	}
	p.base = seg.GetManifestPath()
	for _, pin := range []string{c.ExpectedManifest, c.InspectedManifest} {
		if pin != "" {
			cmp, err := packed.CompareManifestPath(pin, p.base)
			if err != nil {
				return p, err
			}
			if cmp != 0 {
				return p, staleSegmentManifestError(c.SegmentID, pin, p.base)
			}
		}
	}

	if c.Mutation.Type == ManifestMutationCommitUpdates {
		base, ver, err := packed.UnmarshalManifestPath(p.base)
		if err != nil || base == "" || ver < 0 {
			return p, merr.WrapErrDataIntegrityMsg("invalid base manifest %q for segment %d", p.base, c.SegmentID)
		}
		updates := c.Mutation.Updates
		if c.classifiedManifest != "" && c.classifiedManifest != p.base {
			// A writer won between classification and preparation. Re-evaluate
			// against its revision, including exact replays of a replacement.
			var applied bool
			updates, applied, _, err = deltaOpsToManifestUpdates(seg, base, c.entry, c.StorageConfig)
			if err != nil {
				return p, err
			}
			if applied {
				p.next = p.base
				return p, nil
			}
		}
		for _, col := range updates.DropColumns {
			fid, err := strconv.ParseInt(col, 10, 64)
			if err != nil {
				return p, merr.WrapErrParameterInvalidMsg("invalid backfill field ID %q", col)
			}
			if p.droppedFields == nil {
				p.droppedFields = make(map[int64]struct{})
			}
			p.droppedFields[fid] = struct{}{}
		}
		p.next, err = packed.CommitManifestUpdates(base, ver, c.StorageConfig, updates)
		if err != nil {
			return p, err
		}
	} else {
		p.next = c.Mutation.ManifestPath
	}
	cmp, err := packed.CompareManifestPath(p.next, p.base)
	if err != nil {
		return p, err
	}
	if cmp <= 0 {
		return p, staleSegmentManifestError(p.segmentID, p.next, p.base)
	}
	return p, nil
}

// Publish from current segment clones so ordinary metadata updates made while
// loon was writing the revision survive. SegMu is also held by stats, L0 and
// compaction when they publish their own manifest pointers.
func (s *Server) publishBackfillManifests(ctx context.Context, prepared []preparedBackfillManifest, actions []metastore.UpdateAction, applyIndexes func()) error {
	s.meta.SegMu.Lock()
	defer s.meta.SegMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	segments := make([]*meta.SegmentInfo, 0, len(prepared))
	for _, p := range prepared {
		latest := s.meta.Segments.GetSegment(p.segmentID)
		if latest == nil || latest.GetState() != commonpb.SegmentState_Flushed || latest.GetStorageVersion() != storage.StorageV3 {
			return merr.WrapErrServiceUnavailableMsg("segment %d is no longer a flushed V3 segment", p.segmentID)
		}
		if latest.GetManifestPath() != p.base {
			return staleSegmentManifestError(p.segmentID, p.base, latest.GetManifestPath())
		}
		if p.next == p.base {
			continue
		}
		seg := latest.Clone()
		seg.ManifestPath = p.next
		for id := range p.droppedFields {
			delete(seg.TextStatsLogs, id)
			delete(seg.JsonKeyStats, id)
		}
		segments = append(segments, seg)
		actions = append(actions, metastore.PublishSegmentManifest(seg.SegmentInfo))
	}
	if len(actions) == 0 {
		return nil
	}
	if err := s.meta.Catalog.Update(ctx, actions...); err != nil {
		return err
	}
	if applyIndexes != nil {
		applyIndexes()
	}
	for _, seg := range segments {
		s.meta.Segments.SetSegment(seg.GetID(), seg)
	}
	return nil
}

func (s *Server) commitSegmentManifest(ctx context.Context, c SegmentManifestCommit) error {
	p, err := s.prepareBackfillManifest(ctx, c)
	if err != nil {
		return err
	}
	if len(p.droppedFields) == 0 {
		return s.publishBackfillManifests(ctx, []preparedBackfillManifest{p}, nil, nil)
	}
	return s.publishPreparedReplacement(ctx, p)
}

func (s *Server) publishPreparedReplacement(ctx context.Context, p preparedBackfillManifest) error {
	return s.indexMeta.PublishColumnReplacement(p.segmentID, p.droppedFields, func(actions []metastore.UpdateAction, apply func()) error {
		return s.publishBackfillManifests(ctx, []preparedBackfillManifest{p}, actions, apply)
	})
}

func (s *Server) commitSegmentManifests(ctx context.Context, commits []SegmentManifestCommit) error {
	seen := make(map[int64]struct{}, len(commits))
	for _, c := range commits {
		if _, ok := seen[c.SegmentID]; ok {
			return merr.WrapErrParameterInvalidMsg("duplicate segment %d in backfill batch", c.SegmentID)
		}
		seen[c.SegmentID] = struct{}{}
		if c.Mutation.Updates != nil && len(c.Mutation.Updates.DropColumns) > 0 {
			return merr.WrapErrServiceInternalMsg("backfill replacement requires index retirement")
		}
	}
	prepared := make([]preparedBackfillManifest, len(commits))
	group, gctx := errgroup.WithContext(ctx)
	group.SetLimit(backfillClassifyConcurrency)
	for i, c := range commits {
		group.Go(func() error {
			var err error
			prepared[i], err = s.prepareBackfillManifest(gctx, c)
			return err
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	return s.publishBackfillManifests(ctx, prepared, nil, nil)
}
