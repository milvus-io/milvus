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

package external

import (
	"context"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// prepareMilvusTableDeltalogFragments returns the fragments that should be used
// to build a target segment manifest. Virtual-PK collections include source
// manifest deltas so they can be translated into target-owned deltalogs.
func (t *RefreshExternalCollectionTask) prepareMilvusTableDeltalogFragments(
	fragments []packed.Fragment,
) ([]packed.Fragment, error) {
	workFragments, err := t.milvusTableDeltalogFragments(
		fragments,
		!packed.HasExternalPrimaryKey(t.req.GetSchema()),
	)
	if err != nil {
		return nil, err
	}
	for i := range workFragments {
		if err := populateDeltalogIDsFromPath(workFragments[i].Deltalogs); err != nil {
			return nil, err
		}
	}
	return workFragments, nil
}

// milvusTableDeltalogFragments clones fragment deltalogs and optionally merges
// source segment-manifest deltas into each fragment.
func (t *RefreshExternalCollectionTask) milvusTableDeltalogFragments(
	fragments []packed.Fragment,
	includeSourceManifestDeltas bool,
) ([]packed.Fragment, error) {
	workFragments := make([]packed.Fragment, len(fragments))
	copy(workFragments, fragments)
	for i := range workFragments {
		clonedDeltalogs := cloneFieldBinlogs(workFragments[i].Deltalogs)
		if includeSourceManifestDeltas {
			sourceDeltalogs, err := t.getMilvusTableSourceManifestDeltalogs(workFragments[i].FilePath)
			if err != nil {
				return nil, err
			}
			clonedDeltalogs = append(clonedDeltalogs, cloneFieldBinlogs(sourceDeltalogs)...)
		}
		workFragments[i].Deltalogs = clonedDeltalogs
	}
	return workFragments, nil
}

// shouldRefreshMilvusTableDeltalogs compares stable deltalog identities instead
// of entry counts. Virtual-PK translation may change target delete counts even
// when the source delta identity is unchanged.
func (t *RefreshExternalCollectionTask) shouldRefreshMilvusTableDeltalogs(
	seg *datapb.SegmentInfo,
	currentFragments []packed.Fragment,
	newFragments []packed.Fragment,
) (bool, error) {
	if t.parsedSpec == nil || t.parsedSpec.Format != externalspec.FormatMilvusTable {
		return false, nil
	}
	if len(newFragments) == 0 {
		return false, nil
	}
	comparableNewFragments, err := t.milvusTableDeltalogFragments(newFragments, true)
	if err != nil {
		return false, err
	}
	// Compare deltalog identities only. Virtual-PK translation can change the
	// target delete entry count even when the source L0 log itself is unchanged.
	var currentDeltas map[string]struct{}
	if packed.HasExternalPrimaryKey(t.req.GetSchema()) {
		currentDeltas, err = deltalogIdentitySet(currentFragments)
		if err != nil {
			return false, err
		}
	} else {
		currentDeltas, err = targetOwnedDeltalogIdentitySet(seg.GetManifestPath(), currentFragments)
		if err != nil {
			return false, err
		}
	}
	newDeltas, err := deltalogIdentitySet(comparableNewFragments)
	if err != nil {
		return false, err
	}
	return !stringSetEqual(currentDeltas, newDeltas), nil
}

// deltalogIdentitySet returns the set of stable deltalog identities present in
// the supplied fragments.
func deltalogIdentitySet(fragments []packed.Fragment) (map[string]struct{}, error) {
	result := make(map[string]struct{})
	for _, fragment := range fragments {
		for _, fieldBinlog := range fragment.Deltalogs {
			for _, binlog := range fieldBinlog.GetBinlogs() {
				identity, err := deltalogIdentity(binlog)
				if err != nil {
					return nil, err
				}
				if identity == "" {
					continue
				}
				result[identity] = struct{}{}
			}
		}
	}
	return result, nil
}

// targetOwnedDeltalogIdentitySet returns only deltas written under the target
// segment manifest base path. Virtual-PK collections generate these deltas
// during refresh.
func targetOwnedDeltalogIdentitySet(manifestPath string, fragments []packed.Fragment) (map[string]struct{}, error) {
	basePath, _, err := packed.UnmarshalManifestPath(manifestPath)
	if err != nil {
		return nil, merr.WrapErrServiceInternalErr(err, "parse milvus-table manifest path %q", manifestPath)
	}
	targetDeltaPrefix := strings.TrimRight(basePath, "/") + "/_delta/"
	result := make(map[string]struct{})
	for _, fragment := range fragments {
		for _, fieldBinlog := range fragment.Deltalogs {
			for _, binlog := range fieldBinlog.GetBinlogs() {
				if !strings.HasPrefix(binlog.GetLogPath(), targetDeltaPrefix) {
					continue
				}
				identity, err := deltalogIdentity(binlog)
				if err != nil {
					return nil, err
				}
				if identity == "" {
					continue
				}
				result[identity] = struct{}{}
			}
		}
	}
	return result, nil
}

// deltalogIdentity prefers Binlog.LogID and falls back to parsing the StorageV3
// _delta/<logID> path. Invalid fallback paths fail the refresh comparison.
func deltalogIdentity(binlog *datapb.Binlog) (string, error) {
	if binlog == nil {
		return "", nil
	}
	if binlog.GetLogID() != 0 {
		return strconv.FormatInt(binlog.GetLogID(), 10), nil
	}
	logPath := strings.TrimRight(binlog.GetLogPath(), "/")
	if logPath == "" {
		return "", nil
	}
	logID, err := parseMilvusTableDeltalogIDFromPath(logPath)
	if err != nil {
		return "", merr.WrapErrServiceInternalErr(err, "resolve milvus-table deltalog identity from %q", logPath)
	}
	return strconv.FormatInt(logID, 10), nil
}

// stringSetEqual compares two small identity sets.
func stringSetEqual(left, right map[string]struct{}) bool {
	if len(left) != len(right) {
		return false
	}
	for key := range left {
		if _, ok := right[key]; !ok {
			return false
		}
	}
	return true
}

// refreshMilvusTableSegmentManifest rebuilds a target segment manifest from the
// latest source fragments and stores the new manifest path on SegmentInfo.
func (t *RefreshExternalCollectionTask) refreshMilvusTableSegmentManifest(
	ctx context.Context,
	seg *datapb.SegmentInfo,
	fragments []packed.Fragment,
) (*datapb.SegmentInfo, error) {
	workFragments, err := t.prepareMilvusTableDeltalogFragments(fragments)
	if err != nil {
		return nil, err
	}
	manifestPath, err := t.createManifestForSegment(ctx, seg.GetID(), workFragments)
	if err != nil {
		return nil, err
	}
	updated := proto.Clone(seg).(*datapb.SegmentInfo)
	updated.ManifestPath = manifestPath
	updated.SchemaVersion = t.req.GetSchema().GetVersion()
	updated.StorageVersion = storage.StorageV3
	return updated, nil
}

// cloneFieldBinlogs deep-copies binlogs before they are cached or mutated with
// parsed source deltalog IDs.
func cloneFieldBinlogs(binlogs []*datapb.FieldBinlog) []*datapb.FieldBinlog {
	if len(binlogs) == 0 {
		return nil
	}
	cloned := make([]*datapb.FieldBinlog, 0, len(binlogs))
	for _, binlog := range binlogs {
		if binlog == nil {
			continue
		}
		cloned = append(cloned, proto.Clone(binlog).(*datapb.FieldBinlog))
	}
	return cloned
}
