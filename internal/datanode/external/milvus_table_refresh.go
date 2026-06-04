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
	"fmt"
	"path"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/externalspec"
)

func (t *RefreshExternalCollectionTask) prepareMilvusTableDeltalogFragments(
	fragments []packed.Fragment,
) ([]packed.Fragment, error) {
	workFragments, err := t.milvusTableDeltalogFragmentsWithSourceDeltas(fragments)
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

func (t *RefreshExternalCollectionTask) milvusTableDeltalogFragmentsWithSourceDeltas(
	fragments []packed.Fragment,
) ([]packed.Fragment, error) {
	workFragments := make([]packed.Fragment, len(fragments))
	copy(workFragments, fragments)
	isVirtualPK := !packed.HasExternalPrimaryKey(t.req.GetSchema())
	for i := range workFragments {
		clonedDeltalogs := cloneFieldBinlogs(workFragments[i].Deltalogs)
		if isVirtualPK {
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
	comparableNewFragments, err := t.milvusTableDeltalogFragmentsForRefresh(newFragments)
	if err != nil {
		return false, err
	}
	// Compare deltalog identities only. Virtual-PK translation can change the
	// target delete entry count even when the source L0 log itself is unchanged.
	var currentDeltas map[string]struct{}
	if packed.HasExternalPrimaryKey(t.req.GetSchema()) {
		currentDeltas = deltalogIdentitySet(currentFragments)
	} else {
		var err error
		currentDeltas, err = targetOwnedDeltalogIdentitySet(seg.GetManifestPath(), currentFragments)
		if err != nil {
			return false, err
		}
	}
	newDeltas := deltalogIdentitySet(comparableNewFragments)
	return !stringSetEqual(currentDeltas, newDeltas), nil
}

func deltalogIdentitySet(fragments []packed.Fragment) map[string]struct{} {
	result := make(map[string]struct{})
	for _, fragment := range fragments {
		for _, fieldBinlog := range fragment.Deltalogs {
			for _, binlog := range fieldBinlog.GetBinlogs() {
				identity := deltalogIdentity(binlog)
				if identity == "" {
					continue
				}
				result[identity] = struct{}{}
			}
		}
	}
	return result
}

func (t *RefreshExternalCollectionTask) milvusTableDeltalogFragmentsForRefresh(
	fragments []packed.Fragment,
) ([]packed.Fragment, error) {
	workFragments := make([]packed.Fragment, len(fragments))
	copy(workFragments, fragments)
	for i := range workFragments {
		clonedDeltalogs := cloneFieldBinlogs(workFragments[i].Deltalogs)
		sourceDeltalogs, err := t.getMilvusTableSourceManifestDeltalogs(workFragments[i].FilePath)
		if err != nil {
			return nil, err
		}
		clonedDeltalogs = append(clonedDeltalogs, cloneFieldBinlogs(sourceDeltalogs)...)
		workFragments[i].Deltalogs = clonedDeltalogs
	}
	return workFragments, nil
}

func targetOwnedDeltalogIdentitySet(manifestPath string, fragments []packed.Fragment) (map[string]struct{}, error) {
	basePath, _, err := packed.UnmarshalManifestPath(manifestPath)
	if err != nil {
		return nil, fmt.Errorf("parse milvus-table manifest path %q: %w", manifestPath, err)
	}
	targetDeltaPrefix := strings.TrimRight(basePath, "/") + "/_delta/"
	result := make(map[string]struct{})
	for _, fragment := range fragments {
		for _, fieldBinlog := range fragment.Deltalogs {
			for _, binlog := range fieldBinlog.GetBinlogs() {
				if !strings.HasPrefix(binlog.GetLogPath(), targetDeltaPrefix) {
					continue
				}
				identity := deltalogIdentity(binlog)
				if identity == "" {
					continue
				}
				result[identity] = struct{}{}
			}
		}
	}
	return result, nil
}

func deltalogIdentity(binlog *datapb.Binlog) string {
	if binlog.GetLogID() != 0 {
		return strconv.FormatInt(binlog.GetLogID(), 10)
	}
	logPath := strings.TrimRight(binlog.GetLogPath(), "/")
	if logPath == "" {
		return ""
	}
	return path.Base(logPath)
}

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
	updated.StorageVersion = storage.StorageV3
	return updated, nil
}

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
