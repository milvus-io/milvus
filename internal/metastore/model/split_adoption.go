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

package model

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type SplitAdoptionMove struct {
	SegmentID            int64
	PartitionID          int64
	Target               string
	TransformStart       uint64
	ManifestPath         string
	SourceTransformStart uint64
}

type SplitAdoption struct {
	TaskID       int64
	CollectionID int64
	Source       string
	State        string
	AttemptID    string
	Digest       string
	Count        int
	MovesValue   string
	Moves        []SplitAdoptionMove `json:"-"`
}

// SplitAdoptionCatalog is optional only for legacy/test catalogs. Production
// split admission requires it; ordinary collections retain their old catalog API.
type SplitAdoptionCatalog interface {
	FencePreparedSplitAdoptions(context.Context) error
	LoadSplitAdoption(context.Context, int64, int64) (*SplitAdoption, error)
	PrepareSplitAdoption(context.Context, *SplitAdoption) (*SplitAdoption, error)
	CommitSplitAdoption(context.Context, *SplitAdoption, *viewpb.DataViewOfCollection) error
	SaveSplitAdoptionState(context.Context, *SplitAdoption, string) error
	CanonicalizeSplitAdoptionSegment(context.Context, *datapb.SegmentInfo) error
}
