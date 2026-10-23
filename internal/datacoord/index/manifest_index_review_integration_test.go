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

package index

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestManifestReloadMissingPointerFailsClosed(t *testing.T) {
	m, _ := setupManifestReloadMeta(t)
	segment := m.GetSegment(context.Background(), 5001).Clone()
	segment.ManifestPath = ""
	m.Segments.SetSegment(segment.GetID(), segment)
	err := m.ReloadSegmentIndexesFromManifests(context.Background())
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
}
