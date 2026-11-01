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

import "context"

// notifyQueryViewSegmentUpdates runs after catalog and in-memory publication,
// outside segment/manifest locks. New Manifest requirements reach QueryNodes
// through a new DataView and QueryView rather than a SegmentMeta subscription.
func (m *meta) notifyQueryViewSegmentUpdates(ctx context.Context, segments []*SegmentInfo) {
	collections := make(map[int64]struct{})
	for _, segment := range segments {
		collections[segment.GetCollectionID()] = struct{}{}
	}
	for collectionID := range collections {
		m.recomputeDataView(ctx, collectionID)
	}
}
