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

package querycoordv2

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// GetLoadPercentageByResourceGroup shares the public QueryView loading contract.
// An existing group without a configured replica returns -1, not 0 or 100.
func (s *Server) GetLoadPercentageByResourceGroup(ctx context.Context, collectionID int64, rgName string) (int32, error) {
	response, err := s.ShowLoadCollections(ctx, &querypb.ShowCollectionsRequest{CollectionIDs: []int64{collectionID}, ResourceGroup: rgName})
	if err := merr.CheckRPCCall(response, err); err != nil {
		return -1, err
	}
	return int32(response.GetInMemoryPercentages()[0]), nil
}
