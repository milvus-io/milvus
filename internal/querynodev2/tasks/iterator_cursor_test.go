// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information regarding copyright
// ownership. The ASF licenses this file to You under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tasks

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/iteratorutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestMarkIteratorPKCursorResultsAcknowledgesAllGroupedOutputsIncludingEmpty(t *testing.T) {
	first := &internalpb.SearchResults{Status: &commonpb.Status{ExtraInfo: map[string]string{"cost": "7"}}}
	second := &internalpb.SearchResults{Status: merr.Success()}
	task := &SearchTask{originNqs: []int64{1, 1}, result: first, others: []*SearchTask{{result: second}}}
	require.NoError(t, task.markIteratorPKCursorResults())
	require.Equal(t, "2", first.Status.ExtraInfo[iteratorutil.CursorVersionKey])
	require.Equal(t, "2", second.Status.ExtraInfo[iteratorutil.CursorVersionKey])
	require.Equal(t, "7", first.Status.ExtraInfo["cost"])
	// Marker propagation is execution acknowledgement; an empty segment result
	// must participate so the proxy can distinguish mixed workers from EOF.
	require.Nil(t, first.SlicedBlob)
	require.True(t, iteratorutil.AllResultsUsePKCursor([]*internalpb.SearchResults{first, second}))
}
func TestMarkIteratorPKCursorResultsRejectsMissingResultOrStatus(t *testing.T) {
	for _, result := range []*internalpb.SearchResults{nil, {}} {
		task := &SearchTask{originNqs: []int64{1}, result: result}
		require.ErrorIs(t, task.markIteratorPKCursorResults(), merr.ErrServiceInternal)
	}
	task := &SearchTask{originNqs: []int64{1, 1}, result: &internalpb.SearchResults{Status: merr.Success()}, others: []*SearchTask{{}}}
	require.ErrorIs(t, task.markIteratorPKCursorResults(), merr.ErrServiceInternal)
}
