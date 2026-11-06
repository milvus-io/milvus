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

package upsertwal

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

func (s *UpsertWALSuite) TestConcurrentWritesUseFinalTimeTick() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c := s.create(ctx)
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	type outcome struct {
		marker int64
		result *types.AppendResult
		err    error
	}
	completed := make(chan outcome, 8)
	for marker := int64(1); marker <= 8; marker++ {
		go func(marker int64) {
			result, err := producer.Append(ctx, c.upsert(11, marker))
			completed <- outcome{marker, result, err}
		}(marker)
	}
	var winner outcome
	for range 8 {
		result := <-completed
		s.Require().NoError(result.err)
		if winner.result == nil || result.result.TimeTick > winner.result.TimeTick {
			winner = result
		}
	}
	for marker := int64(1); marker <= 8; marker++ {
		expected := 0
		if marker == winner.marker {
			expected = 1
		}
		s.assertRows(ctx, c, 11, marker, expected)
	}
	for pk := int64(101); pk <= 108; pk++ {
		go func(pk int64) {
			result, err := producer.Append(ctx, c.upsert(pk, pk))
			completed <- outcome{pk, result, err}
		}(pk)
	}
	for range 8 {
		s.Require().NoError((<-completed).err)
	}
	for pk := int64(101); pk <= 108; pk++ {
		s.assertRows(ctx, c, pk, pk, 1)
	}
}
