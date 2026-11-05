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
)

func (s *UpsertWALSuite) TestAutocommitReplacementAndRetry() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	c := s.create(ctx)
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	first, err := producer.Append(ctx, c.upsert(11, 1))
	s.Require().NoError(err)
	s.Require().NotNil(first.MessageID)
	s.Require().Positive(first.TimeTick)
	s.assertRows(ctx, c, 11, 1, 1)
	newWrite := c.upsert(11, 2, "replace")
	replacement, err := producer.Append(ctx, newWrite)
	s.Require().NoError(err)
	s.Require().Greater(replacement.TimeTick, first.TimeTick)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
	retry, err := producer.Append(ctx, c.upsert(11, 2, "replace"))
	s.Require().NoError(err)
	s.Require().True(retry.MessageID.EQ(replacement.MessageID))
	s.Require().Equal(replacement.TimeTick, retry.TimeTick)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
	s.assertPhysicalRows(ctx, c, 2)
}
