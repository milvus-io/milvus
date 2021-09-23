// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package datacoord

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

//TODO add more test for other parameters
func TestParamTable(t *testing.T) {
	Params.Init()

	assert.Equal(t, Params.InsertChannelPrefixName, "by-dev-insert-channel-")
	t.Logf("data coord insert channel = %s", Params.InsertChannelPrefixName)

	assert.Equal(t, Params.StatisticsChannelName, "by-dev-datacoord-statistics-channel")
	t.Logf("data coord stats channel = %s", Params.StatisticsChannelName)

	assert.Equal(t, Params.TimeTickChannelName, "by-dev-datacoord-timetick-channel")
	t.Logf("data coord timetick channel = %s", Params.TimeTickChannelName)

	assert.Equal(t, Params.SegmentInfoChannelName, "by-dev-segment-info-channel")
	t.Logf("data coord segment info channel = %s", Params.SegmentInfoChannelName)

	assert.Equal(t, Params.DataCoordSubscriptionName, "by-dev-dataCoord")
	t.Logf("data coord subscription channel = %s", Params.DataCoordSubscriptionName)

}
