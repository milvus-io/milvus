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

package querycoord

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

//TODO add more test for other parameters
func TestParamTable(t *testing.T) {
	Params.Init()

	assert.Equal(t, Params.SearchChannelPrefix, "by-dev-search")
	t.Logf("query coord search channel = %s", Params.SearchChannelPrefix)

	assert.Equal(t, Params.SearchResultChannelPrefix, "by-dev-searchResult")
	t.Logf("query coord search result channel = %s", Params.SearchResultChannelPrefix)

	assert.Equal(t, Params.StatsChannelName, "by-dev-query-node-stats")
	t.Logf("query coord stats channel = %s", Params.StatsChannelName)

	assert.Equal(t, Params.TimeTickChannelName, "by-dev-queryTimeTick")
	t.Logf("query coord  time tick channel = %s", Params.TimeTickChannelName)
}
