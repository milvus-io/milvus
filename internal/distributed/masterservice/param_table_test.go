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

package grpcmasterservice

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParamTable(t *testing.T) {
	Params.Init()

	assert.NotEqual(t, Params.Address, "")
	t.Logf("master address = %s", Params.Address)

	assert.NotEqual(t, Params.Port, 0)
	t.Logf("master port = %d", Params.Port)

	assert.NotEqual(t, Params.IndexServiceAddress, "")
	t.Logf("IndexServiceAddress:%s", Params.IndexServiceAddress)

	assert.NotEqual(t, Params.DataServiceAddress, "")
	t.Logf("DataServiceAddress:%s", Params.DataServiceAddress)

	assert.NotEqual(t, Params.QueryServiceAddress, "")
	t.Logf("QueryServiceAddress:%s", Params.QueryServiceAddress)
}
