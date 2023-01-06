/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package commonpbutil

import (
	"sync/atomic"
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/commonpb"
	"github.com/stretchr/testify/assert"
)

func TestIsHealthy(t *testing.T) {
	{
		v := atomic.Value{}
		v.Store(1)
		assert.False(t, IsHealthy(v))
	}

	{
		v := atomic.Value{}
		v.Store(commonpb.StateCode_Abnormal)
		assert.False(t, IsHealthy(v))
	}

	{
		v := atomic.Value{}
		v.Store(commonpb.StateCode_Stopping)
		assert.False(t, IsHealthy(v))
	}

	{
		v := atomic.Value{}
		v.Store(commonpb.StateCode_Healthy)
		assert.True(t, IsHealthy(v))
	}
}

func TestIsHealthyOrStopping(t *testing.T) {
	{
		v := atomic.Value{}
		v.Store(1)
		assert.False(t, IsHealthyOrStopping(v))
	}

	{
		v := atomic.Value{}
		v.Store(commonpb.StateCode_Abnormal)
		assert.False(t, IsHealthyOrStopping(v))
	}

	{
		v := atomic.Value{}
		v.Store(commonpb.StateCode_Stopping)
		assert.True(t, IsHealthyOrStopping(v))
	}

	{
		v := atomic.Value{}
		v.Store(commonpb.StateCode_Healthy)
		assert.True(t, IsHealthyOrStopping(v))
	}
}
