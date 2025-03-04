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

package importutilv2

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

func TestOption_GetTimeout(t *testing.T) {
	const delta = 3 * time.Second

	options := []*commonpb.KeyValuePair{{Key: Timeout, Value: "300s"}}
	ts, err := GetTimeoutTs(options)
	assert.NoError(t, err)
	pt := tsoutil.PhysicalTime(ts)
	assert.WithinDuration(t, time.Now().Add(300*time.Second), pt, delta)

	options = []*commonpb.KeyValuePair{{Key: Timeout, Value: "1.5h"}}
	ts, err = GetTimeoutTs(options)
	assert.NoError(t, err)
	pt = tsoutil.PhysicalTime(ts)
	assert.WithinDuration(t, time.Now().Add(90*time.Minute), pt, delta)

	options = []*commonpb.KeyValuePair{{Key: Timeout, Value: "1h45m"}}
	ts, err = GetTimeoutTs(options)
	assert.NoError(t, err)
	pt = tsoutil.PhysicalTime(ts)
	assert.WithinDuration(t, time.Now().Add(105*time.Minute), pt, delta)

	options = []*commonpb.KeyValuePair{{Key: Timeout, Value: "invalidTime"}}
	_, err = GetTimeoutTs(options)
	assert.Error(t, err)
}

func TestOption_ParseTimeRange(t *testing.T) {
	s, e, err := ParseTimeRange(nil)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), s)
	assert.Equal(t, uint64(math.MaxUint64), e)

	startTs := tsoutil.GetCurrentTime()
	options := []*commonpb.KeyValuePair{{Key: StartTs, Value: fmt.Sprintf("%d", startTs)}}
	s, e, err = ParseTimeRange(options)
	assert.NoError(t, err)
	assert.Equal(t, startTs, s)
	assert.Equal(t, uint64(math.MaxUint64), e)

	endTs := tsoutil.GetCurrentTime()
	options = []*commonpb.KeyValuePair{{Key: EndTs, Value: fmt.Sprintf("%d", endTs)}}
	s, e, err = ParseTimeRange(options)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), s)
	assert.Equal(t, endTs, e)

	options = []*commonpb.KeyValuePair{{Key: EndTs, Value: "&%#$%^&%^&$%^&&"}}
	_, _, err = ParseTimeRange(options)
	assert.ErrorIs(t, err, merr.ErrImportFailed)

	physicalTs := time.Now().UnixMilli()
	options = []*commonpb.KeyValuePair{{Key: EndTs, Value: fmt.Sprintf("%d", physicalTs)}}
	_, _, err = ParseTimeRange(options)
	assert.ErrorIs(t, err, merr.ErrImportFailed)

	options = []*commonpb.KeyValuePair{{Key: StartTs, Value: "0"}}
	_, _, err = ParseTimeRange(options)
	assert.ErrorIs(t, err, merr.ErrImportFailed)
}
