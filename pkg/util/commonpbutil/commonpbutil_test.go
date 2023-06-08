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
	"testing"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/stretchr/testify/assert"
)

func TestIsHealthy(t *testing.T) {
	type testCase struct {
		code   commonpb.StateCode
		expect bool
	}

	cases := []testCase{
		{commonpb.StateCode_Healthy, true},
		{commonpb.StateCode_Initializing, false},
		{commonpb.StateCode_Abnormal, false},
		{commonpb.StateCode_StandBy, false},
		{commonpb.StateCode_Stopping, false},
	}
	for _, tc := range cases {
		t.Run(tc.code.String(), func(t *testing.T) {
			assert.Equal(t, tc.expect, IsHealthy(tc.code))
		})
	}
}

func TestIsHealthyOrStopping(t *testing.T) {
	type testCase struct {
		code   commonpb.StateCode
		expect bool
	}

	cases := []testCase{
		{commonpb.StateCode_Healthy, true},
		{commonpb.StateCode_Initializing, false},
		{commonpb.StateCode_Abnormal, false},
		{commonpb.StateCode_StandBy, false},
		{commonpb.StateCode_Stopping, true},
	}
	for _, tc := range cases {
		t.Run(tc.code.String(), func(t *testing.T) {
			assert.Equal(t, tc.expect, IsHealthyOrStopping(tc.code))
		})
	}
}
