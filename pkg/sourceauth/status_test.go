// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sourceauth

import (
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestSourceStatusSurvivesNativeAndWorkerBoundaries(t *testing.T) {
	for status := CredentialDenied; status <= CredentialInvalidResponse; status++ {
		code := 116 + int(status)
		native, ok := ErrorFromNativeCode(code)
		require.True(t, ok)
		require.Equal(t, status, StatusFromError(native))
		for _, err := range []error{native, merr.SegcoreError(int32(code), "safe native failure")} {
			restored := merr.Error(merr.Status(err))
			require.Equal(t, status, StatusFromError(restored))
			require.Equal(t, status == CredentialTransient, merr.IsRetryableErr(restored))
			require.Equal(t, status != CredentialTransient, merr.IsNonRetryableErr(restored))
			require.Equal(t, status != CredentialTransient, merr.IsPermanentSourceAuthErr(restored))
		}
		require.Equal(t, status != CredentialTransient, merr.IsPermanentSegcoreErr(merr.SegcoreError(int32(code), "")))
	}
	_, ok := ErrorFromNativeCode(116)
	require.False(t, ok)
	_, ok = ErrorFromNativeCode(122)
	require.False(t, ok)
}
