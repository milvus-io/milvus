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

package merr

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestChannelWorkerUnavailableRoundTrip(t *testing.T) {
	cause := context.DeadlineExceeded // a local dial deadline, classified by the producer
	err := WrapErrChannelWorkerUnavailable(cause)
	require.ErrorIs(t, err, cause)
	require.ErrorIs(t, err, ErrChannelWorkerUnavailable)
	for i := 0; i < 3; i++ {
		st := Status(Wrap(err, "delegator search"))
		require.Equal(t, Code(ErrChannelWorkerUnavailable), st.GetCode())
		require.True(t, st.GetRetriable())
		require.Empty(t, st.GetExtraInfo()[InputErrorFlagKey])
		err = Error(st)
		require.ErrorIs(t, err, ErrChannelWorkerUnavailable)
		require.Equal(t, SystemError, GetErrorType(err))
		require.True(t, IsRetryableErr(err))
	}
}
