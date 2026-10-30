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
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
)

func TestWrapErrIoFailedErrPreservesCauseAndClassification(t *testing.T) {
	for _, cause := range []error{rpctypes.ErrLeaderChanged, rpctypes.ErrPermissionDenied, context.Canceled, context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			previous := WrapErrIoFailedReason("execute etcd txn failed", cause.Error())
			err := Wrap(WrapErrIoFailedErr(cause, "execute %s txn failed", "etcd"), "save resource group")
			require.ErrorIs(t, err, cause)
			require.ErrorIs(t, err, ErrIoFailed)
			require.EqualValues(t, 1001, Code(err))
			require.Equal(t, GetErrorType(previous), GetErrorType(err))
			require.Equal(t, IsRetryableErr(previous), IsRetryableErr(err))
			oldStatus, newStatus := Status(previous), Status(err)
			require.Equal(t, oldStatus.GetCode(), newStatus.GetCode())
			require.Equal(t, oldStatus.GetErrorCode(), newStatus.GetErrorCode())
			require.Equal(t, oldStatus.GetRetriable(), newStatus.GetRetriable())
			if _, ok := cause.(rpctypes.EtcdError); ok {
				var etcdErr rpctypes.EtcdError
				require.ErrorAs(t, err, &etcdErr)
				require.Equal(t, cause, etcdErr)
			}
		})
	}
	require.ErrorIs(t, WrapErrIoFailedErr(nil, "IO failure"), ErrIoFailed)
}
