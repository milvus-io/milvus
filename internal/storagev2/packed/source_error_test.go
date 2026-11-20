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

package packed

import (
	"errors"
	"testing"

	crdberrors "github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/sourceauth"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSourceAuthorizationNativeConsumersErrorClassification(t *testing.T) {
	for code := 117; code <= 121; code++ {
		cause, ok := sourceauth.ErrorFromNativeCode(code)
		require.True(t, ok)
		status := sourceauth.StatusFromError(cause)
		marker, other := ErrLoonPermanent, ErrLoonTransient
		if status == sourceauth.CredentialTransient {
			marker, other = other, marker
		}
		wrapped := &sourceFFIError{cause: cause, marker: marker}
		for name, err := range map[string]error{
			"direct":            wrapped,
			"merr context":      merr.Wrap(wrapped, "read source"),
			"cockroach context": crdberrors.Wrap(wrapped, "read source"),
			"storage wrapper":   merr.WrapErrStorage(wrapped, "read source"),
		} {
			t.Run(name+cause.Error(), func(t *testing.T) {
				require.Equal(t, status, sourceauth.StatusFromError(err))
				for _, is := range []func(error, error) bool{errors.Is, crdberrors.Is} {
					require.True(t, is(err, cause))
					require.True(t, is(err, marker))
					require.False(t, is(err, other))
				}
				if name != "storage wrapper" { // This existing wrapper deliberately relabels the wire code.
					require.Equal(t, merr.Code(cause), merr.Code(err))
					roundTrip := merr.Error(merr.Status(err))
					require.Equal(t, status, sourceauth.StatusFromError(roundTrip))
					require.Equal(t, status == sourceauth.CredentialTransient, merr.Status(err).GetRetriable())
				}
			})
		}
	}
}
