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
	"context"
	"errors"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// StatusError uses stable, redacted errors that survive Milvus status/RPC
// conversion. Only Transient is retryable; denial/expiry never use legacy auth.
func StatusError(status CredentialStatus) error {
	switch status {
	case CredentialOK:
		return nil
	case CredentialDenied:
		return merr.ErrSourceAuthDenied
	case CredentialExpired:
		return merr.ErrSourceAuthExpired
	case CredentialTransient:
		return merr.ErrSourceAuthTransient
	case CredentialCancelled:
		return merr.ErrSourceAuthCancelled
	default:
		return merr.ErrSourceAuthInvalidResponse
	}
}

func StatusFromError(err error) CredentialStatus {
	switch {
	case err == nil:
		return CredentialOK
	case errors.Is(err, context.Canceled), errors.Is(err, merr.ErrSourceAuthCancelled):
		return CredentialCancelled
	case errors.Is(err, merr.ErrSourceAuthExpired):
		return CredentialExpired
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, merr.ErrSourceAuthTransient), errors.Is(err, merr.ErrServiceUnavailable):
		return CredentialTransient
	case errors.Is(err, merr.ErrSourceAuthDenied), errors.Is(err, merr.ErrPrivilegeNotPermitted), errors.Is(err, merr.ErrServiceUnimplemented):
		return CredentialDenied
	default:
		return CredentialInvalidResponse
	}
}

// ErrorFromNativeCode recognizes only the published native source-context ABI.
// Unknown Loon codes keep the caller's existing storage error mapping.
func ErrorFromNativeCode(code int) (error, bool) {
	if code < 117 || code > 121 {
		return nil, false
	}
	return StatusError(CredentialStatus(code - 116)), true
}
