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
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

// TestMilvusErrorWithInner covers the unified milvusError-with-inner that
// replaced wrappedMilvusError: a WrapErrXxxErr factory relabels the chain with
// its own sentinel code/type/retriability while keeping the inner reachable.
func TestMilvusErrorWithInner(t *testing.T) {
	inner := WrapErrCollectionNotLoaded(123) // typed merr, code 101
	res := WrapErrServiceInternalErr(inner, "describe failed for %d", 7)

	// relabels to the outer sentinel's code, masking the inner's
	assert.Equal(t, ErrServiceInternal.code(), Code(res))
	// both identities remain reachable
	assert.True(t, errors.Is(res, ErrServiceInternal))
	assert.True(t, errors.Is(res, ErrCollectionNotLoaded), "inner reachable via Unwrap")
	// classification + retriability follow the outer sentinel
	assert.Equal(t, SystemError, GetErrorType(res))
	assert.False(t, IsRetryableErr(res))
	// message carries both context and inner
	assert.Contains(t, res.Error(), "describe failed for 7")
	assert.Contains(t, res.Error(), "collection not loaded")
	// wire status reports the relabeled code
	assert.Equal(t, ErrServiceInternal.code(), Status(res).GetCode())

	// nil inner falls back to the plain *Msg form
	res2 := WrapErrServiceInternalErr(nil, "plain")
	assert.Equal(t, ErrServiceInternal.code(), Code(res2))
	assert.False(t, errors.Is(res2, ErrCollectionNotLoaded))

	// plain sentinels / wrapFields values carry no inner
	plain := WrapErrCollectionNotFound("foo")
	assert.Equal(t, ErrCollectionNotFound.code(), Code(plain))
	assert.True(t, errors.Is(plain, ErrCollectionNotFound))
	assert.Nil(t, errors.Unwrap(plain))
}

// TestErrorTypeMarker covers WrapErrAsInputError/WrapErrAsSysError marking the
// broad classification through the chain, including on *Msg (errors.Wrapf)
// results where a direct type assertion would have missed it.
func TestErrorTypeMarker(t *testing.T) {
	// *Msg result + mark input: code preserved, classification flipped, status carries the flag
	e := WrapErrAsInputError(WrapErrOperationNotSupportedMsg("only support bm25"))
	assert.Equal(t, InputError, GetErrorType(e))
	assert.Equal(t, ErrOperationNotSupported.code(), Code(e))
	assert.True(t, errors.Is(e, ErrOperationNotSupported))
	st := Status(e)
	assert.Equal(t, "true", st.GetExtraInfo()[InputErrorFlagKey])
	assert.False(t, st.GetRetriable(), "InputError forces Retriable=false")

	// works on a relabeling milvusError-with-inner too; inner stays reachable
	marked := WrapErrAsInputError(WrapErrServiceInternalErr(WrapErrCollectionNotLoaded(1), "ctx"))
	assert.Equal(t, InputError, GetErrorType(marked))
	assert.Equal(t, ErrServiceInternal.code(), Code(marked))
	assert.True(t, errors.Is(marked, ErrCollectionNotLoaded))

	// mark found through an outer merr.Wrap context layer
	wrapped := Wrap(WrapErrAsInputError(WrapErrServiceInternalMsg("boom")), "outer")
	assert.Equal(t, InputError, GetErrorType(wrapped))

	// sentinel-baked classification unaffected (ParameterInvalid stays InputError)
	assert.Equal(t, InputError, GetErrorType(WrapErrParameterInvalidMsg("x")))
	assert.Equal(t, SystemError, GetErrorType(WrapErrServiceInternalMsg("x")))

	// WrapErrAsSysError flips a sentinel-input error to system
	sys := WrapErrAsSysError(WrapErrParameterInvalidMsg("x"))
	assert.Equal(t, SystemError, GetErrorType(sys))

	// WrapErrAsInputErrorWhen only marks matching codes
	hit := WrapErrAsInputErrorWhen(WrapErrParameterInvalidMsg("x"), ErrParameterInvalid)
	assert.Equal(t, InputError, GetErrorType(hit))
	miss := WrapErrAsInputErrorWhen(WrapErrServiceInternalMsg("x"), ErrParameterInvalid)
	assert.Equal(t, SystemError, GetErrorType(miss))

	// nil-safety
	assert.Nil(t, WrapErrAsInputError(nil))
	assert.Nil(t, WrapErrAsSysError(nil))
}

// TestSentinelErrorTypeClassification guards the input/system split that the
// proxy fail_input/fail_system alerting relies on. A sentinel silently losing
// (or gaining) WithErrorType(InputError) flips which party an alert blames.
func TestSentinelErrorTypeClassification(t *testing.T) {
	// The request's own fault -> InputError (and therefore non-retriable).
	inputSentinels := map[string]error{
		"ParameterInvalid":          ErrParameterInvalid,
		"ParameterMissing":          ErrParameterMissing,
		"ParameterTooLarge":         ErrParameterTooLarge,
		"CollectionNotFound":        ErrCollectionNotFound,
		"CollectionLoaded":          ErrCollectionLoaded,
		"DatabaseNotFound":          ErrDatabaseNotFound,
		"ResourceGroupNotFound":     ErrResourceGroupNotFound,
		"IndexDuplicate":            ErrIndexDuplicate,
		"PrivilegeNotPermitted":     ErrPrivilegeNotPermitted,
		"PrivilegeGroupInvalidName": ErrPrivilegeGroupInvalidName,
		"NeedAuthenticate":          ErrNeedAuthenticate,
		"IncorrectParameterFormat":  ErrIncorrectParameterFormat,
		"MissingRequiredParameters": ErrMissingRequiredParameters,
		"InvalidInsertData":         ErrInvalidInsertData,
	}
	for name, err := range inputSentinels {
		assert.Equal(t, InputError, GetErrorType(err), "%s should be InputError", name)
	}

	// Topology / internal conditions stay SystemError (not the user's fault):
	// re-resolving the shard map or a node coming back is the system's job.
	systemSentinels := map[string]error{
		"ServiceInternal":     ErrServiceInternal,
		"ChannelNotFound":     ErrChannelNotFound,
		"SegmentNotFound":     ErrSegmentNotFound,
		"NodeNotFound":        ErrNodeNotFound,
		"ReplicaNotFound":     ErrReplicaNotFound,
		"PartitionNotFound":   ErrPartitionNotFound,
		"CollectionNotLoaded": ErrCollectionNotLoaded,
	}
	for name, err := range systemSentinels {
		assert.Equal(t, SystemError, GetErrorType(err), "%s should stay SystemError", name)
	}
}
