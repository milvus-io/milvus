package datacoord

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestImportErrorClassification(t *testing.T) {
	for _, err := range []error{
		merr.WrapErrImportFailedMsg("bad source"),
		merr.WrapErrDataIntegrityMsg("bad manifest"),
		merr.WrapErrImportSysFailedMsg("bad plan"),
	} {
		assert.True(t, isImportTerminalError(err))
		assert.Equal(t, merr.Code(err), importFailureCode(err))
	}
	assert.Equal(t, int32(0), importFailureCode(nil))

	assert.True(t, isImportOwnershipLost(merr.WrapErrNodeNotFound(10)))
	assert.False(t, isImportOwnershipLost(merr.WrapErrIoKeyNotFound("manifest")))
	assert.True(t, isImportTerminalError(merr.WrapErrIoKeyNotFound("manifest")))
}
