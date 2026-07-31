package adaptor

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
)

func TestBuildInterceptorsReleasesInitialRecoverySnapshot(t *testing.T) {
	param := &interceptors.InterceptorBuildParam{
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{},
	}
	result := buildInterceptorsAndReleaseInitialSnapshot(nil, param)
	t.Cleanup(result.Close)

	assert.Nil(t, param.InitialRecoverSnapshot)
}
