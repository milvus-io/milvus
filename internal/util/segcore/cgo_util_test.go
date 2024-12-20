package segcore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func TestConsumeCStatusIntoError(t *testing.T) {
	err := ConsumeCStatusIntoError(nil)
	assert.NoError(t, err)
}

func TestGetLocalUsedSize(t *testing.T) {
	initcore.InitLocalChunkManager(typeutil.QueryNodeRole, "/tmp/")
	size, err := GetLocalUsedSize(context.Background(), "")
	assert.NoError(t, err)
	assert.NotNil(t, size)
}
