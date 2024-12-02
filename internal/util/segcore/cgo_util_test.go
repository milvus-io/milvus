package segcore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConsumeCStatusIntoError(t *testing.T) {
	err := ConsumeCStatusIntoError(nil)
	assert.NoError(t, err)
}

func TestGetLocalUsedSize(t *testing.T) {
	size, err := GetLocalUsedSize(context.Background(), "")
	assert.NoError(t, err)
	assert.NotNil(t, size)
}
