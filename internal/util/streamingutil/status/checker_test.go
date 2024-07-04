package status

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsCanceled(t *testing.T) {
	assert.False(t, IsCanceled(nil))
	assert.True(t, IsCanceled(context.DeadlineExceeded))
	assert.True(t, IsCanceled(context.Canceled))
	assert.True(t, IsCanceled(status.Error(codes.Canceled, "test")))
	assert.True(t, IsCanceled(ConvertStreamingError("test", status.Error(codes.Canceled, "test"))))
	assert.False(t, IsCanceled(ConvertStreamingError("test", status.Error(codes.Unknown, "test"))))
}
