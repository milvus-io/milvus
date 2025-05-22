package interceptor

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

func TestConvertMilvusErrorIntoStreamingError(t *testing.T) {
	err := merr.WrapErrNodeNotMatch(1, 2)
	err = convertMilvusErrorIntoStreamingError(err)
	s := status.AsStreamingError(err)
	assert.True(t, s.IsSkippedOperation())

	err = merr.WrapErrServiceCrossClusterRouting("1", "2")
	err = convertMilvusErrorIntoStreamingError(err)
	s = status.AsStreamingError(err)
	assert.True(t, s.IsSkippedOperation())
}
