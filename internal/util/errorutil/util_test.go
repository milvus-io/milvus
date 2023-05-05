package errorutil

import (
	"errors"
	"fmt"
	"testing"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestErrorList_Error(t *testing.T) {
	var el ErrorList
	for i := 0; i < 5; i++ {
		el = append(el, errors.New("error occur"))
	}
	for i := 0; i < 5; i++ {
		el = append(el, nil)
	}
	log.Debug("all errors are", zap.Error(el))
}

func TestErrorList_Error_Limit(t *testing.T) {
	var el ErrorList
	for i := 0; i < 15; i++ {
		el = append(el, errors.New("error occur"))
	}
	log.Debug("all errors are", zap.Error(el))
}

func TestErrorList_Is(t *testing.T) {
	var el ErrorList

	target := errors.New("target")

	assert.False(t, errors.Is(el, target))

	el = append(el, target)
	assert.True(t, errors.Is(el, target))

	el = nil
	el = append(el, fmt.Errorf("%w(a=b)", target))

	assert.True(t, errors.Is(el, target))
}
