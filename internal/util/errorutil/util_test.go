package errorutil

import (
	"errors"
	"testing"

	"github.com/milvus-io/milvus/internal/log"
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
