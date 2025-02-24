package logutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

func TestName(t *testing.T) {
	conf := &log.Config{Level: "debug", DisableTimestamp: true}
	logger, _, _ := log.InitTestLogger(t, conf, zap.AddCallerSkip(1), zap.Hooks(func(entry zapcore.Entry) error {
		assert.Equal(t, "Testing", entry.Message)
		return nil
	}))
	wrapper := &zapWrapper{logger, 0}

	wrapper.Info("Testing")
	wrapper.Infoln("Testing")
	wrapper.Infof("%s", "Testing")
	wrapper.Warning("Testing")
	wrapper.Warningln("Testing")
	wrapper.Warningf("%s", "Testing")
	wrapper.Error("Testing")
	wrapper.Errorln("Testing")
	wrapper.Errorf("%s", "Testing")
}
