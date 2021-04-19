package log

import (
	"testing"

	"github.com/stretchr/testify/assert"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

func TestLumberjack(t *testing.T) {
	l := &lumberjack.Logger{
		Filename:   "/var/log/milvus/xxoo",
		MaxSize:    100,
		MaxAge:     1,
		MaxBackups: 2,
		LocalTime:  true,
	}
	_, err := l.Write([]byte{1, 1})
	assert.Nil(t, err)
	err = l.Close()
	assert.Nil(t, err)
}
