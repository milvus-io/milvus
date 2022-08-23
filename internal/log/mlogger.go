package log

import "go.uber.org/zap"

type MLogger struct {
	*zap.Logger
}

func (l *MLogger) RatedDebug(cost float64, msg string, fields ...zap.Field) bool {
	if R().CheckCredit(cost) {
		l.Debug(msg, fields...)
		return true
	}
	return false
}

func (l *MLogger) RatedInfo(cost float64, msg string, fields ...zap.Field) bool {
	if R().CheckCredit(cost) {
		l.Info(msg, fields...)
		return true
	}
	return false
}

func (l *MLogger) RatedWarn(cost float64, msg string, fields ...zap.Field) bool {
	if R().CheckCredit(cost) {
		l.Warn(msg, fields...)
		return true
	}
	return false
}
