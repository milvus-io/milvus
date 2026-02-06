//go:build test

package mlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zapcore"
)

func TestLevelConstants(t *testing.T) {
	// Level constants should match zapcore constants
	assert.Equal(t, zapcore.DebugLevel, DebugLevel)
	assert.Equal(t, zapcore.InfoLevel, InfoLevel)
	assert.Equal(t, zapcore.WarnLevel, WarnLevel)
	assert.Equal(t, zapcore.ErrorLevel, ErrorLevel)
}

func TestSetAndGetLevel(t *testing.T) {
	// Save original level and restore after test
	original := GetLevel()
	defer SetLevel(original)

	// Test setting to debug
	SetLevel(DebugLevel)
	assert.Equal(t, DebugLevel, GetLevel())

	// Test setting to warn
	SetLevel(WarnLevel)
	assert.Equal(t, WarnLevel, GetLevel())

	// Test setting to error
	SetLevel(ErrorLevel)
	assert.Equal(t, ErrorLevel, GetLevel())
}

func TestGetAtomicLevel(t *testing.T) {
	// GetAtomicLevel should return the same level as GetLevel
	atomicLevel := GetAtomicLevel()
	assert.Equal(t, GetLevel(), atomicLevel.Level())

	// Changing via AtomicLevel should be reflected in GetLevel
	original := GetLevel()
	defer SetLevel(original)

	atomicLevel.SetLevel(WarnLevel)
	assert.Equal(t, WarnLevel, GetLevel())
}
