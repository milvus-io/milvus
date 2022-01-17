package proxy

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIDCache_SetGet(t *testing.T) {
	cache := newIDCache(time.Hour, time.Hour)
	// not exist before set
	_, exist := cache.Get(1)
	assert.False(t, exist)
	cache.Set(1, true)
	// exist after set & before expire
	value, exist := cache.Get(1)
	assert.True(t, exist)
	assert.True(t, value)

	cache = newIDCache(time.Millisecond, time.Hour)
	cache.Set(1, true)
	<-time.After(time.Millisecond)
	// not exists after set & expire
	_, exist = cache.Get(1)
	assert.False(t, exist)
}
