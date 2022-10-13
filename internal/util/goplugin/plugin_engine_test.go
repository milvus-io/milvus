package goplugin

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_Plugin_Engine(t *testing.T) {
	pe := PluginEngine{}

	err := pe.Init("./plugin.go")
	assert.NoError(t, err)
	helloFunc, err := pe.GetFunc("goplugin.HelloWorld")
	assert.NoError(t, err)

	hello := helloFunc.(func())
	hello()

	AddFunc, err := pe.GetFunc("goplugin.Add")
	assert.NoError(t, err)

	add := AddFunc.(func(a, b int) int)
	i := add(3, 5)
	assert.Equal(t, i, 8)

	AddOnceFunc, err := pe.GetFunc("goplugin.AddOnce")
	assert.NoError(t, err)
	addOnce := AddOnceFunc.(func())
	addOnce()
	addOnce()
	addOnce()

	GetFunc, err := pe.GetFunc("goplugin.GetNum")
	assert.NoError(t, err)
	getFunc := GetFunc.(func() int)
	assert.Equal(t, getFunc(), 3)
}
