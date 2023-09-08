package resource

import (
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

func TestResourceManager(t *testing.T) {
	{
		manager := NewManager(0, 0, nil)
		manager.Close()
	}

	manager := NewManager(500*time.Millisecond, 2*time.Second, map[string]time.Duration{
		"test": time.Second,
	})
	defer manager.Close()
	{
		assert.Nil(t, manager.Delete("test", "test"))
		res, err := manager.Get("stream", "foo", func() (Resource, error) {
			return NewSimpleResource("stream-foo", "stream", "foo", 0, nil), nil
		})
		assert.NoError(t, err)
		assert.Equal(t, 2*time.Second, res.KeepAliveTime())
		assert.Equal(t, "stream-foo", res.Get())
	}
	{
		_, err := manager.Get("err", "foo", func() (Resource, error) {
			return nil, errors.New("mock test error")
		})
		assert.Error(t, err)
	}
	{
		res, err := manager.Get("test", "foo", func() (Resource, error) {
			return NewSimpleResource("foo", "test", "foo", 0, nil), nil
		})
		assert.NoError(t, err)
		assert.Equal(t, "foo", res.Get())

		assert.Nil(t, manager.Delete("test", "test"))
	}
	{
		time.Sleep(500 * time.Millisecond)
		res, err := manager.Get("test", "foo", func() (Resource, error) {
			return NewSimpleResource("foox", "test", "foo", 0, nil), nil
		})
		assert.NoError(t, err)
		assert.Equal(t, "foo", res.Get())
	}
	{
		time.Sleep(3 * time.Second)
		res, err := manager.Get("test", "foo", func() (Resource, error) {
			return NewSimpleResource("foo2", "test", "foo", 0, nil), nil
		})
		assert.NoError(t, err)
		assert.Equal(t, "foo2", res.Get(), res.KeepAliveTime())
	}
	{
		res := manager.Delete("test", "foo")
		assert.Equal(t, "foo2", res.Get())
		res = manager.Delete("test", "foo")
		assert.Equal(t, "foo2", res.Get())
		time.Sleep(time.Second)

		res, err := manager.Get("test", "foo", func() (Resource, error) {
			return NewSimpleResource("foo3", "test", "foo", 0, nil), nil
		})
		assert.NoError(t, err)
		assert.Equal(t, "foo3", res.Get())
	}
	{
		time.Sleep(2 * time.Second)
		res, err := manager.Get("stream", "foo", func() (Resource, error) {
			return NewSimpleResource("stream-foox", "stream", "foo", 0, nil), nil
		})
		assert.NoError(t, err)
		assert.Equal(t, "stream-foox", res.Get())
	}
	{
		var res Resource
		var err error
		res, err = manager.Get("ever", "foo", func() (Resource, error) {
			return NewSimpleResource("ever-foo", "ever", "foo", NoExpiration, nil), nil
		})
		assert.NoError(t, err)
		assert.Equal(t, "ever-foo", res.Get())

		res, err = manager.Get("ever", "foo", func() (Resource, error) {
			return NewSimpleResource("ever-foo2", "ever", "foo", NoExpiration, nil), nil
		})
		assert.NoError(t, err)
		assert.Equal(t, "ever-foo", res.Get())

		manager.Delete("ever", "foo")
		time.Sleep(time.Second)
		res, err = manager.Get("ever", "foo", func() (Resource, error) {
			return NewSimpleResource("ever-foo3", "ever", "foo", NoExpiration, nil), nil
		})
		assert.NoError(t, err)
		assert.Equal(t, "ever-foo3", res.Get())
	}
}

func TestResource(t *testing.T) {
	{
		isClose := false
		res := NewSimpleResource("obj", "test", "foo", 0, func() {
			isClose = true
		})
		assert.Equal(t, "test", res.Type())
		assert.Equal(t, "foo", res.Name())
		assert.Equal(t, "obj", res.Get())
		assert.EqualValues(t, 0, res.KeepAliveTime())
		res.Close()
		assert.True(t, isClose)
	}

	{
		res := NewResource()
		assert.Empty(t, res.Type())
		assert.Empty(t, res.Name())
		assert.Empty(t, res.Get())
		assert.EqualValues(t, 0, res.KeepAliveTime())
	}

	{
		isClose := false
		res := NewSimpleResource("obj", "test", "foo", 0, func() {
			isClose = true
		})
		isClose2 := false
		wrapper := NewResource(WithResource(res), WithType("test2"), WithName("foo2"), WithObj("obj2"), WithKeepAliveTime(time.Second), WithCloseFunc(func() {
			isClose2 = true
		}))
		wrapper.Close()
		assert.Equal(t, "test2", wrapper.Type())
		assert.Equal(t, "foo2", wrapper.Name())
		assert.Equal(t, "obj2", wrapper.Get())
		assert.Equal(t, time.Second, wrapper.KeepAliveTime())
		assert.True(t, isClose)
		assert.True(t, isClose2)
	}

	{
		isClose := false
		res := NewSimpleResource("obj", "test", "foo", 0, func() {
			isClose = true
		})
		wrapper := NewResource(WithResource(res))
		assert.Equal(t, "test", wrapper.Type())
		assert.Equal(t, "foo", wrapper.Name())
		assert.Equal(t, "obj", wrapper.Get())
		assert.EqualValues(t, 0, wrapper.KeepAliveTime())
		wrapper.Close()
		assert.True(t, isClose)
	}
}
