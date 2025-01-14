package syncutil

import (
	"testing"
	"time"
)

func TestFuture_SetAndGet(t *testing.T) {
	f := NewFuture[int]()
	go func() {
		time.Sleep(1 * time.Second) // Simulate some work
		f.Set(42)
	}()

	val := f.Get()
	if val != 42 {
		t.Errorf("Expected value 42, got %d", val)
	}
}

func TestFuture_Done(t *testing.T) {
	f := NewFuture[string]()
	go func() {
		f.Set("done")
	}()

	select {
	case <-f.Done():
		// Success
	case <-time.After(20 * time.Millisecond):
		t.Error("Expected future to be done within 2 seconds")
	}
}

func TestFuture_Ready(t *testing.T) {
	f := NewFuture[float64]()
	go func() {
		time.Sleep(20 * time.Millisecond) // Simulate some work
		f.Set(3.14)
	}()

	if f.Ready() {
		t.Error("Expected future not to be ready immediately")
	}

	<-f.Done() // Wait for the future to be set

	if !f.Ready() {
		t.Error("Expected future to be ready after being set")
	}
}
