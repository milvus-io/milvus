//go:build go1.26 && !go1.27
// +build go1.26,!go1.27

package stw

import (
	"reflect"

	"github.com/bytedance/mockey/internal/monkey/fn"
	"github.com/bytedance/mockey/internal/monkey/linkname"
)

// Go 1.26 uses the same stopTheWorld/startTheWorld signatures as Go 1.23-1.25
func doStopTheWorld() (resume func()) {
	w := stopTheWorld(stwForTestResetDebugLog)
	return func() { startTheWorld(w) }
}

const stwForTestResetDebugLog = 16

type stwReason uint8

type worldStop struct {
	reason           stwReason
	startedStopping  int64
	finishedStopping int64
	stoppingCPUTime  int64
}

var (
	stopTheWorld  func(reason stwReason) worldStop
	startTheWorld func(w worldStop)
)

func init() {
	stopTheWorldPC := linkname.FuncPCForName("runtime.stopTheWorld")
	stopTheWorld = fn.MakeFunc(reflect.TypeOf(stopTheWorld), stopTheWorldPC).Interface().(func(stwReason) worldStop)
	startTheWorldPC := linkname.FuncPCForName("runtime.startTheWorld")
	startTheWorld = fn.MakeFunc(reflect.TypeOf(startTheWorld), startTheWorldPC).Interface().(func(worldStop))
}
