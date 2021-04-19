package proxy

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/allocator"
)

var trueCnt = 0

func checkFunc(timestamp Timestamp) bool {
	ret := rand.Intn(2) == 1
	if ret {
		trueCnt++
	}
	return ret
}

func TestTimeTick_Start(t *testing.T) {
	fmt.Println("HHH")
}

func TestTimeTick_Start2(t *testing.T) {

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	masterAddr := Params.MasterAddress()
	tsoAllocator, err := allocator.NewTimestampAllocator(ctx, masterAddr)
	assert.Nil(t, err)
	err = tsoAllocator.Start()
	assert.Nil(t, err)

	tt := newTimeTick(ctx, tsoAllocator, Params.TimeTickInterval(), checkFunc)

	defer func() {
		cancel()
		tsoAllocator.Close()
		tt.Close()
	}()

	tt.Start()

	<-ctx.Done()

}
