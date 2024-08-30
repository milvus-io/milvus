package block

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walcache/rm"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/walimplstest"
)

func TestBlockStatefulWithMemory(t *testing.T) {
	rm.Init(1000, 0, "")
	runTest(t)
}

func TestBlockStatefulWithDisk(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "TestBlockStateful")
	if err != nil {
		panic(err)
	}
	rm.Init(1000, 3000, dir)
	runTest(t)
}

func runTest(t *testing.T) {
	wg := sync.WaitGroup{}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b := createImmutableBlock()
			time.Sleep(time.Duration(rand.Int31n(30)) * time.Millisecond)
			scanner, err := b.Read(b.Range().Begin)
			if err != nil {
				assert.ErrorIs(t, err, walcache.ErrEvicted)
				return
			}
			idx := int64(0)
			for {
				err := scanner.Scan(context.Background())
				if err != nil {
					assert.ErrorIs(t, err, io.EOF)
					break
				}
				assert.True(t, bytes.Equal(
					[]byte(strconv.FormatInt(idx, 10)),
					scanner.Message().Payload()))
				idx++
			}
		}()
	}
	wg.Wait()
}

func createImmutableBlock() *ImmutableBlock {
	msgs := make([]message.ImmutableMessage, 0, 100)
	size := 0
	for i := 0; i < 100; i++ {
		payload := []byte(strconv.FormatInt(int64(i), 10))
		size += len(payload)
		msgs = append(msgs, message.NewImmutableMesasge(
			walimplstest.NewTestMessageID(int64(i)),
			payload,
			map[string]string{
				"key": strconv.FormatInt(int64(i), 10),
			},
		))
	}
	return newImmutableBlock(msgs, size, nil)
}
