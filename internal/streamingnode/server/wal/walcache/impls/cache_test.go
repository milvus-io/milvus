package impls

import (
	"context"
	"errors"
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

func TestCache(t *testing.T) {
	dir, err := os.MkdirTemp(os.TempDir(), "TestCache")
	if err != nil {
		panic(err)
	}
	rm.Init(1000, 3000, dir)

	wg := sync.WaitGroup{}
	caches := make([]*Cache, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		caches[i] = NewCache(200)
		go func(i int) {
			defer wg.Done()
			testCache(t, caches[i])
		}(i)
	}
	wg.Wait()
}

func testCache(t *testing.T, c *Cache) {
	msgCount := 3000
	msg := createImmutableMessage(0)
	c.Append([]message.ImmutableMessage{msg})

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		msgs := make([]message.ImmutableMessage, 0)
		for i := 1; i < msgCount; i++ {
			time.Sleep(1 * time.Millisecond)
			msg := createImmutableMessage(i)
			msgs = append(msgs, msg)
			if len(msgs) < rand.Intn(10) {
				continue
			}
			c.Append(msgs)
			msgs = make([]message.ImmutableMessage, 0)
		}
		if len(msgs) > 0 {
			c.Append(msgs)
		}
		c.RecordIncontinuous()
	}()
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := c.Read(walimplstest.NewTestMessageID(0))
			if err != nil {
				assert.NoError(t, err)
			}
			cnt := 0
			for {
				if err := s.Scan(context.Background()); err != nil {
					if errors.Is(err, io.EOF) {
						assert.Equal(t, msgCount, cnt)
						return
					}
					assert.ErrorIs(t, err, walcache.ErrEvicted)
					return
				}
				cnt++
			}
		}()
	}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := c.Read(walimplstest.NewTestMessageID(0))
			if err != nil {
				assert.NoError(t, err)
			}
			cnt := 0
			for {
				if err := s.Scan(context.Background()); err != nil {
					if errors.Is(err, io.EOF) {
						assert.Equal(t, msgCount, cnt)
						return
					}
					assert.ErrorIs(t, err, walcache.ErrEvicted)
					return
				}
				time.Sleep(10 * time.Millisecond)
				cnt++
			}
		}()
	}
	wg.Wait()
}

func createImmutableMessage(i int) message.ImmutableMessage {
	payload := []byte(strconv.FormatInt(int64(i), 10))
	return message.NewImmutableMesasge(
		walimplstest.NewTestMessageID(int64(i)),
		payload,
		map[string]string{
			"key": strconv.FormatInt(int64(i), 10),
		},
	)
}
