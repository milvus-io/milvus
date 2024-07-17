package adaptor_test

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/remeh/sizedwaitgroup"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource/idalloc"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/registry"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/walimplstest"
)

type walTestFramework struct {
	b            wal.OpenerBuilder
	t            *testing.T
	messageCount int
}

func TestWAL(t *testing.T) {
	rc := idalloc.NewMockRootCoordClient(t)
	resource.InitForTest(resource.OptRootCoordClient(rc))

	b := registry.MustGetBuilder(walimplstest.WALName)
	f := &walTestFramework{
		b:            b,
		t:            t,
		messageCount: 1000,
	}
	f.Run()
}

func (f *walTestFramework) Run() {
	wg := sync.WaitGroup{}
	loopCnt := 3
	wg.Add(loopCnt)
	o, err := f.b.Build()
	assert.NoError(f.t, err)
	assert.NotNil(f.t, o)
	defer o.Close()

	for i := 0; i < loopCnt; i++ {
		go func(i int) {
			defer wg.Done()
			f.runOnce(fmt.Sprintf("pchannel-%d", i), o)
		}(i)
	}
	wg.Wait()
}

func (f *walTestFramework) runOnce(pchannel string, o wal.Opener) {
	f2 := &testOneWALFramework{
		t:            f.t,
		opener:       o,
		pchannel:     pchannel,
		messageCount: f.messageCount,
		term:         1,
	}
	f2.Run()
}

type testOneWALFramework struct {
	t            *testing.T
	opener       wal.Opener
	written      []message.ImmutableMessage
	pchannel     string
	messageCount int
	term         int
}

func (f *testOneWALFramework) Run() {
	ctx := context.Background()
	for ; f.term <= 3; f.term++ {
		pChannel := types.PChannelInfo{
			Name: f.pchannel,
			Term: int64(f.term),
		}
		w, err := f.opener.Open(ctx, &wal.OpenOption{
			Channel: pChannel,
		})
		assert.NoError(f.t, err)
		assert.NotNil(f.t, w)
		assert.Equal(f.t, pChannel.Name, w.Channel().Name)

		f.testReadAndWrite(ctx, w)
		// close the wal
		w.Close()
	}
}

func (f *testOneWALFramework) testReadAndWrite(ctx context.Context, w wal.WAL) {
	// Test read and write.
	wg := sync.WaitGroup{}
	wg.Add(3)

	var newWritten []message.ImmutableMessage
	var read1, read2 []message.ImmutableMessage
	go func() {
		defer wg.Done()
		var err error
		newWritten, err = f.testAppend(ctx, w)
		assert.NoError(f.t, err)
	}()
	go func() {
		defer wg.Done()
		var err error
		read1, err = f.testRead(ctx, w)
		assert.NoError(f.t, err)
	}()
	go func() {
		defer wg.Done()
		var err error
		read2, err = f.testRead(ctx, w)
		assert.NoError(f.t, err)
	}()
	wg.Wait()
	// read result should be sorted by timetick.
	f.assertSortByTimeTickMessageList(read1)
	f.assertSortByTimeTickMessageList(read2)

	// all written messages should be read.
	sort.Sort(sortByMessageID(newWritten))
	f.written = append(f.written, newWritten...)
	sort.Sort(sortByMessageID(read1))
	sort.Sort(sortByMessageID(read2))
	f.assertEqualMessageList(f.written, read1)
	f.assertEqualMessageList(f.written, read2)

	// test read with option
	f.testReadWithOption(ctx, w)
}

func (f *testOneWALFramework) testAppend(ctx context.Context, w wal.WAL) ([]message.ImmutableMessage, error) {
	messages := make([]message.ImmutableMessage, f.messageCount)
	swg := sizedwaitgroup.New(10)
	for i := 0; i < f.messageCount-1; i++ {
		swg.Add()
		go func(i int) {
			defer swg.Done()
			time.Sleep(time.Duration(5+rand.Int31n(10)) * time.Millisecond)
			// ...rocksmq has a dirty implement of properties,
			// without commonpb.MsgHeader, it can not work.
			header := commonpb.MsgHeader{
				Base: &commonpb.MsgBase{
					MsgType: commonpb.MsgType_Insert,
					MsgID:   int64(i),
				},
			}
			payload, err := proto.Marshal(&header)
			if err != nil {
				panic(err)
			}
			properties := map[string]string{
				"id":    fmt.Sprintf("%d", i),
				"const": "t",
			}
			typ := message.MessageTypeUnknown
			msg := message.NewMutableMessageBuilder().
				WithMessageType(typ).
				WithPayload(payload).
				WithProperties(properties).
				BuildMutable()
			id, err := w.Append(ctx, msg)
			assert.NoError(f.t, err)
			assert.NotNil(f.t, id)
			messages[i] = msg.IntoImmutableMessage(id)
		}(i)
	}
	swg.Wait()
	// send a final hint message
	header := commonpb.MsgHeader{
		Base: &commonpb.MsgBase{
			MsgType: commonpb.MsgType_Insert,
			MsgID:   int64(f.messageCount - 1),
		},
	}
	payload, err := proto.Marshal(&header)
	if err != nil {
		panic(err)
	}
	properties := map[string]string{
		"id":    fmt.Sprintf("%d", f.messageCount-1),
		"const": "t",
		"term":  strconv.FormatInt(int64(f.term), 10),
	}
	msg := message.NewMutableMessageBuilder().
		WithPayload(payload).
		WithProperties(properties).
		WithMessageType(message.MessageTypeUnknown).
		BuildMutable()
	id, err := w.Append(ctx, msg)
	assert.NoError(f.t, err)
	messages[f.messageCount-1] = msg.IntoImmutableMessage(id)
	return messages, nil
}

func (f *testOneWALFramework) testRead(ctx context.Context, w wal.WAL) ([]message.ImmutableMessage, error) {
	s, err := w.Read(ctx, wal.ReadOption{
		DeliverPolicy: options.DeliverPolicyAll(),
	})
	assert.NoError(f.t, err)
	defer s.Close()

	expectedCnt := f.messageCount + len(f.written)
	msgs := make([]message.ImmutableMessage, 0, expectedCnt)
	for {
		msg, ok := <-s.Chan()
		assert.NotNil(f.t, msg)
		assert.True(f.t, ok)
		msgs = append(msgs, msg)
		termString, ok := msg.Properties().Get("term")
		if !ok {
			continue
		}
		term, err := strconv.ParseInt(termString, 10, 64)
		if err != nil {
			panic(err)
		}
		if int(term) == f.term {
			break
		}
	}
	return msgs, nil
}

func (f *testOneWALFramework) testReadWithOption(ctx context.Context, w wal.WAL) {
	loopCount := 5
	wg := sync.WaitGroup{}
	wg.Add(loopCount)
	for i := 0; i < loopCount; i++ {
		go func() {
			defer wg.Done()
			idx := rand.Int31n(int32(len(f.written)))
			// Test other read options.
			// Test start from some message and timetick is gte than it.
			readFromMsg := f.written[idx]
			s, err := w.Read(ctx, wal.ReadOption{
				DeliverPolicy: options.DeliverPolicyStartFrom(readFromMsg.LastConfirmedMessageID()),
				MessageFilter: func(im message.ImmutableMessage) bool {
					return im.TimeTick() >= readFromMsg.TimeTick()
				},
			})
			assert.NoError(f.t, err)
			maxTimeTick := f.maxTimeTickWritten()
			msgCount := 0
			lastTimeTick := readFromMsg.TimeTick() - 1
			for {
				msg, ok := <-s.Chan()
				msgCount++
				assert.NotNil(f.t, msg)
				assert.True(f.t, ok)
				assert.Greater(f.t, msg.TimeTick(), lastTimeTick)
				lastTimeTick = msg.TimeTick()
				if msg.TimeTick() >= maxTimeTick {
					break
				}
			}

			// shouldn't lost any message.
			assert.Equal(f.t, f.countTheTimeTick(readFromMsg.TimeTick()), msgCount)
			s.Close()
		}()
	}
	wg.Wait()
}

func (f *testOneWALFramework) assertSortByTimeTickMessageList(msgs []message.ImmutableMessage) {
	for i := 1; i < len(msgs); i++ {
		assert.Less(f.t, msgs[i-1].TimeTick(), msgs[i].TimeTick())
	}
}

func (f *testOneWALFramework) assertEqualMessageList(msgs1 []message.ImmutableMessage, msgs2 []message.ImmutableMessage) {
	assert.Equal(f.t, len(msgs2), len(msgs1))
	for i := 0; i < len(msgs1); i++ {
		assert.True(f.t, msgs1[i].MessageID().EQ(msgs2[i].MessageID()))
		// assert.True(f.t, bytes.Equal(msgs1[i].Payload(), msgs2[i].Payload()))
		id1, ok1 := msgs1[i].Properties().Get("id")
		id2, ok2 := msgs2[i].Properties().Get("id")
		assert.True(f.t, ok1)
		assert.True(f.t, ok2)
		assert.Equal(f.t, id1, id2)
		id1, ok1 = msgs1[i].Properties().Get("const")
		id2, ok2 = msgs2[i].Properties().Get("const")
		assert.True(f.t, ok1)
		assert.True(f.t, ok2)
		assert.Equal(f.t, id1, id2)
	}
}

func (f *testOneWALFramework) countTheTimeTick(begin uint64) int {
	cnt := 0
	for _, m := range f.written {
		if m.TimeTick() >= begin {
			cnt++
		}
	}
	return cnt
}

func (f *testOneWALFramework) maxTimeTickWritten() uint64 {
	maxTimeTick := uint64(0)
	for _, m := range f.written {
		if m.TimeTick() > maxTimeTick {
			maxTimeTick = m.TimeTick()
		}
	}
	return maxTimeTick
}

type sortByMessageID []message.ImmutableMessage

func (a sortByMessageID) Len() int {
	return len(a)
}

func (a sortByMessageID) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

func (a sortByMessageID) Less(i, j int) bool {
	return a[i].MessageID().LT(a[j].MessageID())
}
