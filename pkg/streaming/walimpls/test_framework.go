//go:build test
// +build test

package walimpls

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/remeh/sizedwaitgroup"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/options"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randString(l int) string {
	builder := strings.Builder{}
	for i := 0; i < l; i++ {
		builder.WriteRune(letters[rand.Intn(len(letters))])
	}
	return builder.String()
}

type walImplsTestFramework struct {
	b            OpenerBuilderImpls
	t            *testing.T
	messageCount int
}

func NewWALImplsTestFramework(t *testing.T, messageCount int, b OpenerBuilderImpls) *walImplsTestFramework {
	return &walImplsTestFramework{
		b:            b,
		t:            t,
		messageCount: messageCount,
	}
}

// Run runs the test framework.
// if test failed, a error will be returned.
func (f walImplsTestFramework) Run() {
	// create opener.
	o, err := f.b.Build()
	assert.NoError(f.t, err)
	assert.NotNil(f.t, o)
	defer o.Close()

	// Test on multi pchannels
	wg := sync.WaitGroup{}
	pchannelCnt := 3
	wg.Add(pchannelCnt)
	for i := 0; i < pchannelCnt; i++ {
		// construct pChannel
		name := fmt.Sprintf("test_%d_%s", i, randString(4))
		go func(name string) {
			defer wg.Done()
			newTestOneWALImpls(f.t, o, name, f.messageCount).Run()
		}(name)
	}
	wg.Wait()
}

func newTestOneWALImpls(t *testing.T, opener OpenerImpls, pchannel string, messageCount int) *testOneWALImplsFramework {
	return &testOneWALImplsFramework{
		t:            t,
		opener:       opener,
		pchannel:     pchannel,
		written:      make([]message.ImmutableMessage, 0),
		messageCount: messageCount,
		term:         1,
	}
}

type testOneWALImplsFramework struct {
	t            *testing.T
	opener       OpenerImpls
	written      []message.ImmutableMessage
	pchannel     string
	messageCount int
	term         int
}

func (f *testOneWALImplsFramework) Run() {
	ctx := context.Background()

	// test a read write loop
	for ; f.term <= 3; f.term++ {
		pChannel := types.PChannelInfo{
			Name: f.pchannel,
			Term: int64(f.term),
		}
		// create a wal.
		w, err := f.opener.Open(ctx, &OpenOption{
			Channel: pChannel,
		})
		assert.NoError(f.t, err)
		assert.NotNil(f.t, w)
		assert.Equal(f.t, pChannel.Name, w.Channel().Name)
		assert.Equal(f.t, pChannel.Term, w.Channel().Term)

		f.testReadAndWrite(ctx, w)
		// close the wal
		w.Close()
	}
}

func (f *testOneWALImplsFramework) testReadAndWrite(ctx context.Context, w WALImpls) {
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
		read1, err = f.testRead(ctx, w, "scanner1")
		assert.NoError(f.t, err)
	}()
	go func() {
		defer wg.Done()
		var err error
		read2, err = f.testRead(ctx, w, "scanner2")
		assert.NoError(f.t, err)
	}()

	wg.Wait()

	f.assertSortedMessageList(read1)
	f.assertSortedMessageList(read2)
	sort.Sort(sortByMessageID(newWritten))
	f.written = append(f.written, newWritten...)
	f.assertSortedMessageList(f.written)
	f.assertEqualMessageList(f.written, read1)
	f.assertEqualMessageList(f.written, read2)

	// Test different scan policy, StartFrom.
	readFromIdx := len(f.written) / 2
	readFromMsgID := f.written[readFromIdx].MessageID()
	s, err := w.Read(ctx, ReadOption{
		Name:          "scanner_deliver_start_from",
		DeliverPolicy: options.DeliverPolicyStartFrom(readFromMsgID),
	})
	assert.NoError(f.t, err)
	for i := readFromIdx; i < len(f.written); i++ {
		msg, ok := <-s.Chan()
		assert.NotNil(f.t, msg)
		assert.True(f.t, ok)
		assert.True(f.t, msg.MessageID().EQ(f.written[i].MessageID()))
	}
	s.Close()

	// Test different scan policy, StartAfter.
	s, err = w.Read(ctx, ReadOption{
		Name:          "scanner_deliver_start_after",
		DeliverPolicy: options.DeliverPolicyStartAfter(readFromMsgID),
	})
	assert.NoError(f.t, err)
	for i := readFromIdx + 1; i < len(f.written); i++ {
		msg, ok := <-s.Chan()
		assert.NotNil(f.t, msg)
		assert.True(f.t, ok)
		assert.True(f.t, msg.MessageID().EQ(f.written[i].MessageID()))
	}
	s.Close()

	// Test different scan policy, Latest.
	s, err = w.Read(ctx, ReadOption{
		Name:          "scanner_deliver_latest",
		DeliverPolicy: options.DeliverPolicyLatest(),
	})
	assert.NoError(f.t, err)
	timeoutCh := time.After(1 * time.Second)
	select {
	case <-s.Chan():
		f.t.Errorf("should be blocked")
	case <-timeoutCh:
	}
	s.Close()
}

func (f *testOneWALImplsFramework) assertSortedMessageList(msgs []message.ImmutableMessage) {
	for i := 1; i < len(msgs); i++ {
		assert.True(f.t, msgs[i-1].MessageID().LT(msgs[i].MessageID()))
	}
}

func (f *testOneWALImplsFramework) assertEqualMessageList(msgs1 []message.ImmutableMessage, msgs2 []message.ImmutableMessage) {
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

func (f *testOneWALImplsFramework) testAppend(ctx context.Context, w WALImpls) ([]message.ImmutableMessage, error) {
	ids := make([]message.ImmutableMessage, f.messageCount)
	swg := sizedwaitgroup.New(5)
	for i := 0; i < f.messageCount-1; i++ {
		swg.Add()
		go func(i int) {
			defer swg.Done()
			// ...rocksmq has a dirty implement of properties,
			// without commonpb.MsgHeader, it can not work.
			properties := map[string]string{
				"id":    fmt.Sprintf("%d", i),
				"const": "t",
			}
			msg := message.CreateTestEmptyInsertMesage(int64(i), properties)
			id, err := w.Append(ctx, msg)
			assert.NoError(f.t, err)
			assert.NotNil(f.t, id)
			ids[i] = msg.IntoImmutableMessage(id)
		}(i)
	}
	swg.Wait()

	properties := map[string]string{
		"id":    fmt.Sprintf("%d", f.messageCount-1),
		"const": "t",
		"term":  strconv.FormatInt(int64(f.term), 10),
	}
	msg, err := message.NewTimeTickMessageBuilderV1().
		WithHeader(&message.TimeTickMessageHeader{}).
		WithBody(&msgpb.TimeTickMsg{
			Base: &commonpb.MsgBase{
				MsgType: commonpb.MsgType_TimeTick,
				MsgID:   int64(f.messageCount - 1),
			},
		}).
		WithVChannel("v1").
		WithProperties(properties).BuildMutable()
	assert.NoError(f.t, err)

	id, err := w.Append(ctx, msg)
	assert.NoError(f.t, err)
	ids[f.messageCount-1] = msg.IntoImmutableMessage(id)
	return ids, nil
}

func (f *testOneWALImplsFramework) testRead(ctx context.Context, w WALImpls, name string) ([]message.ImmutableMessage, error) {
	s, err := w.Read(ctx, ReadOption{
		Name:                name,
		DeliverPolicy:       options.DeliverPolicyAll(),
		ReadAheadBufferSize: 128,
	})
	assert.NoError(f.t, err)
	assert.Equal(f.t, name, s.Name())
	defer s.Close()

	expectedCnt := f.messageCount + len(f.written)
	msgs := make([]message.ImmutableMessage, 0, expectedCnt)
	for {
		msg, ok := <-s.Chan()
		assert.NotNil(f.t, msg)
		assert.True(f.t, ok)
		msgs = append(msgs, msg)
		if msg.MessageType() == message.MessageTypeTimeTick {
			termString, ok := msg.Properties().Get("term")
			if !ok {
				panic("lost term properties")
			}
			term, err := strconv.ParseInt(termString, 10, 64)
			if err != nil {
				panic(err)
			}
			if int(term) == f.term {
				break
			}
		}
	}
	return msgs, nil
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
