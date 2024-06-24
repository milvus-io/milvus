//go:build test
// +build test

package walimpls

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/remeh/sizedwaitgroup"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/proto/streamingpb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/message"
	"github.com/milvus-io/milvus/internal/util/streamingutil/options"
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

	// construct pChannel
	name := "test_" + randString(4)
	pChannel := &streamingpb.PChannelInfo{
		Name:          name,
		Term:          1,
		ServerID:      1,
		VChannelInfos: []*streamingpb.VChannelInfo{},
	}
	ctx := context.Background()

	// create a wal.
	w, err := o.Open(ctx, &OpenOption{
		Channel: pChannel,
	})
	assert.NoError(f.t, err)
	assert.NotNil(f.t, w)
	defer w.Close()

	f.testReadAndWrite(ctx, w)
}

func (f walImplsTestFramework) testReadAndWrite(ctx context.Context, w WALImpls) {
	// Test read and write.
	wg := sync.WaitGroup{}
	wg.Add(3)

	var written []message.ImmutableMessage
	var read1, read2 []message.ImmutableMessage
	go func() {
		defer wg.Done()
		var err error
		written, err = f.testAppend(ctx, w)
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
	sort.Sort(sortByMessageID(written))
	f.assertEqualMessageList(written, read1)
	f.assertEqualMessageList(written, read2)

	// Test different scan policy, StartFrom.
	readFromIdx := len(read1) / 2
	readFromMsgID := read1[readFromIdx].MessageID()
	s, err := w.Read(ctx, ReadOption{
		Name:          "scanner_deliver_start_from",
		DeliverPolicy: options.DeliverPolicyStartFrom(readFromMsgID),
	})
	assert.NoError(f.t, err)
	for i := readFromIdx; i < len(read1); i++ {
		msg, ok := <-s.Chan()
		assert.NotNil(f.t, msg)
		assert.True(f.t, ok)
		assert.True(f.t, msg.MessageID().EQ(read1[i].MessageID()))
	}
	s.Close()

	// Test different scan policy, StartAfter.
	s, err = w.Read(ctx, ReadOption{
		Name:          "scanner_deliver_start_after",
		DeliverPolicy: options.DeliverPolicyStartAfter(readFromMsgID),
	})
	assert.NoError(f.t, err)
	for i := readFromIdx + 1; i < len(read1); i++ {
		msg, ok := <-s.Chan()
		assert.NotNil(f.t, msg)
		assert.True(f.t, ok)
		assert.True(f.t, msg.MessageID().EQ(read1[i].MessageID()))
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

func (f walImplsTestFramework) assertSortedMessageList(msgs []message.ImmutableMessage) {
	for i := 1; i < len(msgs); i++ {
		assert.True(f.t, msgs[i-1].MessageID().LT(msgs[i].MessageID()))
	}
}

func (f walImplsTestFramework) assertEqualMessageList(msgs1 []message.ImmutableMessage, msgs2 []message.ImmutableMessage) {
	assert.Equal(f.t, f.messageCount, len(msgs1))
	assert.Equal(f.t, f.messageCount, len(msgs2))
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

func (f walImplsTestFramework) testAppend(ctx context.Context, w WALImpls) ([]message.ImmutableMessage, error) {
	ids := make([]message.ImmutableMessage, f.messageCount)
	swg := sizedwaitgroup.New(5)
	for i := 0; i < f.messageCount; i++ {
		swg.Add()
		go func(i int) {
			defer swg.Done()
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
			msg := message.NewBuilder().
				WithPayload(payload).
				WithProperties(properties).
				WithMessageType(typ).
				BuildMutable()
			id, err := w.Append(ctx, msg)
			assert.NoError(f.t, err)
			assert.NotNil(f.t, id)
			ids[i] = message.NewBuilder().
				WithPayload(payload).
				WithProperties(properties).
				WithMessageID(id).
				WithMessageType(typ).
				BuildImmutable()
		}(i)
	}
	swg.Wait()
	return ids, nil
}

func (f walImplsTestFramework) testRead(ctx context.Context, w WALImpls, name string) ([]message.ImmutableMessage, error) {
	s, err := w.Read(ctx, ReadOption{
		Name:          name,
		DeliverPolicy: options.DeliverPolicyAll(),
	})
	assert.NoError(f.t, err)
	assert.Equal(f.t, name, s.Name())
	defer s.Close()

	msgs := make([]message.ImmutableMessage, 0, f.messageCount)
	for i := 0; i < f.messageCount; i++ {
		msg, ok := <-s.Chan()
		assert.NotNil(f.t, msg)
		assert.True(f.t, ok)
		msgs = append(msgs, msg)
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
