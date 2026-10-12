package pulsar

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

const testProducerTopic = "public/default/test-channel"

// fakeProducerClient fails the first failures producer creations; a negative failures fails every creation.
// events records the Close calls of the client and its producers in order.
type fakeProducerClient struct {
	pulsar.Client
	mu       sync.Mutex
	failures int
	options  []pulsar.ProducerOptions
	events   []string
}

func (c *fakeProducerClient) CreateProducer(options pulsar.ProducerOptions) (pulsar.Producer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.options = append(c.options, options)
	if c.failures != 0 {
		if c.failures > 0 {
			c.failures--
		}
		return nil, errors.New("server error: ProducerBusy")
	}
	return &closeRecordingProducer{client: c}, nil
}

func (c *fakeProducerClient) Close() {
	c.record("client closed")
}

func (c *fakeProducerClient) record(event string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, event)
}

// factory returns a clientFactory that always returns c.
func (c *fakeProducerClient) factory() clientFactory {
	return func() (pulsar.Client, error) {
		return c, nil
	}
}

// closeRecordingProducer records its Close call into the events of its client.
type closeRecordingProducer struct {
	pulsar.Producer
	client *fakeProducerClient
}

func (p *closeRecordingProducer) Close() {
	p.client.record("producer closed")
}

func TestNewWALProducerCreatesExclusiveProducer(t *testing.T) {
	c := &fakeProducerClient{failures: 2}

	p, err := newWALProducer(context.Background(), c.factory(), testProducerTopic, mlog.With())

	require.NoError(t, err)
	require.NotNil(t, p)
	assert.Equal(t, pulsar.Client(c), p.client)
	require.Len(t, c.options, 3)
	for _, options := range c.options {
		assert.Equal(t, testProducerTopic, options.Topic)
		assert.Equal(t, pulsar.ProducerAccessModeExclusive, options.ProducerAccessMode)
	}
	assert.Empty(t, c.events)
}

func TestNewWALProducerUsesConfiguredAccessMode(t *testing.T) {
	defer paramtable.Get().PulsarCfg.ProducerAccessMode.SwapTempValue(paramtable.Get().PulsarCfg.ProducerAccessMode.SwapTempValue("shared"))
	c := &fakeProducerClient{}

	_, err := newWALProducer(context.Background(), c.factory(), testProducerTopic, mlog.With())

	require.NoError(t, err)
	require.Len(t, c.options, 1)
	assert.Equal(t, pulsar.ProducerAccessModeShared, c.options[0].ProducerAccessMode)
}

func TestNewWALProducerClosesClientWhenContextDone(t *testing.T) {
	c := &fakeProducerClient{failures: -1}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	p, err := newWALProducer(ctx, c.factory(), testProducerTopic, mlog.With())

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Nil(t, p)
	assert.Equal(t, []string{"client closed"}, c.events)
}

func TestNewWALProducerStopsAtCreateTimeout(t *testing.T) {
	defer paramtable.Get().PulsarCfg.ProducerCreateTimeout.SwapTempValue(paramtable.Get().PulsarCfg.ProducerCreateTimeout.SwapTempValue("100ms"))
	c := &fakeProducerClient{failures: -1}

	p, err := newWALProducer(context.Background(), c.factory(), testProducerTopic, mlog.With())

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Nil(t, p)
	assert.Equal(t, []string{"client closed"}, c.events)
}

func TestNewWALProducerFailsWhenClientCreationFails(t *testing.T) {
	clientErr := errors.New("create client failed")

	p, err := newWALProducer(context.Background(), func() (pulsar.Client, error) {
		return nil, clientErr
	}, testProducerTopic, mlog.With())

	assert.ErrorIs(t, err, clientErr)
	assert.Nil(t, p)
}

func TestWALProducerCloseClosesProducerThenClient(t *testing.T) {
	c := &fakeProducerClient{}
	p, err := newWALProducer(context.Background(), c.factory(), testProducerTopic, mlog.With())
	require.NoError(t, err)

	p.Close()

	assert.Equal(t, []string{"producer closed", "client closed"}, c.events)
}

type failingPulsarProducer struct {
	pulsar.Producer
	err error
}

func (p *failingPulsarProducer) Send(context.Context, *pulsar.ProducerMessage) (pulsar.MessageID, error) {
	return nil, p.err
}

// joinedError is an error with the Unwrap() []error method, as the standard library errors.Join returns.
type joinedError struct {
	errs []error
}

func (e *joinedError) Error() string {
	return "joined error"
}

func (e *joinedError) Unwrap() []error {
	return e.errs
}

func TestWALProducerSendMarksUnusableProducerAsErrFenced(t *testing.T) {
	tests := []struct {
		name   string
		err    error
		fenced bool
	}{
		{
			name:   "fenced",
			err:    &joinedError{errs: []error{pulsar.ErrProducerFenced, errors.New("server error: ProducerFenced")}},
			fenced: true,
		},
		{
			name:   "topic terminated",
			err:    &joinedError{errs: []error{pulsar.ErrTopicTerminated, errors.New("server error: TopicTerminatedError")}},
			fenced: true,
		},
		{
			name:   "topic not found",
			err:    &joinedError{errs: []error{pulsar.ErrTopicNotfound, errors.New("server error: TopicNotFound")}},
			fenced: true,
		},
		{
			name:   "wrapped fenced",
			err:    errors.Wrap(&joinedError{errs: []error{errors.New("server error: ProducerFenced"), pulsar.ErrProducerFenced}}, "send message"),
			fenced: true,
		},
		{
			name:   "closed",
			err:    pulsar.ErrProducerClosed,
			fenced: true,
		},
		{
			name:   "not fenced",
			err:    errors.New("connection closed"),
			fenced: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := &walProducer{producer: &failingPulsarProducer{err: test.err}}

			_, err := p.Send(context.Background(), &pulsar.ProducerMessage{Payload: []byte("payload")})

			assert.ErrorIs(t, err, test.err)
			assert.Equal(t, test.fenced, errors.Is(err, walimpls.ErrFenced))
		})
	}
}
