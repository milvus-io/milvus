package walimpls

import (
	"context"
	"io"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type frameworkTestWAL struct{ WALImpls }

func (*frameworkTestWAL) Append(context.Context, message.MutableMessage) (message.MessageID, error) {
	panic("patch with mockey")
}

func (*frameworkTestWAL) Read(context.Context, ReadOption) (ScannerImpls, error) {
	panic("patch with mockey")
}

type frameworkTestScanner struct{ ScannerImpls }

func (*frameworkTestScanner) Chan() <-chan message.ImmutableMessage { panic("patch with mockey") }
func (*frameworkTestScanner) Name() string                          { return "test" }
func (*frameworkTestScanner) Close() error                          { return nil }

func TestFrameworkAppendReturnsWriterError(t *testing.T) {
	failure := merr.WrapErrServiceUnavailable("test writer rejected append")
	patch := mockey.Mock((*frameworkTestWAL).Append).Return(nil, failure).Build()
	t.Cleanup(func() { patch.UnPatch() })
	f := &testOneWALImplsFramework{t: t, messageCount: 10}
	messages, err := f.testAppend(t.Context(), &frameworkTestWAL{})
	require.ErrorIs(t, err, failure)
	require.Nil(t, messages)
}

func TestFrameworkReadStopsWithoutExpectedMessages(t *testing.T) {
	for _, closed := range []bool{false, true} {
		name := "canceled"
		if closed {
			name = "closed"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			entries := make(chan message.ImmutableMessage)
			if closed {
				close(entries)
			}
			read := mockey.Mock((*frameworkTestWAL).Read).To(func(context.Context, ReadOption) (ScannerImpls, error) {
				if !closed {
					cancel()
				}
				return &frameworkTestScanner{}, nil
			}).Build()
			t.Cleanup(func() { read.UnPatch() })
			ch := mockey.Mock((*frameworkTestScanner).Chan).Return((<-chan message.ImmutableMessage)(entries)).Build()
			t.Cleanup(func() { ch.UnPatch() })
			f := &testOneWALImplsFramework{t: t, messageCount: 10}
			_, err := f.testRead(ctx, &frameworkTestWAL{}, "test")
			if closed {
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			} else {
				require.ErrorIs(t, err, context.Canceled)
			}
		})
	}
}
