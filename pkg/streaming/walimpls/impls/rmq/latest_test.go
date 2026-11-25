package rmq

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestReadRejectsLatestBeforeOpeningBackend(t *testing.T) {
	// A zero-value backend has no usable client: rejection must precede I/O.
	w := &walImpl{}
	s, err := w.Read(context.Background(), walimpls.ReadOption{
		DeliverPolicy: &streamingpb.DeliverPolicy{Policy: &streamingpb.DeliverPolicy_Latest{}},
	})
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Nil(t, s)
}
