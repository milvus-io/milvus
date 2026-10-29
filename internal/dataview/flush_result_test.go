package dataview

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestFlushResultRoundTrip(t *testing.T) {
	for _, version := range []*viewpb.DataVersion{nil, {StreamingVersion: 2, CompactVersion: 3}, {StreamingVersion: 1}} {
		status := FlushResultStatus(version)
		require.True(t, merr.Ok(status))
		actual, err := ParseFlushResult(status)
		require.NoError(t, err)
		require.True(t, proto.Equal(version, actual))
	}
}

func TestFlushResultRejectsUnspecifiedOrInvalidSuccess(t *testing.T) {
	for _, extra := range []map[string]string{
		nil,
		{flushStreamingVersion: "2"},
		{flushStreamingVersion: "0", flushCompactVersion: "0"},
		{flushStreamingVersion: "2", flushCompactVersion: "-1"},
		{flushStreamingVersion: "9223372036854775808", flushCompactVersion: "0"},
		{flushStreamingVersion: "2", flushCompactVersion: "bad"},
		{flushRetired: "true", flushStreamingVersion: "2"},
		{flushRetired: "true", flushCompactVersion: "0"},
	} {
		version, err := ParseFlushResult(&commonpb.Status{ExtraInfo: extra})
		require.Error(t, err)
		require.Equal(t, merr.SystemError, merr.GetErrorType(err))
		require.Nil(t, version)
	}
}
