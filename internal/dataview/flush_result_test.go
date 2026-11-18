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

func TestStagedFlushResultProtocol(t *testing.T) {
	through := &viewpb.DataVersion{StreamingVersion: 2, CompactVersion: 3}
	pending := PendingFlushResultStatus(through)
	_, err := ParseFlushResult(pending)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable, "legacy callers cannot mistake a durability receipt for a handoff")
	version, waiting, err := ParseStagedFlushResult(pending)
	require.NoError(t, err)
	require.True(t, waiting)
	require.True(t, proto.Equal(through, version))
	pending.ExtraInfo[flushRetired] = "true"
	_, _, err = ParseStagedFlushResult(pending)
	require.Error(t, err)
	_, _, err = ParseStagedFlushResult(PendingFlushResultStatus(nil))
	require.Error(t, err, "a pending receipt must prove an immutable DataView boundary")
}
