package dataview

import (
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const (
	flushStreamingVersion = "data_view_streaming_version"
	flushCompactVersion   = "data_view_compact_version"
	flushRetired          = "data_view_flush_retired"
)

// FlushResultStatus returns the original publication version, or an explicit
// retired result for an empty/dropped segment that needs no DataView publication.
func FlushResultStatus(version *viewpb.DataVersion) *commonpb.Status {
	status := merr.Success()
	if version == nil {
		status.ExtraInfo = map[string]string{flushRetired: "true"}
	} else {
		status.ExtraInfo = map[string]string{
			flushStreamingVersion: strconv.FormatInt(version.GetStreamingVersion(), 10),
			flushCompactVersion:   strconv.FormatInt(version.GetCompactVersion(), 10),
		}
	}
	return status
}

// ParseFlushResult requires a successful RPC status. A nil version means the
// coordinator explicitly retired the segment, never an unspecified success.
func ParseFlushResult(status *commonpb.Status) (*viewpb.DataVersion, error) {
	extra := status.GetExtraInfo()
	if extra[flushRetired] == "true" {
		_, hasStreaming := extra[flushStreamingVersion]
		_, hasCompact := extra[flushCompactVersion]
		if hasStreaming || hasCompact {
			return nil, merr.WrapErrServiceInternalMsg("conflicting L1 flush result")
		}
		return nil, nil
	}
	streaming, streamingErr := strconv.ParseInt(extra[flushStreamingVersion], 10, 64)
	compact, compactErr := strconv.ParseInt(extra[flushCompactVersion], 10, 64)
	if streamingErr != nil || compactErr != nil || streaming <= 0 || compact < 0 {
		return nil, merr.WrapErrServiceInternalMsg("missing or invalid L1 flush DataVersion")
	}
	return &viewpb.DataVersion{StreamingVersion: streaming, CompactVersion: compact}, nil
}
