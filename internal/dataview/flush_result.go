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
	flushPending          = "data_view_flush_pending"
)

// FlushGroupProtocol is advertised by writers that distinguish durable
// registration from the later, versioned growing-to-sealed handoff.
const FlushGroupProtocol = "flush_group_protocol"

// FlushGroupRetire distinguishes an owning WAL Drop from an ordinary empty
// Flush. Dropped alone must not retire an existing nonempty private group.
const FlushGroupRetire = "flush_group_retire"

const FlushHandoffAck = "flush_group_handoff_ack"

func FlushHandoffToken(version *viewpb.DataVersion) string {
	return strconv.FormatInt(version.GetStreamingVersion(), 10) + "/" + strconv.FormatInt(version.GetCompactVersion(), 10)
}

func PendingFlushResultStatus(through *viewpb.DataVersion) *commonpb.Status {
	status := FlushResultStatus(through)
	status.ExtraInfo[flushPending] = "true"
	return status
}

func ParseStagedFlushResult(status *commonpb.Status) (version *viewpb.DataVersion, pending bool, err error) {
	if status.GetExtraInfo()[flushPending] == "true" {
		if status.GetExtraInfo()[flushRetired] == "true" {
			return nil, false, merr.WrapErrServiceInternalMsg("conflicting pending L1 flush result")
		}
		copy := merr.Success()
		copy.ExtraInfo = map[string]string{flushStreamingVersion: status.GetExtraInfo()[flushStreamingVersion], flushCompactVersion: status.GetExtraInfo()[flushCompactVersion]}
		version, err = ParseFlushResult(copy)
		return version, true, err
	}
	version, err = ParseFlushResult(status)
	return version, false, err
}

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
	if extra[flushPending] == "true" {
		return nil, merr.WrapErrServiceUnavailableMsg("L1 flush is durable but query publication is pending")
	}
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
