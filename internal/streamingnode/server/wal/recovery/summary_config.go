package recovery

import (
	"math"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// parseSummaryRetentionBytes preserves GetAsSize's binary units, but never turns
// an invalid configuration into an explicit zero retention floor. This is a
// server configuration failure, not an error in the request opening the WAL.
func parseSummaryRetentionBytes(raw string) (uint64, error) {
	value := strings.ToLower(raw)
	unit := strings.TrimLeft(value, "0123456789")
	multiplier := uint64(1)
	switch unit {
	case "":
	case "k", "kb":
		multiplier = 1 << 10
	case "m", "mb":
		multiplier = 1 << 20
	case "g", "gb":
		multiplier = 1 << 30
	default:
		return 0, merr.WrapErrServiceInternalMsg("invalid streaming.summary.maxRetentionBytes %q: expected a nonnegative integer with optional K/KB, M/MB or G/GB suffix", raw)
	}
	size, err := strconv.ParseUint(strings.TrimSuffix(value, unit), 10, 63)
	if err != nil {
		return 0, merr.Wrap(merr.Combine(err, merr.ErrServiceInternal), "invalid streaming.summary.maxRetentionBytes")
	}
	if size > math.MaxInt64/multiplier {
		return 0, merr.WrapErrServiceInternalMsg("streaming.summary.maxRetentionBytes %q exceeds the supported byte range", raw)
	}
	return size * multiplier, nil
}
