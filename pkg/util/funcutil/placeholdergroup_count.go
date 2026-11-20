package funcutil

import (
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// Field numbers of commonpb.PlaceholderGroup and commonpb.PlaceholderValue.
const (
	placeholderGroupPlaceholdersField protowire.Number = 1
	placeholderValueTagField          protowire.Number = 1
	placeholderValueValuesField       protowire.Number = 3
)

// CountPlaceholderGroupValues returns the number of query rows carried by a
// serialized commonpb.PlaceholderGroup, summed over its placeholders. It is
// the same number as unmarshalling the group and adding up len(Values), but it
// walks the wire format and never copies a row, so it costs a few nanoseconds
// per row and does not allocate.
//
// It rejects every input proto.Unmarshal rejects, so it can stand in for the
// unmarshal as the first check on a group that came from a client.
func CountPlaceholderGroupValues(blob []byte) (int64, error) {
	var total int64
	for len(blob) > 0 {
		num, typ, n := protowire.ConsumeTag(blob)
		if n < 0 || num > protowire.MaxValidNumber {
			return 0, errMalformedPlaceholderGroup()
		}
		blob = blob[n:]
		if num != placeholderGroupPlaceholdersField || typ != protowire.BytesType {
			if n = protowire.ConsumeFieldValue(num, typ, blob); n < 0 {
				return 0, errMalformedPlaceholderGroup()
			}
			blob = blob[n:]
			continue
		}
		placeholder, n := protowire.ConsumeBytes(blob)
		if n < 0 {
			return 0, errMalformedPlaceholderGroup()
		}
		blob = blob[n:]
		count, err := countPlaceholderValues(placeholder)
		if err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}

func countPlaceholderValues(placeholder []byte) (int64, error) {
	var count int64
	for len(placeholder) > 0 {
		num, typ, n := protowire.ConsumeTag(placeholder)
		if n < 0 || num > protowire.MaxValidNumber {
			return 0, errMalformedPlaceholderGroup()
		}
		placeholder = placeholder[n:]
		if typ != protowire.BytesType || (num != placeholderValueTagField && num != placeholderValueValuesField) {
			if n = protowire.ConsumeFieldValue(num, typ, placeholder); n < 0 {
				return 0, errMalformedPlaceholderGroup()
			}
			placeholder = placeholder[n:]
			continue
		}
		value, n := protowire.ConsumeBytes(placeholder)
		if n < 0 {
			return 0, errMalformedPlaceholderGroup()
		}
		placeholder = placeholder[n:]
		if num == placeholderValueValuesField {
			count++
		} else if !utf8.Valid(value) {
			// tag is a proto3 string, and proto.Unmarshal refuses one that is
			// not valid UTF-8. This check only keeps the two in agreement; it
			// does not turn away a search that used to run. The Proxy has
			// always unmarshalled the group of every search
			// (ConvertPlaceholderGroup), so such a request was already
			// refused there, and the search task falls back to
			// proto.Unmarshal whenever this function reports an error.
			return 0, errMalformedPlaceholderGroup()
		}
	}
	return count, nil
}

func errMalformedPlaceholderGroup() error {
	return merr.WrapErrParameterInvalidMsg("malformed placeholder group")
}
