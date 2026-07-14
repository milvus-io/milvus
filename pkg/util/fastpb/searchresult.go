package fastpb

import (
	"math"

	"google.golang.org/protobuf/proto"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	schemapb "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

// UnmarshalSearchResultData decodes a wire-format schemapb.SearchResultData into srd.
// Hot fields (fields_data, scores, ids, topks, distances, output_fields, ...) are
// hand-decoded; rare fields (iterator results, highlights, agg buckets) delegate
// to the official codec. Equivalence is pinned by the differential + fuzz tests.
func UnmarshalSearchResultData(b []byte, srd *schemapb.SearchResultData) error {
	proto.Reset(srd) // match official proto.Unmarshal: clear target before decode
	if err := (dec{}).searchResultData(b, srd); err != nil {
		if fallbackOnProto2(err) {
			proto.Reset(srd) // discard partial decode before the authoritative pass
			return proto.Unmarshal(b, srd)
		}
		return err
	}
	return nil
}

func (d dec) searchResultData(b []byte, srd *schemapb.SearchResultData) error {
	var rest []byte
	for len(b) > 0 {
		start := b
		num, wtype, tn := consumeTag(b)
		if tn <= 0 {
			return errMalformed
		}
		b = b[tn:]
		if isProto2Group(wtype) {
			return errProto2
		}
		if wtype == 5 && (num == 4 || num == 10 || num == 12) {
			if len(b) < 4 {
				return errMalformed
			}
			value := math.Float32frombits(le32(b))
			switch num {
			case 4:
				srd.Scores = append(srd.Scores, value)
			case 10:
				srd.Distances = append(srd.Distances, value)
			case 12:
				srd.Recalls = append(srd.Recalls, value)
			}
			b = b[4:]
			continue
		}

		// varint-typed fields
		if wtype == 0 {
			v, vn := consumeVarint(b)
			if vn <= 0 {
				return errMalformed
			}
			switch num {
			case 1:
				srd.NumQueries = int64(v)
			case 2:
				srd.TopK = int64(v)
			case 9:
				srd.AllSearchCount = int64(v)
				// packed repeated fields may also arrive as single varints:
			case 6:
				srd.Topks = append(srd.Topks, int64(v))
			case 19:
				srd.AggTopks = append(srd.AggTopks, int64(v))
			default:
				rest = append(rest, start[:tn+vn]...)
			}
			b = b[vn:]
			continue
		}

		// everything else here is length-delimited
		if wtype != 2 {
			sn := skipField(b, wtype)
			if sn <= 0 {
				return errMalformed
			}
			rest = append(rest, start[:tn+sn]...)
			b = b[sn:]
			continue
		}
		v, vn := consumeBytes(b)
		if vn <= 0 {
			return errMalformed
		}
		b = b[vn:]

		switch num {
		case 3: // fields_data (repeated FieldData)
			fd := &schemapb.FieldData{}
			if err := d.fieldData(v, fd); err != nil {
				return err
			}
			srd.FieldsData = append(srd.FieldsData, fd)
		case 4: // scores (packed fixed32)
			if err := appendPackedF32(v, &srd.Scores); err != nil {
				return err
			}
		case 5: // ids (IDs)
			ids := &schemapb.IDs{}
			if err := d.ids(v, ids); err != nil {
				return err
			}
			if srd.Ids == nil {
				srd.Ids = ids
			} else {
				proto.Merge(srd.Ids, ids) // repeated singular message on wire → proto3 merge
			}
		case 6: // topks (packed varint)
			if err := appendPackedI64(v, &srd.Topks); err != nil {
				return err
			}
		case 7: // output_fields (repeated string)
			s, err := d.str(v)
			if err != nil {
				return err
			}
			srd.OutputFields = append(srd.OutputFields, s)
		case 8: // group_by_field_value (FieldData)
			fd := &schemapb.FieldData{}
			if err := d.fieldData(v, fd); err != nil {
				return err
			}
			if srd.GroupByFieldValue == nil {
				srd.GroupByFieldValue = fd
			} else {
				proto.Merge(srd.GroupByFieldValue, fd)
			}
		case 10: // distances (packed fixed32)
			if err := appendPackedF32(v, &srd.Distances); err != nil {
				return err
			}
		case 11: // search_iterator_v2_results (delegate)
			m := &schemapb.SearchIteratorV2Results{}
			if err := protoUnmarshal(v, m); err != nil {
				return err
			}
			if srd.SearchIteratorV2Results == nil {
				srd.SearchIteratorV2Results = m
			} else {
				proto.Merge(srd.SearchIteratorV2Results, m)
			}
		case 12: // recalls (packed fixed32)
			if err := appendPackedF32(v, &srd.Recalls); err != nil {
				return err
			}
		case 13: // primary_field_name (string)
			s, err := d.str(v)
			if err != nil {
				return err
			}
			srd.PrimaryFieldName = s
		case 14: // highlight_results (repeated commonpb.HighlightResult, delegate)
			m := &commonpb.HighlightResult{}
			if err := protoUnmarshal(v, m); err != nil {
				return err
			}
			srd.HighlightResults = append(srd.HighlightResults, m)
		case 15: // element_indices (LongArray)
			la := &schemapb.LongArray{}
			if err := decodePackedI64(v, &la.Data, la); err != nil {
				return err
			}
			if srd.ElementIndices == nil {
				srd.ElementIndices = la
			} else {
				proto.Merge(srd.ElementIndices, la)
			}
		case 17: // group_by_field_values (repeated FieldData)
			fd := &schemapb.FieldData{}
			if err := d.fieldData(v, fd); err != nil {
				return err
			}
			srd.GroupByFieldValues = append(srd.GroupByFieldValues, fd)
		case 18: // agg_buckets (repeated AggBucket, delegate)
			m := &schemapb.AggBucket{}
			if err := protoUnmarshal(v, m); err != nil {
				return err
			}
			srd.AggBuckets = append(srd.AggBuckets, m)
		case 19: // agg_topks (packed varint)
			if err := appendPackedI64(v, &srd.AggTopks); err != nil {
				return err
			}
		default: // unhandled (future) field → fold into official merge
			rest = append(rest, start[:tn+vn]...)
		}
	}
	if len(rest) > 0 {
		return protoMerge(rest, srd)
	}
	return nil
}

// unmarshalIDs decodes schemapb.IDs: oneof int_id (LongArray, 1) / str_id (StringArray, 2).
func (d dec) ids(b []byte, ids *schemapb.IDs) error {
	full := b
	var rest []byte
	oneofNum := 0
	for len(b) > 0 {
		start := b
		num, wtype, tn := consumeTag(b)
		if tn <= 0 {
			return errMalformed
		}
		b = b[tn:]
		if isProto2Group(wtype) {
			return errProto2
		}
		if wtype != 2 {
			sn := skipField(b, wtype)
			if sn <= 0 {
				return errMalformed
			}
			rest = append(rest, start[:tn+sn]...)
			b = b[sn:]
			continue
		}
		v, vn := consumeBytes(b)
		if vn <= 0 {
			return errMalformed
		}
		b = b[vn:]
		if num == 1 || num == 2 {
			if oneofNum == num {
				return fallbackUnmarshal(full, ids)
			}
			oneofNum = num
		}
		switch num {
		case 1:
			la := &schemapb.LongArray{}
			if err := decodePackedI64(v, &la.Data, la); err != nil {
				return err
			}
			ids.IdField = &schemapb.IDs_IntId{IntId: la}
		case 2:
			sa := &schemapb.StringArray{}
			if err := d.stringArray(v, sa); err != nil {
				return err
			}
			ids.IdField = &schemapb.IDs_StrId{StrId: sa}
		default:
			rest = append(rest, start[:tn+vn]...)
		}
	}
	if len(rest) > 0 {
		return protoMerge(rest, ids)
	}
	return nil
}
