package fastpb

import (
	"google.golang.org/protobuf/proto"

	commonpb "github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	milvuspb "github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	schemapb "github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
)

// TryUnmarshal fast-paths the hot read/write message types through the
// hand-written decoders and reports whether it handled v. Any other type
// returns (false, nil) so the caller falls back to the official proto codec.
// Only types pinned by the differential + fuzz tests are fast-pathed.
func TryUnmarshal(v any, b []byte) (bool, error) {
	switch m := v.(type) {
	case *internalpb.RetrieveResults:
		return true, UnmarshalRetrieveResults(b, m)
	case *milvuspb.InsertRequest:
		return true, UnmarshalInsertRequest(b, m)
	case *milvuspb.UpsertRequest:
		return true, UnmarshalUpsertRequest(b, m)
	default:
		// Everything else (incl. SearchResultData, which is never a top-level gRPC
		// message — it travels as the SlicedBlob bytes of internalpb.SearchResults and
		// is fast-decoded directly at the reduce sites via UnmarshalSearchResultData)
		// falls back to the official codec.
		return false, nil
	}
}

// UnmarshalRetrieveResults decodes internalpb.RetrieveResults (query read path,
// querynode→proxy; internal/trusted). Hot field: fields_data (5).
func UnmarshalRetrieveResults(b []byte, rr *internalpb.RetrieveResults) error {
	proto.Reset(rr) // match official proto.Unmarshal: clear target before decode
	if err := (dec{}).retrieveResults(b, rr); err != nil {
		if fallbackOnProto2(err) {
			proto.Reset(rr) // discard partial decode before the authoritative pass
			return proto.Unmarshal(b, rr)
		}
		return err
	}
	return nil
}

func (d dec) retrieveResults(b []byte, rr *internalpb.RetrieveResults) error {
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
		// varint scalar fields
		if wtype == 0 {
			v, vn := consumeVarint(b)
			if vn <= 0 {
				return errMalformed
			}
			b = b[vn:]
			switch num {
			case 3:
				rr.ReqID = int64(v)
			case 14:
				rr.AllRetrieveCount = int64(v)
			case 15:
				rr.HasMoreResult = v != 0
			case 16:
				rr.ScannedRemoteBytes = int64(v)
			case 17:
				rr.ScannedTotalBytes = int64(v)
			case 18:
				rr.ElementLevel = v != 0
				// fields 6/8 (packed int64) may also appear as a single varint:
			case 6:
				rr.SealedSegmentIDsRetrieved = append(rr.SealedSegmentIDsRetrieved, int64(v))
			case 8:
				rr.GlobalSealedSegmentIDs = append(rr.GlobalSealedSegmentIDs, int64(v))
			default:
				rest = append(rest, start[:tn+vn]...)
			}
			continue
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
		switch num {
		case 1: // Base (delegate)
			m := &commonpb.MsgBase{}
			if err := protoUnmarshal(v, m); err != nil {
				return err
			}
			if rr.Base == nil {
				rr.Base = m
			} else {
				// wire-level merge: a later occurrence's explicit proto3-default
				// scalar (e.g. TargetID=0) must overwrite, matching proto.Unmarshal.
				if err := protoMerge(v, rr.Base); err != nil {
					return err
				}
			}
		case 2: // Status (delegate)
			m := &commonpb.Status{}
			if err := protoUnmarshal(v, m); err != nil {
				return err
			}
			if rr.Status == nil {
				rr.Status = m
			} else {
				if err := protoMerge(v, rr.Status); err != nil { // wire-level merge (see Base)
					return err
				}
			}
		case 4: // Ids
			ids := &schemapb.IDs{}
			if err := d.ids(v, ids); err != nil {
				return err
			}
			if rr.Ids == nil {
				rr.Ids = ids
			} else {
				if err := protoMerge(v, rr.Ids); err != nil { // wire-level merge (see Base)
					return err
				}
			}
		case 5: // fields_data (HOT)
			fd := &schemapb.FieldData{}
			if err := d.fieldData(v, fd); err != nil {
				return err
			}
			rr.FieldsData = append(rr.FieldsData, fd)
		case 6: // sealed_segmentIDs_retrieved (packed int64)
			if err := appendPackedI64(v, &rr.SealedSegmentIDsRetrieved); err != nil {
				return err
			}
		case 7: // channelIDs_retrieved (repeated string)
			s, err := d.str(v)
			if err != nil {
				return err
			}
			rr.ChannelIDsRetrieved = append(rr.ChannelIDsRetrieved, s)
		case 8: // global_sealed_segmentIDs (packed int64)
			if err := appendPackedI64(v, &rr.GlobalSealedSegmentIDs); err != nil {
				return err
			}
		case 13: // CostAggregation (delegate)
			m := &internalpb.CostAggregation{}
			if err := protoUnmarshal(v, m); err != nil {
				return err
			}
			if rr.CostAggregation == nil {
				rr.CostAggregation = m
			} else {
				if err := protoMerge(v, rr.CostAggregation); err != nil { // wire-level merge (see Base)
					return err
				}
			}
		case 19: // element_indices (repeated message, delegate)
			m := &internalpb.ElementIndices{}
			if err := protoUnmarshal(v, m); err != nil {
				return err
			}
			rr.ElementIndices = append(rr.ElementIndices, m)
		default: // unhandled (future) field → fold into official merge
			rest = append(rest, start[:tn+vn]...)
		}
	}
	if len(rest) > 0 {
		return protoMerge(rest, rr)
	}
	return nil
}

// UnmarshalInsertRequest decodes milvuspb.InsertRequest (write path, client→proxy;
// UNTRUSTED ingress → strings are UTF-8 validated). Hot field: fields_data (5).
func UnmarshalInsertRequest(b []byte, ir *milvuspb.InsertRequest) error {
	proto.Reset(ir) // match official proto.Unmarshal: clear target before decode
	if err := (dec{utf8: true}).insertRequest(b, ir); err != nil {
		if fallbackOnProto2(err) {
			proto.Reset(ir) // discard partial decode before the authoritative pass
			return proto.Unmarshal(b, ir)
		}
		return err
	}
	return nil
}

func (d dec) insertRequest(b []byte, ir *milvuspb.InsertRequest) error {
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
		if wtype == 0 {
			v, vn := consumeVarint(b)
			if vn <= 0 {
				return errMalformed
			}
			b = b[vn:]
			switch num {
			case 6:
				ir.HashKeys = append(ir.HashKeys, uint32(v))
			case 7:
				ir.NumRows = uint32(v)
			case 8:
				ir.SchemaTimestamp = v
			default:
				rest = append(rest, start[:tn+vn]...)
			}
			continue
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
		switch num {
		case 1: // Base (delegate)
			m := &commonpb.MsgBase{}
			if err := protoUnmarshal(v, m); err != nil {
				return err
			}
			if ir.Base == nil {
				ir.Base = m
			} else {
				if err := protoMerge(v, ir.Base); err != nil { // wire-level merge (see rr.Base)
					return err
				}
			}
		case 2: // DbName
			s, err := d.str(v)
			if err != nil {
				return err
			}
			ir.DbName = s
		case 3: // CollectionName
			s, err := d.str(v)
			if err != nil {
				return err
			}
			ir.CollectionName = s
		case 4: // PartitionName
			s, err := d.str(v)
			if err != nil {
				return err
			}
			ir.PartitionName = s
		case 5: // fields_data (HOT)
			fd := &schemapb.FieldData{}
			if err := d.fieldData(v, fd); err != nil {
				return err
			}
			ir.FieldsData = append(ir.FieldsData, fd)
		case 6: // hash_keys (packed uint32)
			if err := appendPackedU32(v, &ir.HashKeys); err != nil {
				return err
			}
		case 9: // namespace (proto3 optional string)
			s, err := d.str(v)
			if err != nil {
				return err
			}
			ns := s
			ir.Namespace = &ns
		default: // unhandled (future) field → fold into official merge
			rest = append(rest, start[:tn+vn]...)
		}
	}
	if len(rest) > 0 {
		return protoMerge(rest, ir)
	}
	return nil
}

// UnmarshalUpsertRequest decodes milvuspb.UpsertRequest (write path, client→proxy;
// UNTRUSTED ingress → strings are UTF-8 validated). Fields 1-8 share InsertRequest's
// exact wire layout (hot field: fields_data (5)); the upsert-only fields
// partial_update (9), namespace (10) and field_ops (11) are folded into the official
// merge, so they decode via the standard codec and stay wire-equivalent.
func UnmarshalUpsertRequest(b []byte, ur *milvuspb.UpsertRequest) error {
	proto.Reset(ur) // match official proto.Unmarshal: clear target before decode
	if err := (dec{utf8: true}).upsertRequest(b, ur); err != nil {
		if fallbackOnProto2(err) {
			proto.Reset(ur) // discard partial decode before the authoritative pass
			return proto.Unmarshal(b, ur)
		}
		return err
	}
	return nil
}

func (d dec) upsertRequest(b []byte, ur *milvuspb.UpsertRequest) error {
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
		if wtype == 0 {
			v, vn := consumeVarint(b)
			if vn <= 0 {
				return errMalformed
			}
			b = b[vn:]
			switch num {
			case 6:
				ur.HashKeys = append(ur.HashKeys, uint32(v))
			case 7:
				ur.NumRows = uint32(v)
			case 8:
				ur.SchemaTimestamp = v
			default: // partial_update (9) and any future varint field → official merge
				rest = append(rest, start[:tn+vn]...)
			}
			continue
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
		switch num {
		case 1: // Base (delegate)
			m := &commonpb.MsgBase{}
			if err := protoUnmarshal(v, m); err != nil {
				return err
			}
			if ur.Base == nil {
				ur.Base = m
			} else {
				if err := protoMerge(v, ur.Base); err != nil { // wire-level merge (see rr.Base)
					return err
				}
			}
		case 2: // DbName
			s, err := d.str(v)
			if err != nil {
				return err
			}
			ur.DbName = s
		case 3: // CollectionName
			s, err := d.str(v)
			if err != nil {
				return err
			}
			ur.CollectionName = s
		case 4: // PartitionName
			s, err := d.str(v)
			if err != nil {
				return err
			}
			ur.PartitionName = s
		case 5: // fields_data (HOT)
			fd := &schemapb.FieldData{}
			if err := d.fieldData(v, fd); err != nil {
				return err
			}
			ur.FieldsData = append(ur.FieldsData, fd)
		case 6: // hash_keys (packed uint32)
			if err := appendPackedU32(v, &ur.HashKeys); err != nil {
				return err
			}
		default: // namespace (10), field_ops (11), any future field → official merge
			rest = append(rest, start[:tn+vn]...)
		}
	}
	if len(rest) > 0 {
		return protoMerge(rest, ur)
	}
	return nil
}
