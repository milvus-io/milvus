package dataservice

import (
	"errors"
	"path"
	"strconv"

	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
)

// binlog helper functions persisting binlog paths into kv storage.
// migrated from datanode[Author XuanYang-cn]
// ddl binlog etcd meta key:
//   ${prefix}/${collectionID}/${idx}
// segment binlog etcd meta key:
//   ${prefix}/${segmentID}/${fieldID}/${idx}

// genKey gives a valid key string for lists of UniqueIDs:
//  if alloc is true, the returned keys will have a generated-unique ID at the end.
//  if alloc is false, the returned keys will only consist of provided ids.
func (s *Server) genKey(alloc bool, ids ...UniqueID) (key string, err error) {
	if alloc {
		idx, err := s.allocator.allocID()
		if err != nil {
			return "", err
		}
		ids = append(ids, idx)
	}

	idStr := make([]string, len(ids))
	for _, id := range ids {
		idStr = append(idStr, strconv.FormatInt(id, 10))
	}

	key = path.Join(idStr...)
	return key, nil
}

var (
	errNilKvClient    = errors.New("kv client not initialized")
	errNilID2Paths    = errors.New("nil ID2PathList")
	errNilSegmentInfo = errors.New("nil segment info")
)

//SaveBinLogMetaTxn saves segment-field2Path, collection-tsPath into kv store in transcation
func (s *Server) SaveBinLogMetaTxn(meta map[string]string) error {
	if s.kvClient == nil {
		return errNilKvClient
	}
	return s.kvClient.MultiSave(meta)
}

// prepareField2PathMeta parses fields2Paths ID2PathList
//		into key-value for kv store
func (s *Server) prepareField2PathMeta(segID UniqueID, field2Paths *datapb.ID2PathList) (result map[string]string, err error) {
	if field2Paths == nil {
		return nil, errNilID2Paths
	}
	result = make(map[string]string, len(field2Paths.GetPaths()))
	fieldID := field2Paths.GetID()
	var key string
	for _, p := range field2Paths.GetPaths() {
		key, err = s.genKey(true, segID, fieldID)
		if err != nil {
			return nil, err
		}
		binlogPath := proto.MarshalTextString(&datapb.SegmentFieldBinlogMeta{
			FieldID:    fieldID,
			BinlogPath: p,
		})

		result[path.Join(Params.SegmentBinlogSubPath, key)] = binlogPath
	}
	return result, err
}

// getFieldBinlogMeta querys field binlog meta from kv store
func (s *Server) getFieldBinlogMeta(segmentID UniqueID,
	fieldID UniqueID) (metas []*datapb.SegmentFieldBinlogMeta, err error) {

	prefix, err := s.genKey(false, segmentID, fieldID)
	if err != nil {
		return nil, err
	}

	_, vs, err := s.kvClient.LoadWithPrefix(path.Join(Params.SegmentBinlogSubPath, prefix))
	if err != nil {
		return nil, err
	}

	for _, blob := range vs {
		m := &datapb.SegmentFieldBinlogMeta{}
		if err = proto.UnmarshalText(blob, m); err != nil {
			return nil, err
		}

		metas = append(metas, m)
	}

	return
}

// getSegmentBinlogMeta querys segment bin log meta from kv store
func (s *Server) getSegmentBinlogMeta(segmentID UniqueID) (metas []*datapb.SegmentFieldBinlogMeta, err error) {

	prefix, err := s.genKey(false, segmentID)
	if err != nil {
		return nil, err
	}

	_, vs, err := s.kvClient.LoadWithPrefix(path.Join(Params.SegmentBinlogSubPath, prefix))
	if err != nil {
		return nil, err
	}

	for _, blob := range vs {
		m := &datapb.SegmentFieldBinlogMeta{}
		if err = proto.UnmarshalText(blob, m); err != nil {
			return nil, err
		}

		metas = append(metas, m)
	}
	return
}

// GetVChanPositions get vchannel latest postitions with provided dml channel names
func (s *Server) GetVChanPositions(vchans []vchannel, isAccurate bool) ([]*datapb.VchannelInfo, error) {
	if s.kvClient == nil {
		return nil, errNilKvClient
	}
	pairs := make([]*datapb.VchannelInfo, 0, len(vchans))

	for _, vchan := range vchans {
		segments := s.meta.GetSegmentsByChannel(vchan.DmlChannel)
		flushedSegmentIDs := make([]UniqueID, 0)
		unflushed := make([]*datapb.SegmentInfo, 0)
		var seekPosition *internalpb.MsgPosition
		var useUnflushedPosition bool
		for _, s := range segments {
			if s.State == commonpb.SegmentState_Flushing || s.State == commonpb.SegmentState_Flushed {
				flushedSegmentIDs = append(flushedSegmentIDs, s.ID)
				if seekPosition == nil || (!useUnflushedPosition && s.DmlPosition.Timestamp > seekPosition.Timestamp) {
					seekPosition = s.DmlPosition
				}
				continue
			}

			if s.DmlPosition == nil {
				continue
			}

			unflushed = append(unflushed, s)

			if seekPosition == nil || !useUnflushedPosition || s.DmlPosition.Timestamp < seekPosition.Timestamp {
				useUnflushedPosition = true
				if isAccurate {
					seekPosition = s.DmlPosition
				} else {
					seekPosition = s.StartPosition
				}
			}
		}

		pairs = append(pairs, &datapb.VchannelInfo{
			CollectionID:      vchan.CollectionID,
			ChannelName:       vchan.DmlChannel,
			SeekPosition:      seekPosition,
			UnflushedSegments: unflushed,
			FlushedSegments:   flushedSegmentIDs,
		})
	}
	return pairs, nil
}

func zeroPos(name string) internalpb.MsgPosition {
	return internalpb.MsgPosition{
		ChannelName: name,
	}
}
