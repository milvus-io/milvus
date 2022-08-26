package utils

import (
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

func FilterReleased[E interface{ GetCollectionID() int64 }](elems []E, collections []int64) []E {
	collectionSet := typeutil.NewUniqueSet(collections...)
	ret := make([]E, 0, len(elems))
	for i := range elems {
		collection := elems[i].GetCollectionID()
		if !collectionSet.Contain(collection) {
			ret = append(ret, elems[i])
		}
	}
	return ret
}

func FindMaxVersionSegments(segments []*meta.Segment) []*meta.Segment {
	versions := make(map[int64]int64)
	segMap := make(map[int64]*meta.Segment)
	for _, s := range segments {
		v, ok := versions[s.GetID()]
		if !ok || v < s.Version {
			versions[s.GetID()] = s.Version
			segMap[s.GetID()] = s
		}
	}
	ret := make([]*meta.Segment, 0)
	for _, s := range segMap {
		ret = append(ret, s)
	}
	return ret
}
