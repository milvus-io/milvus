package planner

import "encoding/json"

type NodeKVPairs struct {
	KVs map[string]string
}

func (n *NodeKVPairs) Insert(key, value string) {
	n.KVs[key] = value
}

func (n *NodeKVPairs) String() string {
	// How could `Marshal` return error here?
	bs, _ := json.Marshal(n.KVs)
	return string(bs)
}

func NewNodeKVPairs() *NodeKVPairs {
	return &NodeKVPairs{
		KVs: make(map[string]string),
	}
}
