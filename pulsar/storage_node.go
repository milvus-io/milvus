package pulsar

import (
	"suvlim/pulsar/schema"
	"sync"
)

func BeforeSend() schema.Message {
	segs := make([]string, 2, 2)
	segs[0] = "seg1"
	segs[1] = "seg2"
	var msg schema.Message = &schema.Key2SegMsg{EntityId: 1, Segments: segs, MsgType: schema.OpType(4)}
	return msg
}

func insert([]*schema.InsertMsg) schema.Status{
	return schema.Status{schema.ErrorCode_SUCCESS, ""}
}

var wg sync.WaitGroup
func delete([]*schema.DeleteMsg) schema.Status{
	msg := BeforeSend()
	go Send(msg)
	wg.Wait()
	return schema.Status{schema.ErrorCode_SUCCESS, ""}
}




