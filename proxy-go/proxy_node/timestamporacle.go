package proxy_node

import (
	"context"
	"fmt"
	pb "github.com/czs007/suvlim/pkg/master/grpc/message"
	etcd "go.etcd.io/etcd/clientv3"
	"log"
	"strconv"
	"sync"
	"time"
)

const (
	tsoKeyPath string = "/timestampOracle"
)

type timestamp struct {
	physical uint64 // 18-63 bits
	logical  uint64 // 8-17 bits
	id       uint64 // 0-7 bits
}

type Timestamp uint64

type timestampOracle struct {
	client        *etcd.Client // client of a reliable meta service, i.e. etcd client
	ctx           context.Context
	rootPath      string // this timestampOracle's working root path on the reliable kv service
	saveInterval  uint64
	lastSavedTime uint64
	tso           timestamp // monotonically increasing m_timestamp
	mux           sync.Mutex
}

func ToTimeStamp(t *timestamp) Timestamp {
	ts := (t.physical << 18) + (t.logical << 8) + (t.id & uint64(0xFF))
	return Timestamp(ts)
}

func ToPhysicalTime(t uint64) uint64 {
	return t >> 18
}

func (tso *timestampOracle) Restart(id int64) {
	go func() {
		tso.loadTimestamp()
		tso.tso.id = uint64(id)
		ticker := time.Tick(time.Duration(tso.saveInterval) * time.Millisecond)
		for {
			select {
			case <-ticker:
				_, s := tso.GetTimestamp(1)
				if s.ErrorCode == pb.ErrorCode_SUCCESS {
					_ = tso.saveTimestamp()
				}
				break
			case <-tso.ctx.Done():
				if err := tso.client.Close(); err != nil {
					log.Printf("close etcd client error %v", err)
				}
				return
			}
		}
	}()
}

func (tso *timestampOracle) GetTimestamp(count uint32) ([]Timestamp, pb.Status) {
	physical := uint64(time.Now().UnixNano()) / uint64(1e6)
	var ctso timestamp
	tso.mux.Lock()
	if tso.tso.physical < physical {
		tso.tso.physical = physical
	}
	ctso = tso.tso
	tso.mux.Unlock()
	tt := make([]Timestamp, 0, count)
	// (TODO:shengjh) seems tso.tso has not been updated.
	for i := uint32(0); i < count; i++ {
		ctso.logical = uint64(i)
		tt = append(tt, ToTimeStamp(&ctso))
	}
	return tt, pb.Status{ErrorCode: pb.ErrorCode_SUCCESS}
}

func (tso *timestampOracle) saveTimestamp() pb.Status {
	tso.mux.Lock()
	physical := tso.tso.physical
	tso.mux.Unlock()
	if _, err := tso.client.Put(tso.ctx, tso.rootPath+tsoKeyPath, strconv.FormatUint(physical, 10)); err != nil {
		return pb.Status{ErrorCode: pb.ErrorCode_UNEXPECTED_ERROR, Reason: fmt.Sprintf("put into etcd failed, error = %v", err)}
	}
	tso.mux.Lock()
	tso.lastSavedTime = physical
	tso.mux.Unlock()
	return pb.Status{ErrorCode: pb.ErrorCode_SUCCESS}
}

func (tso *timestampOracle) loadTimestamp() pb.Status {
	ts, err := tso.client.Get(tso.ctx, tso.rootPath+tsoKeyPath)
	if err != nil {
		return pb.Status{ErrorCode: pb.ErrorCode_UNEXPECTED_ERROR, Reason: fmt.Sprintf("get from etcd failed, error = %v", err)}
	}
	if len(ts.Kvs) != 0 {
		n, err := strconv.ParseUint(string(ts.Kvs[0].Value), 10, 64)
		if err != nil {
			return pb.Status{ErrorCode: pb.ErrorCode_UNEXPECTED_ERROR, Reason: fmt.Sprintf("ParseUint failed, error = %v", err)}
		}
		tso.mux.Lock()
		tso.tso.physical = n
		tso.lastSavedTime = n
		tso.mux.Unlock()
	} else {
		tso.mux.Lock()
		tso.tso.physical = uint64(time.Now().UnixNano()) / uint64(1e6)
		tso.lastSavedTime = tso.tso.physical
		tso.mux.Unlock()
	}
	return pb.Status{ErrorCode: pb.ErrorCode_UNEXPECTED_ERROR}
}
