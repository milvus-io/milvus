package switchers

import (
	"context"
	"encoding/json"
	"io"
	"path"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/generic"
	"github.com/milvus-io/milvus/pkg/util/lifetime"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// ServiceResolver is the interface to resolve component(specificly coordinator) addr and serverID.
type ServiceResolver interface {
	Resolve(ctx context.Context, service string) (addr string, id int64, err error)
}

type resolveFunc func(ctx context.Context, service string) (addr string, id int64, err error)

func (f resolveFunc) Resolve(ctx context.Context, service string) (addr string, id int64, err error) {
	return f(ctx, service)
}

type etcdResolver struct {
	cli      clientv3.KV
	metaRoot string
}

func (r *etcdResolver) Resolve(ctx context.Context, service string) (addr string, id int64, err error) {
	resp, err := r.cli.Get(ctx, path.Join(r.metaRoot, service))
	log := log.Ctx(ctx).With(zap.String("service", service))
	if err != nil {
		log.Warn("failed to resolve service address & id", zap.Error(err))
		return "", -1, err
	}
	if len(resp.Kvs) < 1 {
		log.Warn("service not found")
		return "", -1, err
	}
	kv := resp.Kvs[0]
	session := &sessionutil.Session{}
	err = json.Unmarshal(kv.Value, session)
	if err != nil {
		return "", -1, err
	}
	return session.Address, session.ServerID, nil
}

type serviceFactory[CoordClient io.Closer] func(ctx context.Context, id int64, addr string) (CoordClient, error)

// CoordWrapper is client wrapper for coordinators.
// for now, same client may serve during rolling upgrade
// it could be a grpc client for remote coordinator
// or the coordinator in the same process(before/after upgrade).
type CoordWrapper[CoordClient io.Closer] struct {
	current atomic.Pointer[CoordClient]
	// id      atomic.Int64
	idx atomic.Int32

	service   string
	resolver  ServiceResolver
	factories []serviceFactory[CoordClient]
	wg        sync.WaitGroup
	closeCh   lifetime.SafeChan
}

func NewCoordWrapper[CoordClient io.Closer](ctx context.Context, service string, resolver ServiceResolver,
	factories []serviceFactory[CoordClient],
) (*CoordWrapper[CoordClient], error) {
	w := &CoordWrapper[CoordClient]{
		service:   service,
		resolver:  resolver,
		factories: factories,
		closeCh:   lifetime.NewSafeChan(),
	}
	w.idx.Store(-1) // not selected when init

	err := w.reload(ctx)
	if err != nil {
		return nil, err
	}

	w.wg.Add(1)
	go w.backgroundCheck()

	return w, nil
}

func (cw *CoordWrapper[CoordClient]) backgroundCheck() {
	defer cw.wg.Done()

	ticker := time.NewTicker(paramtable.Get().CommonCfg.SessionTTL.GetAsDuration(time.Second))
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			cw.reload(context.Background())
		case <-cw.closeCh.CloseCh():
			log.Info("CoordWrapper closed", zap.String("service", cw.service))
			return
		}
	}
}

func (cw *CoordWrapper[CoordClient]) reload(ctx context.Context) error {
	// get service address and id from registry
	// ignore error here, since grpc factory may handle service not only state
	addr, id, _ := cw.resolver.Resolve(ctx, cw.service)
	currentIdx := cw.idx.Load()

	for idx, factory := range cw.factories {
		client, err := factory(ctx, id, addr)
		if err == nil {
			// change client when source is different
			if int32(idx) != currentIdx {
				cw.current.Store(&client)
				cw.idx.Store(int32(idx))
			}
			return nil
		}
	}
	return merr.WrapErrServiceUnavailable("all factory fail to create client")
}

func (cw *CoordWrapper[CoordClient]) GetCurrent() CoordClient {
	p := cw.current.Load()
	if p == nil {
		return generic.Zero[CoordClient]()
	}
	return *p
}

func (cw *CoordWrapper[CoordClient]) Close() error {
	cw.closeCh.Close()
	cw.wg.Wait()
	return cw.GetCurrent().Close()
}
