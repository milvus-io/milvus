package allocator

import (
	"context"
	"log"
	"time"

	"github.com/zilliztech/milvus-distributed/internal/util/retry"
	"google.golang.org/grpc"

	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/masterpb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

const (
	IDCountPerRPC = 200000
)

type UniqueID = typeutil.UniqueID

type IDAllocator struct {
	Allocator

	masterAddress string
	masterConn    *grpc.ClientConn
	masterClient  masterpb.MasterServiceClient

	countPerRPC uint32

	idStart UniqueID
	idEnd   UniqueID

	PeerID UniqueID
}

func NewIDAllocator(ctx context.Context, masterAddr string) (*IDAllocator, error) {

	ctx1, cancel := context.WithCancel(ctx)
	a := &IDAllocator{
		Allocator: Allocator{
			Ctx:        ctx1,
			CancelFunc: cancel,
		},
		countPerRPC:   IDCountPerRPC,
		masterAddress: masterAddr,
	}
	a.TChan = &EmptyTicker{}
	a.Allocator.SyncFunc = a.syncID
	a.Allocator.ProcessFunc = a.processFunc
	a.Allocator.CheckSyncFunc = a.checkSyncFunc
	a.Allocator.PickCanDoFunc = a.pickCanDoFunc
	a.Init()
	return a, nil
}

func (ia *IDAllocator) Start() error {
	connectMasterFn := func() error {
		return ia.connectMaster()
	}
	err := retry.Retry(10, time.Millisecond*200, connectMasterFn)
	if err != nil {
		panic("connect to master failed")
	}
	ia.Allocator.Start()
	return nil
}

func (ia *IDAllocator) connectMaster() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, ia.masterAddress, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Printf("Connect to master failed, error= %v", err)
		return err
	}
	log.Printf("Connected to master, master_addr=%s", ia.masterAddress)
	ia.masterConn = conn
	ia.masterClient = masterpb.NewMasterServiceClient(conn)
	return nil
}

func (ia *IDAllocator) syncID() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	req := &masterpb.IDRequest{
		Base: &commonpb.MsgBase{
			MsgType:   commonpb.MsgType_kRequestID,
			MsgID:     0,
			Timestamp: 0,
			SourceID:  ia.PeerID,
		},
		Count: ia.countPerRPC,
	}
	resp, err := ia.masterClient.AllocID(ctx, req)

	cancel()
	if err != nil {
		log.Println("syncID Failed!!!!!")
		return false
	}
	ia.idStart = resp.GetID()
	ia.idEnd = ia.idStart + int64(resp.GetCount())
	return true
}

func (ia *IDAllocator) checkSyncFunc(timeout bool) bool {
	return timeout || len(ia.ToDoReqs) > 0
}

func (ia *IDAllocator) pickCanDoFunc() {
	total := uint32(ia.idEnd - ia.idStart)
	need := uint32(0)
	idx := 0
	for _, req := range ia.ToDoReqs {
		iReq := req.(*IDRequest)
		need += iReq.count
		if need <= total {
			ia.CanDoReqs = append(ia.CanDoReqs, req)
			idx++
		} else {
			break
		}
	}
	ia.ToDoReqs = ia.ToDoReqs[idx:]
}

func (ia *IDAllocator) processFunc(req Request) error {
	idRequest := req.(*IDRequest)
	idRequest.id = ia.idStart
	ia.idStart++
	return nil
}

func (ia *IDAllocator) AllocOne() (UniqueID, error) {
	ret, _, err := ia.Alloc(1)
	if err != nil {
		return 0, err
	}
	return ret, nil
}

func (ia *IDAllocator) Alloc(count uint32) (UniqueID, UniqueID, error) {
	req := &IDRequest{BaseRequest: BaseRequest{Done: make(chan error), Valid: false}}

	req.count = count
	ia.Reqs <- req
	req.Wait()

	if !req.IsValid() {
		return 0, 0, nil
	}
	start, count := req.id, req.count
	return start, start + int64(count), nil
}
