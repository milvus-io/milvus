package components

import (
	"context"
	"fmt"
	"log"
	"time"

	dsc "github.com/zilliztech/milvus-distributed/internal/distributed/dataservice"
	isc "github.com/zilliztech/milvus-distributed/internal/distributed/indexservice/client"
	msc "github.com/zilliztech/milvus-distributed/internal/distributed/masterservice"
	qns "github.com/zilliztech/milvus-distributed/internal/distributed/querynode"
	qsc "github.com/zilliztech/milvus-distributed/internal/distributed/queryservice/client"

	ds "github.com/zilliztech/milvus-distributed/internal/dataservice"
	is "github.com/zilliztech/milvus-distributed/internal/indexservice"
	ms "github.com/zilliztech/milvus-distributed/internal/masterservice"
	qs "github.com/zilliztech/milvus-distributed/internal/queryservice"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb2"
)

type QueryNode struct {
	ctx context.Context
	svr *qns.Server

	dataService   *dsc.Client
	masterService *msc.GrpcClient
	indexService  *isc.Client
	queryService  *qsc.Client
}

func NewQueryNode(ctx context.Context) (*QueryNode, error) {
	const retry = 10
	const interval = 500

	svr, err := qns.NewServer(ctx)
	if err != nil {
		panic(err)
	}

	// --- QueryService ---
	qs.Params.Init()
	log.Println("QueryService address:", qs.Params.Address)
	log.Println("Init Query service client ...")
	queryService, err := qsc.NewClient(qs.Params.Address, 20*time.Second)
	if err != nil {
		panic(err)
	}

	if err = queryService.Init(); err != nil {
		panic(err)
	}

	if err = queryService.Start(); err != nil {
		panic(err)
	}

	var cnt int
	for cnt = 0; cnt < retry; cnt++ {
		if cnt != 0 {
			log.Println("Query service isn't ready ...")
			log.Printf("Retrying getting query service's states in ... %v ms", interval)
		}

		qsStates, err := queryService.GetComponentStates()

		if err != nil {
			continue
		}
		if qsStates.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
			continue
		}
		if qsStates.State.StateCode != internalpb2.StateCode_INITIALIZING && qsStates.State.StateCode != internalpb2.StateCode_HEALTHY {
			continue
		}
		break
	}
	if cnt >= retry {
		panic("Query service isn't ready")
	}
	if err := svr.SetQueryService(queryService); err != nil {
		panic(err)
	}

	// --- Master Service Client ---
	ms.Params.Init()
	addr := fmt.Sprintf("%s:%d", ms.Params.Address, ms.Params.Port)
	log.Println("Master service address:", addr)
	log.Println("Init master service client ...")
	masterService, err := msc.NewGrpcClient(addr, 20*time.Second)
	if err != nil {
		panic(err)
	}

	if err = masterService.Init(); err != nil {
		panic(err)
	}

	if err = masterService.Start(); err != nil {
		panic(err)
	}

	ticker := time.NewTicker(interval * time.Millisecond)
	tctx, tcancel := context.WithTimeout(ctx, 10*interval*time.Millisecond)
	defer func() {
		ticker.Stop()
		tcancel()
	}()

	for {
		var states *internalpb2.ComponentStates
		select {
		case <-ticker.C:
			states, err = masterService.GetComponentStates()
			if err != nil {
				continue
			}
		case <-tctx.Done():
			return nil, errors.New("master client connect timeout")
		}
		if states.State.StateCode == internalpb2.StateCode_HEALTHY {
			break
		}
	}

	if err := svr.SetMasterService(masterService); err != nil {
		panic(err)
	}

	// --- IndexService ---
	is.Params.Init()
	log.Println("Index service address:", is.Params.Address)
	indexService := isc.NewClient(is.Params.Address)

	if err := indexService.Init(); err != nil {
		panic(err)
	}

	if err := indexService.Start(); err != nil {
		panic(err)
	}

	ticker = time.NewTicker(interval * time.Millisecond)
	tctx, tcancel = context.WithTimeout(ctx, 10*interval*time.Millisecond)
	defer func() {
		ticker.Stop()
		tcancel()
	}()

	for {
		var states *internalpb2.ComponentStates
		select {
		case <-ticker.C:
			states, err = indexService.GetComponentStates()
			if err != nil {
				continue
			}
		case <-tctx.Done():
			return nil, errors.New("Index service client connect timeout")
		}
		if states.State.StateCode == internalpb2.StateCode_HEALTHY {
			break
		}
	}

	if err := svr.SetIndexService(indexService); err != nil {
		panic(err)
	}

	// --- DataService ---
	ds.Params.Init()
	log.Printf("Data service address: %s:%d", ds.Params.Address, ds.Params.Port)
	log.Println("Init data service client ...")
	dataService := dsc.NewClient(fmt.Sprintf("%s:%d", ds.Params.Address, ds.Params.Port))
	if err = dataService.Init(); err != nil {
		panic(err)
	}
	if err = dataService.Start(); err != nil {
		panic(err)
	}

	for cnt = 0; cnt < retry; cnt++ {
		dsStates, err := dataService.GetComponentStates()
		if err != nil {
			log.Printf("retry cout = %d, error = %s", cnt, err.Error())
			continue
		}
		if dsStates.Status.ErrorCode != commonpb.ErrorCode_SUCCESS {
			log.Printf("retry cout = %d, error = %s", cnt, err.Error())
			continue
		}
		if dsStates.State.StateCode != internalpb2.StateCode_INITIALIZING && dsStates.State.StateCode != internalpb2.StateCode_HEALTHY {
			continue
		}
		break
	}
	if cnt >= retry {
		panic("Data service isn't ready")
	}

	if err := svr.SetDataService(dataService); err != nil {
		panic(err)
	}

	return &QueryNode{

		ctx: ctx,
		svr: svr,

		dataService:   dataService,
		masterService: masterService,
		indexService:  indexService,
		queryService:  queryService,
	}, nil

}

func (q *QueryNode) Run() error {
	if err := q.svr.Init(); err != nil {
		panic(err)
	}

	if err := q.svr.Start(); err != nil {
		panic(err)
	}
	log.Println("Query node successfully started ...")
	return nil
}

func (q *QueryNode) Stop() error {
	_ = q.dataService.Stop()
	_ = q.masterService.Stop()
	_ = q.queryService.Stop()
	_ = q.indexService.Stop()
	return q.svr.Stop()
}
