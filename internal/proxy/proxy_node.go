package proxy

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/zilliztech/milvus-distributed/internal/conf"

	"github.com/zilliztech/milvus-distributed/internal/proto/internalpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
	etcd "go.etcd.io/etcd/clientv3"
	"strconv"
)

type BaseRequest interface {
	Type() internalpb.ReqType
	PreExecute() commonpb.Status
	Execute() commonpb.Status
	PostExecute() commonpb.Status
	WaitToFinish() commonpb.Status
}

type ProxyOptions struct {
	//proxy server
	address                string //grpc server address
	master_address         string //master server addess
	collectionMetaRootPath string // etcd root path,read metas of collections and segments from etcd
	pulsarAddr             string // pulsar address for reader
	readerTopicsPrefix     string
	numReadTopics          int
	deleteTopic            string
	queryTopic             string
	resultTopic            string
	resultGroup            string
	numReaderNode          int
	proxyId                int64 //start from 1
	etcdEndpoints          []string

	//timestamporacle
	tsoRootPath     string //etcd root path, store timestamp into this key
	tsoSaveInterval uint64

	//timetick
	timeTickInterval uint64
	timeTickTopic    string
	timeTickPeerId   int64 //start from 1

	// inner member
	proxyServer *proxyServer
	tso         *timestampOracle
	timeTick    *timeTick
	ctx         context.Context
	cancel      context.CancelFunc
}

func ReadProxyOptionsFromConfig() (*ProxyOptions, error) {

	etcdRootPath := conf.Config.Etcd.Rootpath
	if etcdRootPath[len(etcdRootPath)-1] == '/' {
		etcdRootPath = etcdRootPath[0 : len(etcdRootPath)-1]
	}

	return &ProxyOptions{
		address:                conf.Config.Proxy.Network.Address + ":" + strconv.Itoa(conf.Config.Proxy.Network.Port),
		master_address:         conf.Config.Master.Address + ":" + strconv.Itoa(int(conf.Config.Master.Port)),
		collectionMetaRootPath: etcdRootPath,
		pulsarAddr:             "pulsar://" + conf.Config.Pulsar.Address + ":" + strconv.Itoa(int(conf.Config.Pulsar.Port)),
		readerTopicsPrefix:     conf.Config.Proxy.PulsarTopics.ReaderTopicPrefix,
		numReadTopics:          conf.Config.Proxy.PulsarTopics.NumReaderTopics,
		deleteTopic:            conf.Config.Proxy.PulsarTopics.DeleteTopic,
		queryTopic:             conf.Config.Proxy.PulsarTopics.QueryTopic,
		resultTopic:            conf.Config.Proxy.PulsarTopics.ResultTopic,
		resultGroup:            conf.Config.Proxy.PulsarTopics.ResultGroup,
		numReaderNode:          conf.Config.Proxy.NumReaderNodes,
		proxyId:                int64(conf.Config.Proxy.ProxyId),
		etcdEndpoints:          []string{conf.Config.Etcd.Address + ":" + strconv.Itoa(int(conf.Config.Etcd.Port))},
		tsoRootPath:            etcdRootPath,
		tsoSaveInterval:        uint64(conf.Config.Proxy.TosSaveInterval),
		timeTickInterval:       uint64(conf.Config.Proxy.TimeTickInterval),
		timeTickTopic:          conf.Config.Proxy.PulsarTopics.TimeTickTopic,
		timeTickPeerId:         int64(conf.Config.Proxy.ProxyId),
	}, nil
}

func StartProxy(opt *ProxyOptions) error {
	//global context
	opt.ctx, opt.cancel = context.WithCancel(context.Background())

	///////////////////// timestamporacle //////////////////////////
	etcdTso, err := etcd.New(etcd.Config{Endpoints: opt.etcdEndpoints})
	if err != nil {
		return err
	}
	tso := &timestampOracle{
		client:       etcdTso,
		ctx:          opt.ctx,
		rootPath:     opt.tsoRootPath,
		saveInterval: opt.tsoSaveInterval,
	}
	tso.Restart(opt.proxyId)

	/////////////////// proxy server ///////////////////////////////
	//readerTopics, send insert and delete message into these topics
	readerTopics := make([]string, 0, opt.numReadTopics)
	for i := 0; i < opt.numReadTopics; i++ {
		readerTopics = append(readerTopics, opt.readerTopicsPrefix+strconv.Itoa(i))
	}
	etcdProxy, err := etcd.New(etcd.Config{Endpoints: opt.etcdEndpoints})
	if err != nil {
		return err
	}

	srv := &proxyServer{
		address:       opt.address,
		masterAddress: opt.master_address,
		rootPath:      opt.collectionMetaRootPath,
		pulsarAddr:    opt.pulsarAddr,
		readerTopics:  readerTopics,
		deleteTopic:   opt.deleteTopic,
		queryTopic:    opt.queryTopic,
		resultTopic:   opt.resultTopic,
		resultGroup:   opt.resultTopic,
		numReaderNode: opt.numReaderNode,
		proxyId:       opt.proxyId,
		getTimestamp:  tso.GetTimestamp,
		client:        etcdProxy,
		ctx:           opt.ctx,
	}

	//errChan := make(chan error, 1)
	//go func() {
	//	err := startProxyServer(srv)
	//	errChan <- err
	//}()
	err = startProxyServer(srv)
	if err != nil {
		return err
	}

	//wait unit grpc server has started
	//if err := <-errChan; err != nil {
	//	return err
	//}

	////////////////////////// time tick /////////////////////////////////
	ttClient, err := pulsar.NewClient(pulsar.ClientOptions{URL: opt.pulsarAddr})
	if err != nil {
		return err
	}
	ttProducer, err := ttClient.CreateProducer(pulsar.ProducerOptions{Topic: opt.timeTickTopic})
	if err != nil {
		return err
	}

	tt := &timeTick{
		interval:             opt.timeTickInterval,
		pulsarProducer:       ttProducer,
		peer_id:              opt.timeTickPeerId,
		ctx:                  opt.ctx,
		areRequestsDelivered: func(ts Timestamp) bool { return srv.reqSch.AreRequestsDelivered(ts, 2) },
		getTimestamp: func() (Timestamp, commonpb.Status) {
			ts, st := tso.GetTimestamp(1)
			return ts[0], st
		},
	}
	s := tt.Restart()
	if s.ErrorCode != commonpb.ErrorCode_SUCCESS {
		return fmt.Errorf(s.Reason)
	}

	opt.proxyServer = srv
	opt.tso = tso
	opt.timeTick = tt

	srv.wg.Wait()
	return nil
}
