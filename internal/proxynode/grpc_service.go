package proxynode

//
//func (node *NodeImpl) DescribePartition(ctx context.Context, in *milvuspb.PartitionName) (*milvuspb.PartitionDescription, error) {
//	log.Println("describe partition: ", in)
//
//	return &milvuspb.PartitionDescription{
//		Status: &commonpb.Status{
//			ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
//			Reason:    "Deprecated!",
//		},
//		Name:       in,
//		Statistics: nil,
//	}, nil
//
//}
//
//func (p *NodeImpl) DescribePartition2(ctx context.Context, in *milvuspb.PartitionName) (*milvuspb.PartitionDescription, error) {
//	log.Println("describe partition: ", in)
//	dpt := &DescribePartitionTask{
//		Condition: NewTaskCondition(ctx),
//		DescribePartitionRequest: internalpb.DescribePartitionRequest{
//			MsgType:       commonpb.MsgType_kDescribePartition,
//			ReqID:         0,
//			Timestamp:     0,
//			ProxyID:       0,
//			PartitionName: in,
//			//TODO, ReqID,Timestamp,ProxyID
//		},
//		masterClient: p.masterClient,
//		result:       nil,
//		ctx:          nil,
//	}
//
//	var cancel func()
//	dpt.ctx, cancel = context.WithTimeout(ctx, reqTimeoutInterval)
//	defer cancel()
//
//	err := func() error {
//		select {
//		case <-ctx.Done():
//			return errors.New("describe partion timeout")
//		default:
//			return p.sched.DdQueue.Enqueue(dpt)
//		}
//	}()
//
//	if err != nil {
//		return &milvuspb.PartitionDescription{
//			Status: &commonpb.Status{
//				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
//				Reason:    err.Error(),
//			},
//			Name:       in,
//			Statistics: nil,
//		}, nil
//	}
//
//	err = dpt.WaitToFinish()
//	if err != nil {
//		return &milvuspb.PartitionDescription{
//			Status: &commonpb.Status{
//				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
//				Reason:    err.Error(),
//			},
//			Name:       in,
//			Statistics: nil,
//		}, nil
//	}
//	return dpt.result, nil
//}
//
//func (node *NodeImpl) DescribeIndexProgress(ctx context.Context, req *milvuspb.DescribeIndexProgressRequest) (*milvuspb.BoolResponse, error) {
//	log.Println("Describe index progress for: ", req.FieldName)
//	dipt := &GetIndexStateTask{
//		Condition: NewTaskCondition(ctx),
//		IndexStateRequest: milvuspb.IndexStateRequest{
//			Base: &commonpb.MsgBase{
//				MsgType:  commonpb.MsgType_kGetIndexState,
//				SourceID: Params.ProxyID(),
//			},
//			CollectionName: req.CollectionName,
//			FieldName:      req.FieldName,
//		},
//		masterClient: node.masterClient,
//	}
//
//	var cancel func()
//	dipt.ctx, cancel = context.WithTimeout(ctx, reqTimeoutInterval)
//	defer cancel()
//
//	fn := func() error {
//		select {
//		case <-ctx.Done():
//			return errors.New("create index timeout")
//		default:
//			return node.sched.DdQueue.Enqueue(dipt)
//		}
//	}
//	err := fn()
//	if err != nil {
//		return &milvuspb.BoolResponse{
//			Status: &commonpb.Status{
//				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
//				Reason:    err.Error(),
//			},
//			Value: false,
//		}, nil
//	}
//
//	err = dipt.WaitToFinish()
//	if err != nil {
//		return &milvuspb.BoolResponse{
//			Status: &commonpb.Status{
//				ErrorCode: commonpb.ErrorCode_UNEXPECTED_ERROR,
//				Reason:    err.Error(),
//			},
//			Value: false,
//		}, nil
//	}
//
//	return dipt.result, nil
//}
