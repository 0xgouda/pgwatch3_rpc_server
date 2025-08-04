package sinks

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// just some unlikely string for a DB name to avoid using maps of maps for DB+metric data
var	dbMetricJoinStr = "¤¤¤" 

type SyncMetricHandler struct {
	syncChannel chan *pb.SyncReq
	reqsCnt     map[string]int
	reqsCntLock sync.Mutex
	pb.UnimplementedReceiverServer
}

func NewSyncMetricHandler(chanSize int) SyncMetricHandler {
	if chanSize == 0 {
		chanSize = 1024
	}
	return SyncMetricHandler{
		syncChannel: make(chan *pb.SyncReq, chanSize),
		reqsCnt: make(map[string]int),
	}
}

func (handler *SyncMetricHandler) SyncMetric(ctx context.Context, req *pb.SyncReq) (*pb.Reply, error) {
	operation := req.GetOperation()
	if operation != pb.SyncOp_AddOp && operation != pb.SyncOp_DeleteOp {
		return nil, status.Errorf(codes.InvalidArgument, "invalid operation type")
	}
	// any SyncReq must specify DBName to add/remove it or metric from it
	if req.GetDBName() == "" {
		return nil, status.Errorf(codes.InvalidArgument, "invalid sync request DBName can't be empty")
	}

	// handle concurrent add/delete operations 
	// on the same source db from different pgwatch instances
	dbMetricStr := req.GetDBName() + dbMetricJoinStr + req.GetMetricName()
	handler.reqsCntLock.Lock()
	defer handler.reqsCntLock.Unlock()

	var shouldHandle bool
	var toAdd int
	if operation == pb.SyncOp_AddOp {
		toAdd = 1
		shouldHandle = (handler.reqsCnt[dbMetricStr] == 0)
	} else {
		toAdd = -1
		shouldHandle = (handler.reqsCnt[dbMetricStr] == 1) 
	}
	handler.reqsCnt[dbMetricStr] += toAdd

	if !shouldHandle {
		return nil, nil
	}

	select {
	case handler.syncChannel <- req:
		opName := "Add"
		if operation == pb.SyncOp_DeleteOp {
			opName = "Delete"
		}
		msg := fmt.Sprintf("gRPC Receiver Synced: DBName %s MetricName %s Operation %s", req.GetDBName(), req.GetMetricName(), opName)
		return &pb.Reply{Logmsg: msg}, nil
	case <-time.After(5 * time.Second):
		handler.reqsCnt[dbMetricStr] -= toAdd
		return nil, status.Errorf(codes.DeadlineExceeded, "timeout while trying to sync metric")
	}
}

func (handler *SyncMetricHandler) GetSyncChannelContent() (*pb.SyncReq, bool) {
	content, ok := <-handler.syncChannel
	return content, ok
}

func (handler *SyncMetricHandler) HandleSyncMetric() {
	for {
		// default HandleSyncMetric = empty channel and do nothing
		handler.GetSyncChannelContent()
	}
}