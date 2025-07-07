package sinks

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
)

type SyncMetricHandler struct {
	syncChannel chan *pb.SyncReq
	pb.UnimplementedReceiverServer
}

func NewSyncMetricHandler(chanSize int) SyncMetricHandler {
	if chanSize == 0 {
		chanSize = 1024
	}
	return SyncMetricHandler{syncChannel: make(chan *pb.SyncReq, chanSize)}
}

func (handler *SyncMetricHandler) SyncMetric(ctx context.Context, req *pb.SyncReq) (*pb.Reply, error) {
	reply := &pb.Reply{}
	if req.GetOperation() != pb.SyncOp_AddOp && req.GetOperation() != pb.SyncOp_DeleteOp {
		return reply, errors.New("invalid operation type")
	}
	if req.GetDBName() == "" && req.GetMetricName() == "" {
		return reply, errors.New("invalid sync request both DBName and MetricName are empty")
	}

	opName := "Add"
	if req.GetOperation() == pb.SyncOp_DeleteOp {
		opName = "Delete"
	}

	select {
	case handler.syncChannel <- req:
		reply.Logmsg = fmt.Sprintf("Receiver Synced %s %s %s", req.GetDBName(), req.GetMetricName(), opName)
		return reply, nil
	case <-time.After(5 * time.Second):
		return reply, errors.New("timeout while trying to sync metric")
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