package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
)

type TextReceiver struct {
	FullPath string
	sinks.SyncMetricHandler
}

func NewTextReceiver(fullPath string) (tr *TextReceiver) {
	tr = &TextReceiver{
		FullPath:          fullPath,
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
	}

	go tr.HandleSyncMetric()

	return tr
}

func (r TextReceiver) UpdateMeasurements(ctx context.Context, msg *pb.MeasurementEnvelope) (reply *pb.Reply, err error) {
	if err = sinks.IsValidMeasurement(msg); err != nil {
		return reply, err
	}

	// Write Metrics in a text file
	fileName := msg.DBName + ".txt"
	file, err := os.OpenFile(r.FullPath+"/"+fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		log.Println("Unable to open file. Error: " + err.Error())
		return reply, err
	}

	writer := bufio.NewWriter(file)
	defer func() {_ = file.Close()}()

	output := "DBName: " + msg.DBName + "\n" + "Metric: " + msg.MetricName + "\n"

	for _, data := range msg.Data {
		output += sinks.GetJson(data) + "\n"
	}

	output += "\n===================================\n"

	_, err = fmt.Fprintln(writer, output)
	if err != nil {
		return reply, err
	}
	return reply, writer.Flush()
}