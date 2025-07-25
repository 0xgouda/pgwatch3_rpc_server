package main

import (
	"context"
	"encoding/csv"
	"log"
	"os"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"
)

type CSVReceiver struct {
	FullPath string
	sinks.SyncMetricHandler
}

/*
* Structure for CSV storage:
*   - Database Name
*       - Metric1.csv
*       - Metric2.csv
 */

func NewCSVReceiver(fullPath string) (tr *CSVReceiver) {
	tr = &CSVReceiver{
		FullPath:          fullPath,
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
	}

	go tr.HandleSyncMetric()

	return tr
}

func (r CSVReceiver) UpdateMeasurements(ctx context.Context, msg *pb.MeasurementEnvelope) (*pb.Reply, error) {
	dbDir := r.FullPath + "/" + msg.GetDBName()
	metricFile := dbDir + msg.GetMetricName() + ".csv"

	// Create Database folder if does not exist
	err := os.MkdirAll(dbDir, os.ModePerm)
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(metricFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal("Unable to access file. Error: " + err.Error())
		return nil, err
	}

	writer := csv.NewWriter(file)

	customTagsJSON := sinks.GetJson(msg.GetCustomTags())
	for _, measurement := range msg.GetData() {
		record := []string{
			msg.GetMetricName(),
			sinks.GetJson(measurement),
			customTagsJSON,
		}

		if err := writer.Write(record); err != nil {
			log.Println("Unable to write to CSV file " + metricFile + "Error: " + err.Error())
			return nil, err
		}
	}

	writer.Flush()
	if err := writer.Error(); err != nil {
		return nil, err
	}
	return &pb.Reply{}, nil
}