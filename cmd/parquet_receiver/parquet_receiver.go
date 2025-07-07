package main

import (
	"context"
	"log"
	"os"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
	"github.com/destrex271/pgwatch3_rpc_server/sinks/pb"

	"github.com/parquet-go/parquet-go"
)

type ParquetReceiver struct {
	bufferPath string
	sinks.SyncMetricHandler
}

type ParquetSchema struct {
	DBName            string
	MetricName        string
	Data              string // json string
	Tags              string
}

func NewParquetReceiver(fullPath string) *ParquetReceiver {
	// Create buffer storage
	buffer_path := fullPath + "/parquet_readings"
	_ = os.MkdirAll(buffer_path, os.ModePerm)

	pr := &ParquetReceiver{
		bufferPath: buffer_path,
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
	}

	go pr.HandleSyncMetric()
	return pr
}

func (r ParquetReceiver) UpdateMeasurements(ctx context.Context, msg *pb.MeasurementEnvelope) (reply *pb.Reply, err error) {
	if err = sinks.IsValidMeasurement(msg); err != nil {
		return reply, err
	}

	filename := msg.DBName + ".parquet"
	if _, err := os.Stat(r.bufferPath + "/" + filename); os.IsNotExist(err) {
		_, _ = os.Create(r.bufferPath + "/" + filename)
		log.Printf("[INFO]: Created File %s", filename)
	}

	_, err = os.Open(r.bufferPath + "/" + filename)
	if err != nil {
		log.Println("[ERROR]: Unable to open file", err)
		return reply, err
	}

	data_points, err := parquet.ReadFile[ParquetSchema](r.bufferPath + "/" + filename)
	if err != nil {
		data_points = []ParquetSchema{}
	}

	for _, metric_data := range msg.Data {
		data := ParquetSchema{}
		data.DBName = msg.DBName
		data.MetricName = msg.MetricName
		data.Data = sinks.GetJson(metric_data)
		data.Tags = sinks.GetJson(msg.CustomTags)
		// Append to data points
		data_points = append(data_points, data)
	}

	err = parquet.WriteFile(r.bufferPath + "/" + filename, data_points)
	if err != nil {
		log.Println("[ERROR]: Unable to write to file.\nStacktrace -> ", err)
		return reply, err
	}
	log.Println("[INFO]: Updated Measurements for Database: ", msg.DBName)

	return reply, nil
}
