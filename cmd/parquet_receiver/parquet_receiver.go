package main

import (
	"errors"
	"log"
	"os"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/parquet-go/parquet-go"
)

type ParquetReceiver struct {
	FullPath string
	sinks.SyncMetricHandler
}

type ParquetSchema struct {
	DBName            string
	SourceType        string
	MetricName        string
	Data              string // json string
	Tags              string
	MetricDefinitions string // json string
	SysIdentifier     string
}

func NewParquetReceiver(fullPath string) *ParquetReceiver {
	pr := &ParquetReceiver{
		FullPath:          fullPath,
		SyncMetricHandler: sinks.NewSyncMetricHandler(1024),
	}

	go pr.HandleSyncMetric()

	return pr
}

func (r ParquetReceiver) UpdateMeasurements(msg *api.MeasurementEnvelope, logMsg *string) error {
	if err := sinks.IsValidMeasurement(msg); err != nil {
		return  err
	}

	filename := msg.DBName + ".parquet"

	// Create temporary storage and buffer storage
	buffer_path := r.FullPath + "/parquet_readings"
	_ = os.MkdirAll(buffer_path, os.ModePerm)

	if _, err := os.Stat(buffer_path + "/" + filename); errors.Is(err, os.ErrNotExist) {
		_, _ = os.Create(buffer_path + "/" + filename)
		log.Println("[INFO]: Created File")
	}

	_, err := os.Open(buffer_path + "/" + filename)
	if err != nil {
		log.Println("[ERROR]: Unable to open file", err)
		return err
	}

	data_points, err := parquet.ReadFile[ParquetSchema](buffer_path + "/" + filename)
	if err != nil {
		data_points = []ParquetSchema{}
	}

	log.Println("[INFO]: Updated Measurements for Database: ", msg.DBName)
	for _, metric_data := range msg.Data {
		// populate data
		data := new(ParquetSchema)
		data.DBName = msg.DBName
		data.SourceType = msg.SourceType
		data.MetricName = msg.MetricName
		data.Data = sinks.GetJson(metric_data)
		data.MetricDefinitions = sinks.GetJson(msg.MetricDef)
		data.Tags = sinks.GetJson(msg.CustomTags)
		data.SysIdentifier = msg.SystemIdentifier

		// Append to data points
		data_points = append(data_points, *data)
	}

	err = parquet.WriteFile(buffer_path+"/"+filename, data_points)

	if err != nil {
		log.Println("[ERROR]: Unable to write to file.\nStacktrace -> ", err)
		return err
	}

	return nil
}
