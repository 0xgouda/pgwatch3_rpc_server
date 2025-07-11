package main

import (
	"context"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/cybertec-postgresql/pgwatch/v3/api"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
	tcollama "github.com/testcontainers/testcontainers-go/modules/ollama"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

const new_image = "tinyllama_image"

func initOllamaContainer(ctx context.Context) (*tcollama.OllamaContainer, error) {
	ollamaContainer, err := tcollama.Run(ctx, new_image)
	if err != nil {
		ollamaContainer, err = tcollama.Run(ctx, "ollama/ollama:0.1.25")
		if err != nil {
			log.Printf("failed to start container: %s", err)
			return nil, err
		}

		// Pull model and commit container
		_, _, err = ollamaContainer.Exec(ctx, []string{"ollama", "pull", "tinyllama"})
		if err != nil {
			log.Println("unable to pull llama3: " + err.Error())
			return nil, err
		}
		_ = ollamaContainer.Commit(ctx, new_image)
	}

	return ollamaContainer, nil
}

func initPostgresContainer(ctx context.Context) (*postgres.PostgresContainer, error) {
	dbName := "postgres"
	dbUser := "postgres"
	dbPassword := "postgres"

	postgresContainer, err := postgres.Run(ctx,
		"docker.io/postgres:16-alpine",
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPassword),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(5*time.Second)),
	)

	if err != nil {
		return nil, err
	}
	return postgresContainer, nil
}

func getMeasurementEnvelope() *api.MeasurementEnvelope {
	measurement := make(map[string]any)
	measurement["cpu"] = rand.Float64() * 1
	measurement["checkpointer"] = rand.Intn(100)
	var measurements []map[string]any
	measurements = append(measurements, measurement)

	sql := make(map[int]string)
	sql[12] = "select * from abc;"
	metrics := &api.Metric{
		SQLs:        sql,
		InitSQL:     "select * from abc;",
		NodeStatus:  "healthy",
		StorageName: "teststore",
		Description: "test metric",
	}

	return &api.MeasurementEnvelope{
		DBName:           "test",
		SourceType:       "test_source",
		MetricName:       "health",
		CustomTags:       nil,
		Data:             measurements,
		MetricDef:        *metrics,
		RealDbname:       "test",
		SystemIdentifier: "Identifier",
	}
}

func TestNewLLamaReceiver(t *testing.T) {
	ctx := context.Background()

	ollamaContainer, err := initOllamaContainer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	postgresContainer, err := initPostgresContainer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := ollamaContainer.Terminate(ctx); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
		if err := postgresContainer.Terminate(ctx); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()

	// Create new receiver
	connectionStr, err := ollamaContainer.ConnectionString(ctx)
	if err != nil {
		log.Println("Unable to get ollama connection string")
		t.Fatal(err)
	}

	pgConnectionStr, err := postgresContainer.ConnectionString(ctx)
	if err != nil {
		log.Println("Unable to get Postgres connection string")
		t.Fatal(err)
	}

	recv, err := NewLLamaReceiver(connectionStr, pgConnectionStr, ctx, 10)

	assert.NotNil(t, recv, "Receiver object is nil")
	assert.Nil(t, err, "Error encountered while creating receiver")
}

func TestSetupTables(t *testing.T) {
	ctx := context.Background()

	ollamaContainer, err := initOllamaContainer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	postgresContainer, err := initPostgresContainer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := ollamaContainer.Terminate(ctx); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
		if err := postgresContainer.Terminate(ctx); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()

	// Create new receiver
	connectionStr, err := ollamaContainer.ConnectionString(ctx)
	if err != nil {
		log.Println("Unable to get ollama connection string")
		t.Fatal(err)
	}

	pgConnectionStr, err := postgresContainer.ConnectionString(ctx)
	if err != nil {
		log.Println("Unable to get Postgres connection string")
		t.Fatal(err)
	}

	recv, err := NewLLamaReceiver(connectionStr, pgConnectionStr, ctx, 10)

	assert.NotNil(t, recv, "Receiver object is nil")
	assert.Nil(t, err, "Error encountered while creating receiver")

	// obtain db conneciton
	conn, err := recv.ConnPool.Acquire(recv.Ctx)

	assert.Nil(t, err, "error encountered while acquiring new connection")
	assert.NotNil(t, conn, "connection obtained in nil")
	defer conn.Release()

	// Call setup tables function
	err = recv.SetupTables()
	assert.Nil(t, err, "error encountered while setting up tables")

	// Check postgres for DB table
	var doesExist bool
	err = conn.QueryRow(recv.Ctx, `SELECT EXISTS (
		SELECT FROM information_schema.tables 
		WHERE  table_name   = 'db'
    );`).Scan(&doesExist)

	assert.Nil(t, err, "error encountered while querying table")
	assert.True(t, doesExist, "table DB does not exist")

	// Check postgres for Measurement table
	err = conn.QueryRow(recv.Ctx, `SELECT EXISTS (
		SELECT FROM information_schema.tables 
		WHERE  table_name   = 'measurements'
    );`).Scan(&doesExist)

	assert.Nil(t, err, "error encountered while querying table")
	assert.True(t, doesExist, "table Measurements does not exist")

	// Check postgres for Insights
	err = conn.QueryRow(recv.Ctx, `SELECT EXISTS (
		SELECT FROM information_schema.tables 
		WHERE  table_name   = 'insights'
    );`).Scan(&doesExist)

	assert.Nil(t, err, "error encountered while querying table")
	assert.True(t, doesExist, "table insights does not exist")
}

func TestUpdateMeasurements(t *testing.T) {
	ctx := context.Background()

	ollamaContainer, err := initOllamaContainer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	postgresContainer, err := initPostgresContainer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := ollamaContainer.Terminate(ctx); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
		if err := postgresContainer.Terminate(ctx); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()

	// Create new receiver
	connectionStr, err := ollamaContainer.ConnectionString(ctx)
	if err != nil {
		log.Println("Unable to get ollama connection string")
		t.Fatal(err)
	}

	pgConnectionStr, err := postgresContainer.ConnectionString(ctx)
	if err != nil {
		log.Println("Unable to get Postgres connection string")
		t.Fatal(err)
	}

	recv, err := NewLLamaReceiver(connectionStr, pgConnectionStr, ctx, 1)

	assert.NotNil(t, recv, "Receiver object is nil")
	assert.Nil(t, err, "Error encountered while creating receiver")

	// obtain db conneciton
	conn, err := recv.ConnPool.Acquire(recv.Ctx)

	assert.Nil(t, err, "error encountered while acquiring new connection")
	assert.NotNil(t, conn, "connection obtained in nil")
	defer conn.Release()

	// Get current number of insights in database
	if err != nil {
		t.Fatal(err)
	}

	// Send Update Measurements
	msg := getMeasurementEnvelope()
	logMsg := new(string)

	err = recv.UpdateMeasurements(msg, logMsg)

	assert.Nil(t, err, "error encountered while updating measurements")

	// Check insights table for new entry
	newInsightsCount := 0
	recv.InsightsGenerationWg.Wait()
	err = conn.QueryRow(recv.Ctx, "SELECT COUNT(*) FROM insights;").Scan(&newInsightsCount)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, newInsightsCount, 1, "No new entries inserted in insights table")
}

// Insert multiple records
func TestUpdateMeasurements_Multiple(t *testing.T) {
	ctx := context.Background()

	ollamaContainer, err := initOllamaContainer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	postgresContainer, err := initPostgresContainer(ctx)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := ollamaContainer.Terminate(ctx); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
		if err := postgresContainer.Terminate(ctx); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()

	// Create new receiver
	connectionStr, err := ollamaContainer.ConnectionString(ctx)
	if err != nil {
		log.Println("Unable to get ollama connection string")
		t.Fatal(err)
	}

	pgConnectionStr, err := postgresContainer.ConnectionString(ctx)
	if err != nil {
		log.Println("Unable to get Postgres connection string")
		t.Fatal(err)
	}

	recv, err := NewLLamaReceiver(connectionStr, pgConnectionStr, ctx, 1)

	assert.NotNil(t, recv, "Receiver object is nil")
	assert.Nil(t, err, "Error encountered while creating receiver")

	// obtain db conneciton
	conn, err := recv.ConnPool.Acquire(recv.Ctx)

	assert.Nil(t, err, "error encountered while acquiring new connection")
	assert.NotNil(t, conn, "connection obtained in nil")
	defer conn.Release()

	// Get current number of insights in database
	if err != nil {
		t.Fatal(err)
	}

	// Send Update Measurements
	msg := getMeasurementEnvelope()
	logMsg := new(string)

	oldCount := 0
	for range 10 {
		err = recv.UpdateMeasurements(msg, logMsg)
		assert.Nil(t, err, "error encountered while updating measurements")
	}

	newInsightsCount := 0
	t.Log("waiting.....")
	recv.InsightsGenerationWg.Wait()
	t.Log("waiting done")
	err = conn.QueryRow(recv.Ctx, "SELECT COUNT(*) FROM insights;").Scan(&newInsightsCount)
	if err != nil {
		t.Fatal(err)
	}
	assert.Greater(t, newInsightsCount, oldCount, "No new entries inserted in insights table")
}