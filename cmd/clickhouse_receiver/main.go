package main

import (
	"flag"
	"log"
	"os"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
)

func main() {
	port := flag.String("port", "-1", "Specify the port where you want your sink to receive the measurements on.")
	flag.Parse()

	User := os.Getenv("CLICKHOUSE_USER")
	Password := os.Getenv("CLICKHOUSE_PASSWORD")
	serverURI := os.Getenv("CLICKHOUSE_SERVER_URI")
	DBName := os.Getenv("CLICKHOUSE_DB")

	server, err := NewClickHouseReceiver(User, Password, DBName, serverURI, false)
	if err != nil {
		log.Fatalf("Unable to create ClickHouse receiver: %v", err)
	}

	if err = sinks.Listen(server, *port); err != nil {
		log.Fatal(err)
	}
}
