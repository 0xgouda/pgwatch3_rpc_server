package main

import (
	"flag"
	"log"

	"github.com/destrex271/pgwatch3_rpc_server/sinks"
)

func main() {
	port := flag.String("port", "-1", "Specify the port where you want your sink to receive the measurements on.")
	StorageFolder := flag.String("rootFolder", ".", "Only for formats like CSV...\n")
	flag.Parse()

	if *port == "-1" {
		log.Println("[ERROR]: No Port Specified")
		return
	}
	server := NewCSVReceiver(*StorageFolder)

	// if no error it should never return
	err := sinks.Listen(server, *port)
	if err != nil {
		log.Fatal(err)
	}
}
