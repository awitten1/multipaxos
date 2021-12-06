package main

import (
	"flag"
	"fmt"
	"log"
	"strings"

	db "github.com/awitten1/multipaxos/internal/db"
	"github.com/awitten1/multipaxos/internal/server"
)

func main() {
	err := parseCliArgs()
	if err != nil {
		panic(err)
	}
	db.State, err = db.SetupDB()
	if err != nil {
		panic(err)
	}
	log.Printf("Starting server")
	server.StartServer()
}

func parseCliArgs() error {
	var peers string
	var port int
	var replicaId int

	flag.StringVar(&peers, "peers", "", "Comma separated list of <ip address>:<port number>")
	flag.IntVar(&port, "port", -1, "Port number to listen on")
	flag.IntVar(&replicaId, "replica", -1, "Replica ID number (must be unique)")
	flag.StringVar(&db.DBPath, "dbpath", "", "Directory to persist to")

	flag.Parse()
	if peers == "" {
		return fmt.Errorf("must provide list of peers")
	}
	if port < 0 {
		return fmt.Errorf("must provide valid port number")
	}
	server.Port = int32(port)
	if replicaId < 0 || replicaId >= 256 {
		return fmt.Errorf("Replica Id must be a positive, 8-bit integer")
	}
	server.Replica = int8(replicaId)
	if db.DBPath == "" {
		return fmt.Errorf("must provide a db directory")
	}
	server.Peers = strings.Split(peers, ",")

	return nil
}
