package db

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/awitten1/multipaxos/internal/server"
	_ "github.com/mattn/go-sqlite3"
)

var (
	DBPath string
	State  *PaxosState
)

type PaxosState struct {
	db *sql.DB
}

func SetupDB() (*PaxosState, error) {
	log.Printf("setting up persistent storage")
	path := fmt.Sprintf("%s/paxos-%d.db", DBPath, server.Replica)
	log.Printf("Using path %s to persist all information", path)
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}

	err = createAcceptedLogTable(db)
	if err != nil {
		return nil, err
	}
	err = createPaxosInfoTable(db)
	if err != nil {
		return nil, err
	}

	log.Printf("Created necessary database tables")

	return &PaxosState{db: db}, nil
}

// Create a table to persist all necessary information
func createPaxosInfoTable(db *sql.DB) error {
	sqlStmt := `CREATE TABLE IF NOT EXISTS paxosInfo(
					logIndex INT NOT NULL PRIMARY KEY, 
					nextBallot INT, 
					highestVotedForDecree VARCHAR(100), 
					ballotNumOfHighestVotedForDecree INT
				)`
	_, err := db.Exec(sqlStmt)
	if err != nil {
		return err
	}
	return nil
}

// Create a table to persist all accepted logs
func createAcceptedLogTable(db *sql.DB) error {
	sqlStmt := `CREATE TABLE IF NOT EXISTS acceptedLogs(
					logIndex INT NOT NULL PRIMARY KEY, 
					decree VARCHAR(100) NOT NULL
				)`
	_, err := db.Exec(sqlStmt)
	if err != nil {
		return err
	}
	log.Printf("Created table for storing accepted log entries")
	return nil
}
