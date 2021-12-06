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

// Compute all gaps in log and send a prepare message for each entry
func (state *PaxosState) CreatePrepareMapRequest() ([]uint64, uint64, error) {
	sqlStmt := `SELECT (logIndex, decree) FROM acceptedLogs ORDER BY logIndex ASC`
	rows, err := State.db.Query(sqlStmt)
	var lastSeen uint64 = 0
	ret := make([]uint64, 0)

	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var currIdx uint64
		var decree string
		if err = rows.Scan(&currIdx, &decree); err != nil {
			return nil, 0, err
		}
		for i := lastSeen + 1; i < currIdx; i++ {
			ret = append(ret, i)
		}
		lastSeen = currIdx
	}
	return ret, lastSeen + 1, nil
}

// Create a table to persist all necessary information
// highestVotedForDecree: In response to a prepare(n) message
//           return a map m: logIndex --> highestVotedForDecree
//           for all rows with ballotNumOfHighestVotedForDecree < n
// ballotNumOfHighestVotedForDecree: ballot num corresponding to highestVotedForDecree
func createPaxosInfoTable(db *sql.DB) error {
	sqlStmt := `CREATE TABLE IF NOT EXISTS paxosInfo(
					logIndex UNSIGNED BIG INT NOT NULL PRIMARY KEY, 
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
					logIndex UNSIGNED BIG INT NOT NULL PRIMARY KEY, 
					decree VARCHAR(100) NOT NULL
				)`
	_, err := db.Exec(sqlStmt)
	if err != nil {
		return err
	}
	log.Printf("Created table for storing accepted log entries")
	return nil
}
