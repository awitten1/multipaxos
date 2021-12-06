package db

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

var (
	DBPath string
	State  *PaxosState
)

type PaxosState struct {
	db *sql.DB
}

type PaxosInstanceState struct {
	LogIndex                         uint64
	HighestVotedForDecree            string
	BallotNumOfHighestVotedForDecree uint64
}

func (s *PaxosState) PrintLog() {
	sqlStmt := `SELECT *
				FROM acceptedLogs
				ORDER BY logIndex ASC`
	rows, err := s.db.Query(sqlStmt)
	if err != nil {
		log.Printf("Error querying accepted log entry table")
		return
	}
	defer rows.Close()
	prev := 0
	logEntries := make([]string, 0)
	for rows.Next() {
		var logIndex uint64
		var logEntry string
		rows.Scan(&logIndex, &logEntry)
		for i := prev; i < int(logIndex)-1; i++ {
			logEntries = append(logEntries, "-")
		}
		logEntries = append(logEntries, logEntry)
		prev = int(logIndex)
	}
	log.Print(strings.Join(logEntries, ","))
}

func (s *PaxosState) GetRowByLogIndex(logIndex uint64) (*PaxosInstanceState, error) {
	sqlStmt := `SELECT logIndex, highestVotedForDecree, ballotNumOfHighestVotedForDecree
				FROM paxosInfo
				WHERE logIndex=?`
	rows, err := s.db.Query(sqlStmt, logIndex)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var paxosInstance PaxosInstanceState
	if rows.Next() {
		if err = rows.Scan(&paxosInstance.LogIndex, &paxosInstance.HighestVotedForDecree, &paxosInstance.BallotNumOfHighestVotedForDecree); err != nil {
			return nil, err
		}
	}
	return &paxosInstance, nil
}

func (s *PaxosState) InsertAcceptedLog(logIndex uint64, decree string) {
	sqlStmt := `INSERT INTO acceptedLogs(logIndex, decree) VALUES(?,?)`
	_, err := s.db.Exec(sqlStmt, logIndex, decree)
	if err != nil {
		log.Printf("failed to insert accept log entry: %s", err.Error())
	}
}

func (s *PaxosState) UpsertPaxosLogState(logIndex uint64, decree string, ballotNum uint64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	sqlStmt := `INSERT INTO paxosInfo
					(logIndex, highestVotedForDecree, ballotNumOfHighestVotedForDecree)
					VALUES(?,?,?)
	  				ON CONFLICT(logIndex) DO UPDATE SET
					  highestVotedForDecree=?,
					  ballotNumOfHighestVotedForDecree=?`

	stmt, err := tx.Prepare(sqlStmt)
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec(logIndex, decree, ballotNum)
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

func SetupDB(replica uint8) (*PaxosState, error) {
	log.Printf("setting up persistent storage")
	path := fmt.Sprintf("%s/paxos-%d.db", DBPath, replica)
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
