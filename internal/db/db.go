package db

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/awitten1/multipaxos/internal/rpc"
	_ "github.com/mattn/go-sqlite3"
)

var (
	DBPath string
	DB     *PaxosState
)

type PaxosState struct {
	db         *sql.DB
	nextBallot uint64
	mu         sync.RWMutex
}

type PaxosInstanceState struct {
	LogIndex                         uint64
	HighestVotedForDecree            string
	BallotNumOfHighestVotedForDecree uint64
}

// Get the accepted log entries (could have gaps)
func (s *PaxosState) GetLog() ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sqlStmt := `SELECT *
				FROM acceptedLogs
				ORDER BY logIndex ASC`
	rows, err := s.db.Query(sqlStmt)
	if err != nil {
		log.Printf("Error querying accepted log entry table: %s", err.Error())
		return nil, err
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
	return logEntries, nil
}

// Get information on a paxos instance
func (s *PaxosState) PaxosInstanceState(logIndex uint64) (*PaxosInstanceState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sqlStmt := `SELECT logIndex, decree, ballotNum
				FROM paxosInfo
				WHERE logIndex=?`
	rows, err := s.db.Query(sqlStmt, logIndex)
	if err != nil {
		log.Printf("Failure getting paxos instance info: %s", err.Error())
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

// Insert a new accepted log entry into the accepted logs table
func (s *PaxosState) InsertAcceptedLog(entry *AcceptedLog) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sqlStmt := `INSERT INTO acceptedLogs(logIndex, decree) VALUES(?,?)`
	_, err := s.db.Exec(sqlStmt, entry.Index, entry.Decree)
	if err != nil {
		log.Printf("failed to insert accept log entry: %s", err.Error())
	}
}

// Print all paxos info information
func (s *PaxosState) PrintPaxosInfo() {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sqlStmt := `SELECT * FROM paxosInfo ORDER BY logIndex ASC`
	rows, err := s.db.Query(sqlStmt)
	if err != nil {
		log.Printf("Could not print paxos info: %s", err.Error())
		return
	}
	defer rows.Close()
	var prev uint64 = 0
	output := make([]string, 0)
	for rows.Next() {
		var currIdx uint64
		var dec string
		var ballotNumOfHighestVotedForDec uint64
		rows.Scan(&currIdx, &dec, &ballotNumOfHighestVotedForDec)
		for i := prev; i < currIdx-1; i++ {
			output = append(output, "---")
		}
		output = append(output, fmt.Sprintf("(%s,%d)", dec, ballotNumOfHighestVotedForDec))
		prev = currIdx
	}
	fmt.Println(strings.Join(output, ","))
}

type AcceptedLog struct {
	Index  uint64
	Decree string
}

// Get a single accepted log entry
func (s *PaxosState) GetAcceptedLog(logIndex uint64) (*AcceptedLog, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sqlStmt := `SELECT * FROM acceptedLogs WHERE logIndex=?`
	rows, err := s.db.Query(sqlStmt, logIndex)
	if err != nil {
		log.Printf("Error retrieving accepted log: %s", err.Error())
		return nil, err
	}
	defer rows.Close()
	if rows.Next() {
		var ret AcceptedLog
		if err = rows.Scan(&ret.Index, &ret.Decree); err != nil {
			log.Printf("Error scanning for accepted log: %s", err.Error())
			return nil, err
		}
		return &ret, nil
	}
	return nil, nil
}

// upsert paxos log instance information
func (s *PaxosState) UpsertPaxosLogState(logIndex uint64, decree string, ballotNum uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	sqlStmt := `INSERT INTO paxosInfo
					(logIndex, decree, ballotNum)
					VALUES(?,?,?)
	  				ON CONFLICT(logIndex) DO UPDATE SET
					  decree=?,
					  ballotNum=?`

	stmt, err := tx.Prepare(sqlStmt)
	if err != nil {
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec(logIndex, decree, ballotNum, decree, ballotNum)
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}

// setup db
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
	nextBallot, err := createNextBallotNumberTable(db)
	if err != nil {
		return nil, err
	}

	log.Printf("Created necessary database tables")

	return &PaxosState{db: db, nextBallot: nextBallot}, nil
}

// Get and update proposal number (only call when sending a prepare message)
func (state *PaxosState) GetAndUpdateProposalNumber() (uint64, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	new := atomic.AddUint64(&state.nextBallot, 1)
	sqlStmt := `UPDATE proposalNumber
				SET proposalNumber=?
				WHERE id=1`
	_, err := state.db.Exec(sqlStmt, new)
	if err != nil {
		log.Printf("Could not update proposal number in table: %s", err.Error())
		return 0, err
	}
	state.nextBallot = new
	return new, err
}

func (state *PaxosState) GetBallotNumber() uint64 {
	return state.nextBallot
}

// Compute first gap in log and send a prepare message for each entry
func (state *PaxosState) ComputeFirstGapInLog() (uint64, error) {
	state.mu.RLock()
	defer state.mu.RUnlock()
	sqlStmt := `SELECT logIndex FROM acceptedLogs ORDER BY logIndex ASC`
	rows, err := state.db.Query(sqlStmt)
	var prev uint64 = 0

	if err != nil {
		return 0, err
	}
	defer rows.Close()
	for rows.Next() {
		var currIdx uint64
		if err = rows.Scan(&currIdx); err != nil {
			return 0, err
		}

		// Found first gap
		if prev+1 != currIdx {
			return prev, nil
		}
	}
	// Could be missing entries after last element if no gaps found before
	return prev + 1, nil
}

func (s *PaxosState) GetMaxAcceptedLogIndex() (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sqlStmt := `SELECT COALESCE(MAX(logIndex), 0)
				FROM acceptedLogs`
	rows, err := s.db.Query(sqlStmt)
	if err != nil {
		log.Printf("Got an error trying to compute the max accepted log entry")
		return 0, err
	}
	defer rows.Close()
	var idx uint64
	if rows.Next() {
		if err = rows.Scan(&idx); err != nil {
			log.Printf("error scanning for max accepted log: %s", err.Error())
			return 0, err
		}
	} else {
		return 0, nil
	}
	return idx, nil
}

// Create promise body
func (state *PaxosState) CreatePrepareResp(prepareBody *rpc.PrepareBody) (*rpc.PromiseBody, error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	sqlStmt := `SELECT logIndex, decree, ballotNum
				FROM paxosInfo
				WHERE logIndex >= ?`
	rows, err := state.db.Query(sqlStmt, prepareBody.SmallestUndecidedIndex)
	if err != nil {
		log.Printf("Could not create promise body: %s", err.Error())
		return nil, err
	}
	resp := rpc.PromiseBody{Decrees: make(map[uint64]string), AckedAsLeader: true, BallotNums: make(map[uint64]uint64)}

	sqlStmt = `UPDATE paxosInfo
				SET ballotNum=?
				WHERE logIndex=?`

	defer rows.Close()
	for rows.Next() {
		var logIndex uint64
		var decree string
		var ballotNum uint64
		if err = rows.Scan(&logIndex, &decree, &ballotNum); err != nil {
			log.Printf("Could not build promise body (during scan): %s", err.Error())
			return nil, err
		}

		defer state.db.Exec(sqlStmt, logIndex, prepareBody.GetProposalNumber())
		if err != nil {
			log.Printf("failed to update row (during the promise): %s", err.Error())
			return &rpc.PromiseBody{AckedAsLeader: false}, nil
		}

		if ballotNum < prepareBody.GetProposalNumber() {
			resp.Decrees[logIndex] = decree
			resp.BallotNums[logIndex] = ballotNum
		} else {
			return &rpc.PromiseBody{AckedAsLeader: false}, nil
		}
	}
	return &resp, nil
}

// Create a table to persist all necessary information
// decree: In response to a prepare(n) message
//           return a map m: logIndex --> decree
//           for all rows with ballotNum < n
// ballotNum: ballot num corresponding to decree
func createPaxosInfoTable(db *sql.DB) error {
	sqlStmt := `CREATE TABLE IF NOT EXISTS paxosInfo(
					logIndex UNSIGNED BIG INT NOT NULL PRIMARY KEY,
					decree VARCHAR(100),
					ballotNum INT
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

// Create a table to persist previous proposal number.  This table will only have one row
func createNextBallotNumberTable(db *sql.DB) (uint64, error) {
	sqlStmt := `CREATE TABLE IF NOT EXISTS proposalNumber(
				id INT PRIMARY KEY,
				proposalNumber UNSIGNED BIG INT NOT NULL)`
	_, err := db.Exec(sqlStmt)
	if err != nil {
		return 0, err
	}
	rows, err := db.Query("SELECT proposalNumber FROM proposalNumber")
	if err != nil {
		log.Printf("Could not initialize proposal number")
		return 0, err
	}
	defer rows.Close()
	var nextBallot uint64 = 1
	if rows.Next() {
		rows.Scan(&nextBallot)
	} else {
		sqlStmt = `INSERT INTO proposalNumber VALUES(1, 1)`
		_, err := db.Exec(sqlStmt)
		if err != nil {
			log.Printf("Could not insert row into proposal number table")
			return 0, err
		}
	}

	log.Printf("Created table for persisting proposal number")
	return nextBallot, nil
}
