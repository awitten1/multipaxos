package db

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
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
}

type PaxosInstanceState struct {
	LogIndex                         uint64
	HighestVotedForDecree            string
	BallotNumOfHighestVotedForDecree uint64
}

func (s *PaxosState) GetLog() ([]string, error) {
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

type AcceptedLog struct {
	Index  uint64
	Decree string
}

func (s *PaxosState) GetAcceptedLog(logIndex uint64) (*AcceptedLog, error) {
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
	nextBallot, err := createNextBallotNumberTable(db)
	if err != nil {
		return nil, err
	}

	log.Printf("Created necessary database tables")

	return &PaxosState{db: db, nextBallot: nextBallot}, nil
}

func (state *PaxosState) GetAndUpdateProposalNumber() (uint64, error) {
	new := atomic.AddUint64(&state.nextBallot, 1)
	sqlStmt := `UPDATE proposalNumber
				SET proposalNumber=?
				WHERE id=1`
	_, err := state.db.Exec(sqlStmt)
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

// Compute all gaps in log and send a prepare message for each entry
func (state *PaxosState) CreatePrepareRequest() (uint64, error) {
	sqlStmt := `SELECT (logIndex) FROM acceptedLogs ORDER BY logIndex ASC`
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

func (state *PaxosState) CreatePrepareResp(prepareBody *rpc.PrepareBody) (*rpc.PromiseBody, error) {
	sqlStmt := `SELECT (logIndex, highestVotedForDecree, ballotNumOfHighestVotedForDecree)
				FROM paxosInfo
				WHERE logIndex > ?`
	rows, err := state.db.Query(sqlStmt, prepareBody.SmallestUndecidedIndex)
	if err != nil {
		log.Printf("Could not create promise body: %s", err.Error())
		return nil, err
	}
	resp := rpc.PromiseBody{Decrees: make(map[uint64]string), AckedAsLeader: true, CorrespondingBallotNum: make(map[uint64]uint64)}

	defer rows.Close()
	for rows.Next() {
		var logIndex uint64
		var highestVotedForDecree string
		var ballotNumOfHighestVotedForDecree uint64
		if err = rows.Scan(&logIndex, &highestVotedForDecree, &ballotNumOfHighestVotedForDecree); err != nil {
			log.Printf("Could not build promise body (during scan): %s", err.Error())
			return nil, err
		}
		if ballotNumOfHighestVotedForDecree < prepareBody.GetProposalNumber() {
			resp.Decrees[logIndex] = highestVotedForDecree
			resp.CorrespondingBallotNum[logIndex] = ballotNumOfHighestVotedForDecree
		} else {
			return &rpc.PromiseBody{AckedAsLeader: false}, nil
		}
	}
	return &resp, nil
}

func (state *PaxosState) CreateCurrentReplicaPromiseMaps(logIndex uint64) (*rpc.PromiseBody, error) {
	sqlStmt := `SELECT (logIndex, highestVotedForDecree, ballotNumOfHighestVotedForDecree)
		FROM paxosInfo
		WHERE logIndex > ?`
	rows, err := state.db.Query(sqlStmt, logIndex)
	if err != nil {
		log.Printf("Could not query local databse for sending prepare: %s", err.Error())
		return nil, err
	}
	defer rows.Close()
	ret := rpc.PromiseBody{Decrees: make(map[uint64]string), CorrespondingBallotNum: make(map[uint64]uint64), AckedAsLeader: true}
	for rows.Next() {
		var logIndex uint64
		var highestVotedForDecree string
		var ballotNumOfHighestVotedForDecree uint64
		if err = rows.Scan(&logIndex, &highestVotedForDecree, &ballotNumOfHighestVotedForDecree); err != nil {
			log.Printf("Could not build promise body (during scan): %s", err.Error())
			return nil, err
		}
		if acceptedLog, _ := DB.GetAcceptedLog(logIndex); acceptedLog != nil {
			continue
		}
		ret.Decrees[logIndex] = highestVotedForDecree
		ret.CorrespondingBallotNum[logIndex] = ballotNumOfHighestVotedForDecree
	}
	return &ret, nil
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

// Create a table to persist previous proposal number.  This table will only have one row
func createNextBallotNumberTable(db *sql.DB) (uint64, error) {
	sqlStmt := `CREATE TABLE IF NOT EXISTS proposalNumber(
				id INT PRIMARY KEY,
				proposalNumber UNSIGNED BIG INT NOT NULL)`
	_, err := db.Exec(sqlStmt)
	if err != nil {
		return 0, err
	}
	rows, err := db.Query("SELECT (proposalNumber) FROM proposalNumber")
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
