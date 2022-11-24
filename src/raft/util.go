package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

//<-------------------------------- Debug utils ---------------------------------------
//refer to https://blog.josejg.com/debugging-pretty/

// Debugging
const Debug = false

type logTopic string

const (
	dClient  logTopic = "CLNT"
	dCommit  logTopic = "CMIT"
	dDrop    logTopic = "DROP"
	dError   logTopic = "ERRO"
	dInfo    logTopic = "INFO"
	dLeader  logTopic = "LEAD"
	dLog     logTopic = "LOG1"
	dLog2    logTopic = "LOG2"
	dPersist logTopic = "PERS"
	dSnap    logTopic = "SNAP"
	dTerm    logTopic = "TERM"
	dTest    logTopic = "TEST"
	dTimer   logTopic = "TIMR"
	dTrace   logTopic = "TRCE"
	dVote    logTopic = "VOTE"
	dWarn    logTopic = "WARN"
)

func getVerbosity() int {
	verb := os.Getenv("VERBOSE")
	lev := 0
	if verb != "" {
		var err error
		lev, err = strconv.Atoi(verb)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", verb)
		}
	}
	return lev
}

var debugStart time.Time
var debugVerbosity int

func initLog() {
	debugVerbosity = getVerbosity()
	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
	debugStart = time.Now()
}

func Log(topic logTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

//------------------------------------------------------------------------------------>
