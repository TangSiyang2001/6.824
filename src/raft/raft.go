package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//<----------------- Role -------------------
type Role int

const (
	Follower = iota
	Candidate
	Leader
)

//------------------------------------------>

type Term int
type LogEntry struct {
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//persistent state on all servers
	currentTerm Term
	votedFor    int
	log         []LogEntry
	role        Role
	//-------------------------------

	//volatile state on all servers
	commitIdx   int
	lastApplied int
	//-------------------------------

	//volatile state on leaders
	nextIdx          []int
	matchIdx         []int
	heartBeatTimeout time.Duration
	//-------------------------------

	//volitile state on follwers
	//150-300ms
	electionTimeout   time.Duration
	heartBeatListener chan int
	//-------------------------------
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return int(rf.currentTerm), rf.role == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//<-------------------------------------------------- AppenEntries ------------------------------------------------
type AppenEntriesArgs struct {
	//leader's term
	Term
	LeaderId    int
	PrevLogIdx  int
	PrevLogTerm Term
	Entries     []LogEntry
	//leader's commitIndex
	LeaderCommit int
}

type AppenEntriesReply struct {
	Term
	Success bool
}

func (rf *Raft) AppendEntries(args *AppenEntriesArgs, reply *AppenEntriesReply) {
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.currentTerm {
		if rf.role != Follower {
			rf.role = Follower
		}
		if rf.votedFor != -1 {
			rf.votedFor = -1
		}
		rf.resetElecTimeout()
		//We just ensure the heartbeat ability in lab 2a
		//TODO:further log replication
		rf.currentTerm = args.Term
		reply.Success = true
		reply.Term = rf.currentTerm
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppenEntriesArgs, reply *AppenEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) startHeartBeat() {
	Log(dInfo, "leader :{%v} start heartbeat", rf.me)
	for rf.role == Leader {
		args := AppenEntriesArgs{
			Term:     rf.currentTerm,
			LeaderId: rf.me,
			//TODO:to be modify in continuing labs
			PrevLogIdx:   0,
			PrevLogTerm:  0,
			Entries:      []LogEntry{},
			LeaderCommit: rf.commitIdx,
		}
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			reply := AppenEntriesReply{}
			go func(id int) {
				//TODO:think should heartbeat rpc be retry here
				rf.sendAppendEntries(id, &args, &reply)
				//TODO:process reply
			}(i)
		}
		time.Sleep(rf.heartBeatTimeout)
	}
}

//-----------------------------------------------------------------------------------------------------------------

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//current term
	Term
	CandidateId int
	LastLogIdx  int
	LastLogTerm Term
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if rf.killed() {
		return
	}
	if rf.role == Leader {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.currentTerm &&
		(rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		// &&rf.isLogUpToDay(args.LastLogTerm, args.LastLogIdx) {

		Log(dInfo, "vote for candidate:{%v}", args.CandidateId)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.resetElecTimeout()
		return
	}
}

func (rf *Raft) isLogUpToDay(logTerm Term, logIdx int) bool {
	return logTerm > rf.currentTerm || (logTerm == rf.currentTerm && logIdx >= rf.commitIdx)
}

func (rf *Raft) resetElecTimeout() {
	rf.electionTimeout = rf.newElecTimeout()
}

func (rf *Raft) newElecTimeout() time.Duration {
	rand.Seed(time.Now().Unix())
	return time.Duration(150+rand.Intn(200)) * time.Millisecond
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//time.After() might be more suitable than time.Sleep()
		for rf.role != Leader {
			select {
			case <-rf.heartBeatListener:
				rf.resetElecTimeout()
				Log(dInfo, "get hearbeat")
			case <-time.After(rf.electionTimeout):
				rf.elect()
			}
		}
		// if rf.role == Candidate {
		// 	Log(dError, "invalid state,should not be candidate here")
		// 	rf.Kill()
		// }
		//rf.role is Leader when arriving here
		if rf.role == Leader {
			rf.startHeartBeat()
		}
	}
}

func (rf *Raft) elect() {
	rf.role = Candidate
	rf.currentTerm++
	//TODO:preprocess before an election
	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		//TODO:modify in continuing labs
		LastLogIdx:  0,
		LastLogTerm: 0,
	}
	var nVotes int = 0
	//vote for self first
	reply := RequestVoteReply{}
	if rf.votedFor != -1 {
		rf.votedFor = -1
	}
	rf.sendRequestVote(rf.me, &args, &reply)
	if !reply.VoteGranted {
		Log(dError, "fail to vote for self ,me::%v", rf.me)
		rf.role = Follower
		return
	}
	nVotes++

	//request for vote
	nPeers := len(rf.peers)
	nMajority := nPeers/2 + 1
	votesListener := make(chan int, nPeers)
	for i := 0; i < nPeers; i++ {
		if i == rf.me {
			continue
		}
		go func(id int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(id, &args, &reply)
			if reply.VoteGranted {
				//TODO:deal with reply.Term
				votesListener <- 1
			}
		}(i)
	}
	for rf.role == Candidate {
		timeout := rf.electionTimeout
		select {
		case <-votesListener:
			timeStart := time.Now()
			nVotes++
			if nVotes >= nMajority {
				Log(dInfo, "me :{%v} will becomes leader", rf.me)
				//election is succeed
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.role == Candidate {
					Log(dInfo, "me :{%v} becomes leader", rf.me)
					rf.role = Leader
				}
				return
			}

			timeout -= time.Since(timeStart)
			if timeout <= 0 {
				rf.elect()
			}
		case <-time.After(timeout):
			//restart election
			//TODO:check the timeout here,it won't be rf.electionTimeout every single loop
			// rf.role = Follower
			//retrieve to restart an election
			rf.elect()
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	InitLog()
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.commitIdx = 0
	rf.currentTerm = 0
	rf.electionTimeout = rf.newElecTimeout()
	rf.heartBeatListener = make(chan int, 1)
	rf.heartBeatTimeout = 100 * time.Millisecond
	rf.lastApplied = 0
	rf.log = []LogEntry{}
	rf.matchIdx = []int{}
	rf.nextIdx = []int{}
	rf.role = Follower
	rf.votedFor = -1
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	Log(dInfo, "init")
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
