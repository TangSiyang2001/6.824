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
	Term
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	muCh      chan bool           // channel to release the lock
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
	//mind that log idx start from 1
	nextIdx          []int
	matchIdx         []int
	heartBeatTimeout time.Duration
	applyCh          chan ApplyMsg
	//-------------------------------

	//volitile state on follwers
	//150-350ms
	electionTimeout time.Duration
	timeoutResetCh  chan int
	closeCh         chan int
	//-------------------------------
}

func (rf *Raft) lock(name string) {
	Log(dInfo, "lock: {%v} on node {%v}", name, rf.me)
	rf.mu.Lock()
	go func() {
		select {
		case <-time.After(5 * time.Second):
			Log(dError, "dead lock {%v} on node {%v}", name, rf.me)
			rf.mu.Unlock()
		case <-rf.muCh:
		}
	}()
}

func (rf *Raft) isMajority(n int) bool {
	return n*2 >= len(rf.peers)
}

func (rf *Raft) unlock(name string) {
	Log(dInfo, "unlock %v on node %v", name, rf.me)
	rf.muCh <- true
	rf.mu.Unlock()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.lock("GET STATE")
	defer rf.unlock("GET STATE")
	Log(dSnap, "me :{%v},role :{%v},term{%v}", rf.me, rf.role, rf.currentTerm)
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

func (rf *Raft) resetElecTimeout() {
	rf.electionTimeout = rf.newElecTimeout()
	rf.timeoutResetCh <- 1
}

func (rf *Raft) newElecTimeout() time.Duration {
	// rand.Seed(time.Now().Unix())
	return time.Duration(150+rand.Intn(200)) * time.Millisecond
}

func (rf *Raft) stepDown(newTerm Term) {
	if newTerm < rf.currentTerm {
		return
	}
	rf.role = Follower
	rf.currentTerm = newTerm
	rf.unvote()
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
	//FIXME:skeptical dead lock once happended here,try to make a repetition
	//TODO:try to add role identification
	rf.lock("append entries")
	defer rf.unlock("append entries")
	reply.Success = false
	if rf.killed() {
		reply.Term = -1
		return
	}
	if args.Term >= rf.currentTerm {
		rf.stepDown(args.Term)
		rf.votedFor = args.LeaderId
		//We just ensure the heartbeat ability in lab 2a
		//TODO:further log replication
		reply.Success = true
		rf.resetElecTimeout()
	}
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppenEntriesArgs, reply *AppenEntriesReply) bool {
	//TODO:think about rpc timeout and retry
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) leaderLoop() {
	stepDownCh := make(chan Term, 1)
	for !rf.killed() && rf.role == Leader {
		Log(dInfo, "leader :{%v} start heartbeat,term :{%v}", rf.me, rf.currentTerm)
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(server int) {
				args := AppenEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIdx:   0,
					PrevLogTerm:  0,
					Entries:      nil,
					LeaderCommit: rf.commitIdx,
				}

				//log replication related info
				//log idx start from 1
				args.Entries = rf.log[rf.nextIdx[server]-1:]
				if rf.nextIdx[server] > 0 {
					prevLogIdx := rf.nextIdx[server] - 1
					args.PrevLogIdx = prevLogIdx
					args.PrevLogTerm = rf.log[prevLogIdx-1].Term
				}

				reply := AppenEntriesReply{}
				//TODO:think aboutshould heartbeat rpc be retry here
				ok := rf.sendAppendEntries(server, &args, &reply)
				//step down if we found a term higher than the current server
				if ok && !reply.Success && reply.Term > rf.currentTerm {
					//stepdown
					stepDownCh <- reply.Term
				}
				//TODO:process reply
			}(i)
		}

		select {
		case term := <-stepDownCh:
			//when arriving here,step down to become a follower
			rf.stepDown(term)
			rf.resetElecTimeout()
			return
		case <-time.After(rf.heartBeatTimeout):
			//do nothing
		case <-rf.closeCh:
			return
		}
	}
}

//-----------------------------------------------------------------------------------------------------------------

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
	// Your code here (2B).
	index := -1
	term := -1
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() || rf.role != Leader {
		return index, term, false
	}
	//append log
	log := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, log)
	return len(rf.log), term, true
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
	Log(dInfo, "kill %v,role: %v,term : %v", rf.me, rf.role, rf.currentTerm)
	close(rf.closeCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) nonLeaderLoop() {
	//non-leader loop
	electionCh := make(chan bool, 1)
	for !rf.killed() && rf.role != Leader {
		select {
		case <-time.After(rf.electionTimeout):
			Log(dInfo, "elec timeout ,me %v", rf.me)
			go rf.elect(&electionCh)
		case <-electionCh:
			//donothing,fall through to break the non-leader loop
		case <-rf.timeoutResetCh:
		case <-rf.closeCh:
			return
		}
	}
}

//<------------------------------------------ Leader election -----------------------------------
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

func (rf *Raft) unvote() {
	rf.votedFor = -1
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	rf.lock("req vote")
	defer rf.unlock("req vote")
	if rf.killed() {
		reply.Term = -1
		return
	}
	if rf.role == Leader || args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.stepDown(args.Term)
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.role = Follower
		rf.votedFor = args.CandidateId
		Log(dInfo, "vote for candidate:{%v},me :%v,term {%v}", args.CandidateId, rf.me, rf.currentTerm)
	}
	rf.resetElecTimeout()
	reply.Term = rf.currentTerm
}

func (rf *Raft) isLogUpToDay(logTerm Term, logIdx int) bool {
	return logTerm > rf.currentTerm || (logTerm == rf.currentTerm && logIdx >= rf.commitIdx)
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

//prevote aims to ensure the candidate can communicate with the majority of peers
type PrevoteArgs struct {
}

type PrevoteReply struct {
	Succ bool
}

func (rf *Raft) sendPrevote(server int, args *PrevoteArgs, reply *PrevoteReply) bool {
	ok := rf.peers[server].Call("Raft.PreVote", args, reply)
	return ok
}

func (rf *Raft) PreVote(args *PrevoteArgs, reply *PrevoteReply) {
	//TODO:prevote restriction
	reply.Succ = true
}

func (rf *Raft) paraReqPreVote() bool {
	respCh := make(chan PrevoteReply, 1)
	wg := sync.WaitGroup{}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(server int) {
			defer wg.Done()
			reply := PrevoteReply{
				Succ: false,
			}
			ok := rf.sendPrevote(server, &PrevoteArgs{}, &reply)
			if ok {
				Log(dVote, "node {%v} recv pong from server {%v}", rf.me, server)
				respCh <- reply
			}
		}(i)
	}
	nVotes := 1
	voteEndCh := make(chan bool, 1)
	go func() {
		wg.Wait()
		//vote stage finished
		voteEndCh <- false
	}()

	go func() {
		for reply := range respCh {
			if reply.Succ {
				nVotes++
				if rf.isMajority(nVotes) {
					voteEndCh <- true
				}
			}
		}
	}()

	rpcTimeout := 100 * time.Millisecond
	succ := false
	select {
	case <-time.After(rpcTimeout):
	case succ = <-voteEndCh:
	}
	return succ || rf.isMajority(nVotes)
}

func (rf *Raft) elect(eleCh *chan bool) {
	rf.lock("election")
	defer rf.unlock("election")
	var succ bool
	succ = rf.preElec()
	if !succ {
		return
	}
	//block until election stage finishes
	succ = rf.doElec()
	if succ {
		rf.ascend(eleCh)
	} else {
		// rf.stepDown(rf.currentTerm)
		rf.resetElecTimeout()
	}
}

func (rf *Raft) preElec() bool {
	if rf.killed() || rf.role == Leader {
		return false
	}
	//TODO:prevote to qualify the node
	return rf.paraReqPreVote()
}

func (rf *Raft) doElec() bool {
	rf.role = Candidate
	//vote for self
	rf.currentTerm++
	rf.resetElecTimeout()
	rf.votedFor = rf.me

	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
		//TODO:modify in continuing labs
		LastLogIdx:  0,
		LastLogTerm: 0,
	}
	return rf.paraReqVote(args, rf.proccReqVote)
}

func (rf *Raft) proccReqVote(respCh *chan RequestVoteReply, wg *sync.WaitGroup) bool {
	//vote from self by default
	nVotes := 1
	//contains false if the election is certainly failed,or true if check is required
	voteEndCh := make(chan bool, 1)
	go func() {
		wg.Wait()
		//vote stage finished
		voteEndCh <- true
	}()

	go func() {
		for reply := range *respCh {
			if reply.VoteGranted {
				nVotes++
			} else if reply.Term > rf.currentTerm {
				rf.stepDown(reply.Term)
				//fast fail
				voteEndCh <- false
				return
			}
		}
	}()

	succ := false
	rpcTimeout := 130 * time.Millisecond
	select {
	case <-time.After(rpcTimeout):
		succ = true
	case succ = <-voteEndCh:
	}
	return succ && rf.isMajority(nVotes)
}

//request for vote parallelly
func (rf *Raft) paraReqVote(args RequestVoteArgs,
	callback func(respCh *chan RequestVoteReply, wg *sync.WaitGroup) bool) bool {

	respCh := make(chan RequestVoteReply, len(rf.peers))
	wg := sync.WaitGroup{}
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(server int) {
			defer wg.Done()
			reply := RequestVoteReply{
				Term:        -1,
				VoteGranted: false,
			}
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				Log(dVote, "node {%v} recv vote from server {%v},vote: {%v},my elec tmo:{%v}",
					rf.me,
					server,
					reply.VoteGranted,
					rf.electionTimeout)

				respCh <- reply
			}
		}(i)
	}
	return callback(&respCh, &wg)
}

//to become leader
func (rf *Raft) ascend(eleCh *chan bool) {
	if rf.role != Candidate {
		return
	}
	rf.role = Leader
	*eleCh <- true
	Log(dInfo, "node {%v} become leader,term: {%v}", rf.me, rf.currentTerm)
}

//------------------------------------------------------------------------------------------------

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		//time.After() might be more suitable than time.Sleep()
		rf.nonLeaderLoop()

		//rf.role is Leader when arriving here
		rf.leaderLoop()

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
	rf.muCh = make(chan bool, 1)
	// Your initialization code here (2A, 2B, 2C).
	rf.commitIdx = 0
	rf.currentTerm = 0
	rf.electionTimeout = rf.newElecTimeout()
	rf.timeoutResetCh = make(chan int, 1)
	rf.closeCh = make(chan int, 1)
	rf.heartBeatTimeout = 90 * time.Millisecond
	rf.lastApplied = 0
	rf.log = []LogEntry{}
	rf.matchIdx = []int{}
	rf.nextIdx = []int{}
	rf.role = Follower
	rf.votedFor = -1
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	Log(dInfo, "init")
	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
