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
	"sort"
	"sync"
	"labrpc"
	"math/rand"
	"time"
)

// import "bytes"
// import "encoding/gob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type ServerState int
const (
	Follower ServerState = 0
	Candidate ServerState = 1
	Leader ServerState = 2
)

type Log struct {
	Term int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	currentTerm int
	votedFor  int
	log	[]Log

	receivedHeartBeat bool
	state ServerState

	commitIndex int
	lastApplied int

	nextIndex []int
	matchIndex []int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

const MinElectionTimeout = 1000
const MaxElectionTimeout = 1500

func randElectionTimeout() time.Duration {
	randTimeout := (MinElectionTimeout + rand.Intn(MaxElectionTimeout - MinElectionTimeout))
	return time.Duration(randTimeout) * time.Millisecond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.state == Leader)
	// DPrintf("Term[%d] -- Peer[%d] GetState(): isLeader[%v].\n", rf.currentTerm, rf.me, isleader)
	rf.mu.Unlock()
	return term, isleader
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int	// index of log entry immediately preceding new ones
	PrevLogTerm int 	// term of prevLogIndex entry
	Entries []Log
	LeaderCommit int
	// Your data here (2A, 2B).
}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term int
	Success bool
	// Your data here (2A).
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	
	rf.updateCurrentTerm(args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = true
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if args.LeaderCommit < rf.commitIndex {
		reply.Success = false
		return
	} 
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	if len(args.Entries) != 0 {
		DPrintf("Term[%d] -- Peer[%d] AppendEntries: log: %v, args: %v.\n", args.Term, rf.me, rf.log, args)
		conflictIndex := min(len(rf.log), len(args.Entries) + args.PrevLogIndex)
		for i := args.PrevLogIndex + 1; i < len(rf.log) && i < len(args.Entries) + args.PrevLogIndex; i++ {
			if rf.log[i].Term != args.Entries[i - args.PrevLogIndex].Term {
				conflictIndex = i
				break
			}
		}
		rf.log = append(rf.log[:conflictIndex], args.Entries[conflictIndex - (args.PrevLogIndex + 1):]...)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log) - 1)
	}
	rf.receivedHeartBeat = true
}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
	
	// Your data here (2A, 2B).
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term int
	VoteGranted bool
	
	// Your data here (2A).
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.updateCurrentTerm(args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	lastIndex := len(rf.log) - 1
	if rf.log[lastIndex].Term > args.LastLogTerm || 
		(rf.log[lastIndex].Term == args.LastLogTerm && lastIndex > args.LastLogIndex) {
		DPrintf("Term[%d] -- Peer[%d] rejected [%d] by: LastLog\n", rf.currentTerm, rf.me, args.CandidateId)
		return
	}
	if args.Term < rf.currentTerm {
		DPrintf("Term[%d] -- Peer[%d] rejected [%d] by: Term\n", rf.currentTerm, rf.me, args.CandidateId)
		return 
	}
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		DPrintf("Term[%d] -- Peer[%d] rejected [%d] by: votedFor\n", rf.currentTerm, rf.me, args.CandidateId)
		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	if rf.state == Follower {
		rf.receivedHeartBeat = true
	}
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
// may fail or lose an election.
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
	rf.mu.Lock()
	index = len(rf.log)
	term = rf.currentTerm
	isLeader = rf.state == Leader

	if isLeader {
		DPrintf("Term[%d] -- Peer[%d] new command: index[%d] - [%d]\n", term, rf.me, index, command)
		rf.log = append(rf.log, Log{term, command})
		go rf.broadcastAppendEntries()
	}
	rf.mu.Unlock()
	
	return index, term, isLeader
}

func (rf *Raft) getAppendEntriesArgs(nextIndex int) *AppendEntriesArgs {
	rf.mu.Lock()
	nextIndex = min(nextIndex, len(rf.log) - 1)
	entries := rf.log[nextIndex:]
	prevIndex := nextIndex - 1
	args := AppendEntriesArgs{rf.currentTerm, rf.me, prevIndex, 
		rf.log[prevIndex].Term, entries, rf.commitIndex}
	rf.mu.Unlock()
	return &args
}

func (rf *Raft) broadcastAppendEntries() {
	for idx, _ := range rf.peers {
		if idx != rf.me {
			go func(server int) {
				nextIndex := rf.nextIndex[server]
				for nextIndex > 0 {
					args := rf.getAppendEntriesArgs(nextIndex)
					reply := AppendEntriesReply{-1, false}
					ok := rf.sendAppendEntries(server, args, &reply)
					rf.updateCurrentTerm(reply.Term)
					if ok && reply.Success {
						rf.mu.Lock()
						rf.nextIndex[server] = nextIndex + len(args.Entries)
						rf.matchIndex[server] = rf.nextIndex[server] - 1
						rf.mu.Unlock()
						break
					}
					nextIndex --
				}
			}(idx)
		}
	}
}
//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.receivedHeartBeat = false
	rf.state = Follower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.log = []Log{Log{0, nil}}		// add a head log to make index start from 1 

	rf.lastApplied = 0
	rf.commitIndex = 0
	// Your initialization code here (2A, 2B, 2C).

	// create a background goroutine that will kick off 
	// leader election periodically by sending out RequestVote 
	// RPCs when it hasn't heard from another peer for a while.
	go rf.checkElectionTimeout()
	go rf.loopForHeartBeat()
	
	go rf.loopForApplyMsg(applyCh)
	go rf.loopForCommitIndex()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}

func (rf *Raft) broadcastHeartBeat() {
	for idx, _ := range rf.peers {
		if idx != rf.me {
			go func(server int) {
				rf.mu.Lock()
				args := AppendEntriesArgs{rf.currentTerm, rf.me, rf.commitIndex, 
					rf.log[rf.commitIndex].Term, []Log{}, rf.commitIndex}
				reply := AppendEntriesReply{-1, false}
				rf.mu.Unlock()
				rf.sendAppendEntries(server, &args, &reply)
			}(idx)
		}
	}
}

func (rf *Raft) loopForHeartBeat() {
	for {
		rf.mu.Lock()
		if rf.state == Leader {
			go rf.broadcastHeartBeat()
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond) // send heartbeat per 100 ms
	}
}

func (rf *Raft) handleElection(term int) {
	DPrintf("Term[%d] -- Peer[%d] handleElection(): begin...\n", term, rf.me)
	// send request vote to everyone
	votedChan := make(chan bool)
	for idx, _ := range rf.peers {
		if idx != rf.me {
			go func(server int) {
				reply := RequestVoteReply{-1, false}
				ok := rf.sendRequestVote(server, &RequestVoteArgs{term, rf.me, 
					len(rf.log) - 1, rf.log[len(rf.log) - 1].Term}, &reply)
				rf.updateCurrentTerm(reply.Term)
				votedChan <- ok && reply.VoteGranted 
				DPrintf("Term[%d] -- Peer[%d] handleElection(): vote from [%d] - %v\n", 
					term, rf.me, server, reply.VoteGranted)
			}(idx)
		}
	}

	DPrintf("Term[%d] -- Peer[%d] handleElection(): waiting votes .\n", term, rf.me)
	total := len(rf.peers)
	receivedVotes := 1 	// 1 vote for self
	for i := 0; i < total - 1; i++ {
		if <- votedChan {
			receivedVotes += 1
		}
		if receivedVotes >= (total + 1) / 2 {
			break
		}
	}

	DPrintf("Term[%d] -- Peer[%d] handleElection(): received votes [%d/%d].\n", 
		term, rf.me, receivedVotes, total)

	if receivedVotes >= (total + 1) / 2 {
		rf.mu.Lock()
		if rf.state == Candidate && term >= rf.currentTerm {
			rf.state = Leader
			rf.nextIndex = make([]int, len(rf.peers))
			rf.matchIndex = make([]int, len(rf.peers))
			for idx, _ := range rf.nextIndex {
				rf.nextIndex[idx] = len(rf.log)
			}
			DPrintf("Term[%d] -- Peer[%d] changed to Leader.\n", rf.currentTerm, rf.me)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) checkElectionTimeout() {
	for {
		time.Sleep(randElectionTimeout())
		rf.mu.Lock()
		if rf.state != Leader && !rf.receivedHeartBeat {
			rf.state = Candidate
			rf.currentTerm += 1
			rf.votedFor = rf.me
			DPrintf("Term[%d] -- Peer[%d] changed to Candidate.\n", rf.currentTerm, rf.me)
			go rf.handleElection(rf.currentTerm)
		}
		rf.receivedHeartBeat = false
		rf.mu.Unlock()
	}
}

func (rf *Raft) updateCurrentTerm(term int) {
	rf.mu.Lock()
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.state = Follower
		rf.votedFor = -1
		DPrintf("Term[%d] -- Peer[%d] changed to Follower.\n", rf.currentTerm, rf.me)
	}
	rf.mu.Unlock()
}

func (rf *Raft) loopForCommitIndex() {
	for {
		rf.mu.Lock()
		if rf.state == Leader {
			total := len(rf.matchIndex)
			sortedMatchIndex := make([]int, total)
			copy(sortedMatchIndex, rf.matchIndex)
			sort.Ints(sortedMatchIndex)
			// DPrintf("Term[%d] -- Peer[%d] sortMatch: %v\n", rf.currentTerm, rf.me, sortedMatchIndex)
			medianIndex := sortedMatchIndex[total / 2 + 1]

			for medianIndex > rf.commitIndex {
				if medianIndex < len(rf.log) && rf.log[medianIndex].Term == rf.currentTerm {
					rf.commitIndex = medianIndex
					DPrintf("Term[%d] -- Peer[%d] update commitIndex: %d from most agreement.\n", 
						rf.currentTerm, rf.me, rf.commitIndex)
					break
				}
				medianIndex --
			}
		}
		rf.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}

func (rf *Raft) loopForApplyMsg(applyCh chan ApplyMsg) {
	for {
		rf.mu.Lock()
		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied += 1
			msg := ApplyMsg{rf.lastApplied, rf.log[rf.lastApplied].Command, false, []byte{}}
			DPrintf("Term[%d] -- Peer[%d] sending applyMsg: %v.\n", rf.currentTerm, rf.me, msg)
			go func(msg ApplyMsg) {
				applyCh <- msg
			}(msg)
		}
		rf.mu.Unlock()

		time.Sleep(10 * time.Millisecond)
	}
}
