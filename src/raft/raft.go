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
	log	[]string

	receivedHeartBeat bool
	state ServerState
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
	// DPrintf("[%d] -- Peer[%d] GetState(): isLeader[%v].\n", rf.currentTerm, rf.me, isleader)
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
	Entries []string
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
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else {
		rf.receivedHeartBeat = true
		// TODO: 2B here
		reply.Success = true
		reply.Term = rf.currentTerm
	}
	rf.mu.Unlock()
	rf.updateCurrentTerm(args.Term)
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
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.votedFor = args.CandidateId
		}
		if rf.state == Follower {
			rf.receivedHeartBeat = true
		}
	}
	rf.mu.Unlock()
	rf.updateCurrentTerm(args.Term)
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


	return index, term, isLeader
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
	rf.currentTerm = -1
	// Your initialization code here (2A, 2B, 2C).

	// TODO: create a background goroutine that will kick off 
	// leader election periodically by sending out RequestVote 
	// RPCs when it hasn't heard from another peer for a while.
	go rf.checkElectionTimeout()
	go rf.loopForHeartBeat()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}

func (rf *Raft) broadcastHeartBeat(args *AppendEntriesArgs) {
	for idx, _ := range rf.peers {
		if idx != rf.me {
			go func(server int) {
				reply := AppendEntriesReply{-1, false}
				rf.sendAppendEntries(server, args, &reply)
			}(idx)
		}
	}
}

func (rf *Raft) loopForHeartBeat() {
	for {
		rf.mu.Lock()
		if rf.state == Leader {
			args := AppendEntriesArgs{rf.currentTerm, rf.me, -1, -1, []string{}, -1}
			go rf.broadcastHeartBeat(&args)
		}
		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond) // send heartbeat per 100 ms
	}
}

func (rf *Raft) handleElection(term int) {
	DPrintf("[%d] -- Peer[%d] handleElection(): begin...\n", term, rf.me)
	// send request vote to everyone
	votedChan := make(chan bool)
	for idx, _ := range rf.peers {
		if idx != rf.me {
			go func(server int) {
				reply := RequestVoteReply{-1, false}
				ok := rf.sendRequestVote(server, &RequestVoteArgs{term, rf.me, -1, -1}, &reply)
				rf.updateCurrentTerm(reply.Term)
				votedChan <- ok && reply.VoteGranted 
				DPrintf("[%d] -- Peer[%d] handleElection(): vote from [%d] - %v\n", term, rf.me, server, reply.VoteGranted)
			}(idx)
		}
	}

	DPrintf("[%d] -- Peer[%d] handleElection(): waiting votes .\n", term, rf.me)
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

	DPrintf("[%d] -- Peer[%d] handleElection(): received votes [%d/%d].\n", term, rf.me, receivedVotes, total)

	if receivedVotes >= (total + 1) / 2 {
		rf.mu.Lock()
		if rf.state == Candidate && term >= rf.currentTerm {
			rf.state = Leader
			DPrintf("[%d] -- Peer[%d] changed to Leader.\n", rf.currentTerm, rf.me)
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
			DPrintf("[%d] -- Peer[%d] changed to Candidate.\n", rf.currentTerm, rf.me)
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
		rf.receivedHeartBeat = true
		DPrintf("[%d] -- Peer[%d] changed to Follower.\n", rf.currentTerm, rf.me)
	}
	rf.mu.Unlock()
}