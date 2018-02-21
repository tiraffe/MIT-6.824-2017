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

import "bytes"
import "encoding/gob"



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
	logOffset int	// index offset for log compaction

	receivedHeartBeat bool
	state ServerState

	commitIndex int
	lastApplied int

	nextIndex []int
	matchIndex []int

	applyCh chan ApplyMsg 
	snapshotData []byte
}

const MinElectionTimeout = 1000
const MaxElectionTimeout = 1500

func randElectionTimeout() time.Duration {
	randTimeout := (MinElectionTimeout + rand.Intn(MaxElectionTimeout - MinElectionTimeout))
	return time.Duration(randTimeout) * time.Millisecond
}

func (rf *Raft) getLog(index int) Log {
	return rf.log[index - rf.logOffset]
}

func (rf *Raft) getLogsByRange(stratIndex int, endIndex int) []Log {
	s := max(stratIndex - rf.logOffset, 0)
	e := min(endIndex - rf.logOffset, len(rf.log))

	return rf.log[s: e]
}

func (rf *Raft) getLogLength() int {
	return rf.logOffset + len(rf.log)
}

func (rf *Raft) GetStateSize() int {
	return rf.persister.RaftStateSize()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	term := rf.currentTerm
	isleader := (rf.state == Leader)
	rf.mu.Unlock()

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
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
}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term int
	ConflictIndex int
	Success bool
}


func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.updateCurrentTerm(args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Term[%d] -- Peer[%d] AppendEntries: Start... FROM [%d, %d]\n", rf.currentTerm, rf.me, args.LeaderId, args.Term)

	reply.Success = true
	reply.Term = rf.currentTerm
	reply.ConflictIndex = args.PrevLogIndex

	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	if args.PrevLogIndex < rf.logOffset || args.PrevLogIndex >= rf.getLogLength() || rf.getLog(args.PrevLogIndex).Term != args.PrevLogTerm {
		if args.PrevLogIndex < rf.logOffset || args.PrevLogIndex >= rf.getLogLength() {
			reply.ConflictIndex = rf.getLogLength()
		} else {
			index := args.PrevLogIndex
			targetTerm := rf.getLog(args.PrevLogIndex).Term
			for index > rf.logOffset && rf.getLog(index - 1).Term == targetTerm {
				index --
			}
			reply.ConflictIndex = index
		}
		reply.Success = false
		return
	}

	conflictIndex := min(rf.getLogLength(), len(args.Entries) + args.PrevLogIndex + 1)
	for i := args.PrevLogIndex + 1; i < rf.getLogLength() && i < len(args.Entries) + args.PrevLogIndex + 1; i++ {
		if rf.getLog(i).Term != args.Entries[i - args.PrevLogIndex - 1].Term {
			conflictIndex = i
			break
		}
	}
	// if current log contains all entries, don't truncate the log.
	if conflictIndex != len(args.Entries) + args.PrevLogIndex + 1 {
		rf.log = append(rf.getLogsByRange(0, conflictIndex), args.Entries[conflictIndex - (args.PrevLogIndex + 1):]...)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLogLength() - 1)
	}
	rf.receivedHeartBeat = true
	rf.persist()
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
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.updateCurrentTerm(args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Term[%d] -- Peer[%d] RequestVote: Start...\n", rf.currentTerm, rf.me)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	lastIndex := rf.getLogLength() - 1
	if rf.getLog(lastIndex).Term > args.LastLogTerm || 
		(rf.getLog(lastIndex).Term == args.LastLogTerm && lastIndex > args.LastLogIndex) {
		return
	}
	if args.Term < rf.currentTerm {
		return 
	}
	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		return
	}

	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	if rf.state == Follower {
		rf.receivedHeartBeat = true
	}
	rf.persist()
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

type InstallSnapshotArgs struct {
	Term int
	LastIncludedIndex int
	LastIncludedTerm int
	Data []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.updateCurrentTerm(args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	
	if rf.getLogLength() > args.LastIncludedIndex && rf.logOffset <= args.LastIncludedIndex && 
			rf.getLog(args.LastIncludedIndex).Term == args.LastIncludedTerm {
		rf.log = rf.getLogsByRange(args.LastIncludedIndex, rf.getLogLength())
	} else {
		rf.log = []Log{Log{args.LastIncludedTerm, nil}}
	}
	rf.logOffset = args.LastIncludedIndex
	rf.receivedHeartBeat = true

	msg := ApplyMsg{Index: args.LastIncludedIndex, UseSnapshot: true, Snapshot: args.Data}

	rf.applyCh <- msg
	rf.lastApplied = args.LastIncludedIndex
	rf.commitIndex = args.LastIncludedIndex
	rf.snapshotData = args.Data
	rf.persist()
	rf.saveSnapshot()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
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

	rf.mu.Lock()
	index = rf.getLogLength()
	term = rf.currentTerm
	isLeader = rf.state == Leader

	if isLeader {
		DPrintf("Term[%d] -- Peer[%d] new command: index[%d] - [%d]\n", term, rf.me, index, command)
		rf.log = append(rf.log, Log{term, command})
		rf.persist()
	}
	rf.mu.Unlock()
	
	return index, term, isLeader
}

func (rf *Raft) getAppendEntriesArgs(nextIndex int) (*AppendEntriesArgs, bool) {
	// Entries can be [], which represents HeartBeat.
	entries := rf.getLogsByRange(nextIndex, rf.getLogLength())
	prevIndex := nextIndex - 1
	args := &AppendEntriesArgs{rf.currentTerm, rf.me, prevIndex, 
		rf.getLog(prevIndex).Term, entries, rf.commitIndex} 
	isLeader := rf.state == Leader

	return args, isLeader
}

func (rf *Raft) getInstallSnapshotArgs() (*InstallSnapshotArgs, bool) {
	args := &InstallSnapshotArgs{rf.currentTerm, rf.logOffset, rf.log[0].Term, rf.snapshotData}
	isLeader := rf.state == Leader

	return args, isLeader
}

func (rf *Raft) broadcastAppendEntries() {
	for idx, _ := range rf.peers {
		if idx != rf.me {
			go func(server int) {
				nextIndex := rf.nextIndex[server]
				for nextIndex > 0 {
					rf.mu.Lock()
					if nextIndex > rf.logOffset {
						args, isLeader := rf.getAppendEntriesArgs(nextIndex)
						rf.mu.Unlock()
						if !isLeader { // Important! State may change during this process.
							return
						}
						reply := &AppendEntriesReply{-1, -1, false}
						ok := rf.sendAppendEntries(server, args, reply)
						rf.updateCurrentTerm(reply.Term)
						if ok && reply.Success {
							rf.mu.Lock()
							rf.nextIndex[server] = nextIndex + len(args.Entries)
							rf.matchIndex[server] = rf.nextIndex[server] - 1
							rf.mu.Unlock()
							break
						} else if !reply.Success {
							nextIndex = reply.ConflictIndex
						}
					} else {
						args, isLeader := rf.getInstallSnapshotArgs()
						rf.mu.Unlock()
						if !isLeader {
							return 
						}
						reply := &InstallSnapshotReply{-1}
						ok := rf.sendInstallSnapshot(server, args, reply)
						rf.updateCurrentTerm(reply.Term)
						if ok {
							rf.mu.Lock()
							rf.nextIndex[server] = rf.logOffset + 1
							rf.matchIndex[server] = rf.nextIndex[server] - 1
							rf.mu.Unlock()
							break
						}
					}
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
	DPrintf("Term[%d] -- Peer[%d] ---------------- KILL ------------------\n", rf.currentTerm, rf.me)
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
	rf.logOffset = 0
	rf.log = []Log{Log{0, nil}}		// add a head log to make index start from 1 

	rf.lastApplied = 0
	rf.commitIndex = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.readSnapshot(persister.ReadSnapshot())

	// create a background goroutine that will kick off 
	// leader election periodically by sending out RequestVote 
	// RPCs when it hasn't heard from another peer for a while.
	go rf.checkElectionTimeout()

	go rf.loopForAppendEntries()
	go rf.loopForApplyMsg()
	
	DPrintf("Term[%d] -- Peer[%d] ---------------- START ------------------\n", rf.currentTerm, rf.me)
	return rf
}

func (rf *Raft) loopForAppendEntries() {
	for {
		if rf.state == Leader {
			rf.broadcastAppendEntries()
		}
		time.Sleep(50 * time.Millisecond) // send appendEntries per 50 ms
	}
}

func (rf *Raft) handleElection(term int) {
	DPrintf("Term[%d] -- Peer[%d] handleElection(): begin...\n", term, rf.me)
	
	votedChan := make(chan bool)
	args := RequestVoteArgs{term, rf.me, rf.getLogLength() - 1, rf.getLog(rf.getLogLength() - 1).Term}
	for idx, _ := range rf.peers { // send vote request to everyone
		if idx != rf.me {
			go func(server int) {
				reply := RequestVoteReply{-1, false}
				ok := rf.sendRequestVote(server, &args, &reply)
				rf.updateCurrentTerm(reply.Term)
				select {
				case votedChan <- ok && reply.VoteGranted:
					DPrintf("Term[%d] -- Peer[%d] handleElection(): vote from [%d] - %v\n", 
						term, rf.me, server, reply.VoteGranted)
				case <- time.After(100 * time.Millisecond):
					// election already finished.
				}
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
				rf.nextIndex[idx] = rf.getLogLength()
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
			rf.persist()
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
		rf.persist()
		DPrintf("Term[%d] -- Peer[%d] changed to Follower.\n", rf.currentTerm, rf.me)
	}
	rf.mu.Unlock()
}

func (rf *Raft) updateCommitIndex() {
	rf.mu.Lock()
	if rf.state == Leader {
		total := len(rf.matchIndex)
		sortedMatchIndex := make([]int, total)
		copy(sortedMatchIndex, rf.matchIndex)
		sort.Ints(sortedMatchIndex)
		medianIndex := sortedMatchIndex[(total + 1) / 2]

		for medianIndex > rf.commitIndex {
			if medianIndex < rf.getLogLength() && rf.getLog(medianIndex).Term == rf.currentTerm {
				rf.commitIndex = medianIndex
				DPrintf("Term[%d] -- Peer[%d] update commitIndex: %d from most agreement - %v.\n", 
					rf.currentTerm, rf.me, rf.commitIndex, rf.matchIndex)
				break
			}
			medianIndex --
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) loopForApplyMsg() {
	for {
		rf.mu.Lock()
		for rf.lastApplied < rf.commitIndex {
			msg := ApplyMsg{rf.lastApplied + 1, rf.getLog(rf.lastApplied + 1).Command, false, []byte{}} 
			DPrintf("Term[%d] -- Peer[%d] sending applyMsg: %v.  -- %v\n", rf.currentTerm, rf.me, msg, rf.log)
			rf.lastApplied ++
			rf.applyCh <- msg
		}
		rf.mu.Unlock()
		rf.updateCommitIndex()
		time.Sleep(10 * time.Millisecond)
	}
}

type Snapshot struct {
	Data []byte
	LastIncludedIndex int
	LastIncludedTerm int
}

func (rf *Raft) UpdateSnapshot(data []byte, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index < rf.logOffset || index >= rf.getLogLength() {
		return
	}
	rf.snapshotData = data
	rf.log = rf.getLogsByRange(index, rf.getLogLength())
	rf.logOffset = index
	rf.lastApplied = index
	DPrintf("Term[%d] -- Peer[%d] changed logOffset to %d", rf.currentTerm, rf.me, rf.logOffset)
	rf.saveSnapshot()
	rf.persist()
}

func (rf *Raft) saveSnapshot() {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(Snapshot{rf.snapshotData, rf.logOffset, rf.log[0].Term})
	rf.persister.SaveSnapshot(w.Bytes())
}

func (rf *Raft) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	snapshot := Snapshot{[]byte{}, 0, 0}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&snapshot)

	rf.logOffset = snapshot.LastIncludedIndex
	rf.lastApplied = snapshot.LastIncludedIndex
	rf.commitIndex = snapshot.LastIncludedIndex
	rf.snapshotData = snapshot.Data

	msg := ApplyMsg{Index: snapshot.LastIncludedIndex, UseSnapshot: true, Snapshot: snapshot.Data}
	go func() {
		rf.applyCh <- msg
	}()	
}
