package raftkv

import (
	"time"
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

import "bytes"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Key			string
	Value		string	
	ClientId	int64
	OpType		string
	OpIndex		int64
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	data 	map[string]string
	callbackCh map[int64]chan Result // [clientId] result
	lastCommitedOpIndex map[int64]int64 

	lastCommitedLogIndex int
}


func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	DPrintf("# [%d]: Get(%v, %v): start...", kv.me, args, reply)
	_, isLeader := kv.rf.GetState()

	if isLeader {
		reply.WrongLeader = false
		kv.mu.Lock()
		if _, ok := kv.callbackCh[args.ClientId]; !ok {
			kv.callbackCh[args.ClientId] = make(chan Result)
		}
		kv.mu.Unlock()

		kv.rf.Start(Op{args.Key, "", args.ClientId, "Get", args.OpIndex})

		select {
		case res := <- kv.callbackCh[args.ClientId]:
			if res.OpIndex == args.OpIndex {
				if res.Value != "" {
					reply.Err = OK
					reply.Value = res.Value
				} else {
					reply.Err = ErrNoKey
				}
			}
		case <- time.After(2 * time.Second):
			reply.Err = TimeOut
		}
	} else {
		reply.WrongLeader = true
	}

	DPrintf("# [%d]: Get(%v, %v): End...", kv.me, args, reply)
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	DPrintf("# [%d]: PutAppend(%v, %v): start...", kv.me, args, reply)
	_, isLeader := kv.rf.GetState()

	if isLeader {
		reply.WrongLeader = false
		kv.mu.Lock()
		if _, ok := kv.callbackCh[args.ClientId]; !ok {
			kv.callbackCh[args.ClientId] = make(chan Result)
		}
		kv.mu.Unlock()
		
		kv.rf.Start(Op{args.Key, args.Value, args.ClientId, args.Op, args.OpIndex})
		
		select {
		case res := <- kv.callbackCh[args.ClientId]:
			if res.OpIndex >= args.OpIndex {
				reply.Err = OK
			}
		case <- time.After(2 * time.Second):
			reply.Err = TimeOut
		}
	} else {
		reply.WrongLeader = true
	}

	DPrintf("# [%d]: PutAppend(%v, %v): end...", kv.me, args, reply)
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.data = make(map[string]string)
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 100)
	kv.callbackCh = make(map[int64]chan Result)
	kv.lastCommitedOpIndex = make(map[int64]int64)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.loopForApplyMsg()

	return kv
}

type Result struct {
	OpIndex int64
	Value string
}

func (kv *RaftKV) ApplyCommand(command Op) Result {
	result := Result{command.OpIndex, ""}

	if kv.lastCommitedOpIndex[command.ClientId] < command.OpIndex {
		switch command.OpType {
		case "Put":
			kv.data[command.Key] = command.Value
		case "Append":
			kv.data[command.Key] += command.Value
		}
		DPrintf("# [%d]: Applied %v.", kv.me, command)
		kv.lastCommitedOpIndex[command.ClientId] = command.OpIndex
	}
	if val, ok := kv.data[command.Key]; ok {
		result.Value = val
	}
	return result
}

func (kv *RaftKV) loopForApplyMsg() {
	for {
		msg := <- kv.applyCh
		if msg.UseSnapshot {
			data := msg.Snapshot
			kv.deserializeSnapshot(data)
			kv.lastCommitedLogIndex = msg.Index
		} else {
			command := msg.Command.(Op)
			res := kv.ApplyCommand(command)
			if msg.Index > kv.lastCommitedLogIndex {
				kv.lastCommitedLogIndex = msg.Index
			}

			select {
			case kv.callbackCh[command.ClientId] <- res:
				DPrintf("# [%d]: Callback succeeded.", kv.me)
			default:
				DPrintf("# [%d]: Callback failed.", kv.me)
			}

			if kv.maxraftstate != -1 && kv.rf.GetStateSize() >= kv.maxraftstate {
				DPrintf("# [%d]: Start SaveSnapshot.", kv.me)
				data := kv.serializeSnapshot()
				go kv.rf.UpdateSnapshot(data, kv.lastCommitedLogIndex)
			}
		}
	}
}

func (kv *RaftKV) serializeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.lastCommitedOpIndex)
	return w.Bytes()
}

func (kv *RaftKV) deserializeSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&kv.data)
	d.Decode(&kv.lastCommitedOpIndex)
}
