package raftkv

import (
	"time"
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 1

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
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	data 	map[string]string
	callbackCh map[int64]chan int64 // [clientId] result
	lastCommitedOpIndex map[int64]int64 
	// Your definitions here.
}


func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, isLeader := kv.rf.GetState()
	reply.Err = OK
	if isLeader {
		reply.WrongLeader = false
		if _, ok := kv.callbackCh[args.ClientId]; !ok {
			kv.callbackCh[args.ClientId] = make(chan int64)
		}

		kv.rf.Start(Op{args.Key, "", args.ClientId, "Get", args.OpIndex})

		select {
		case index := <- kv.callbackCh[args.ClientId]:
			if index >= args.OpIndex {
				if val, ok := kv.data[args.Key]; ok {
					reply.Err = OK
					reply.Value = val
				} else {
					reply.Err = ErrNoKey
				}
			} else {
				reply.Err = DirtyRead
			}
		case <- time.After(5 * time.Second):
			reply.Err = TimeOut
		}
		delete(kv.callbackCh, args.ClientId)
	} else {
		reply.WrongLeader = true
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("# [%d]: PutAppend(%v, %v): start...", kv.me, args, reply)
	_, isLeader := kv.rf.GetState()
	if isLeader {
		reply.WrongLeader = false
		if _, ok := kv.callbackCh[args.ClientId]; !ok {
			kv.callbackCh[args.ClientId] = make(chan int64)
		}
		
		kv.rf.Start(Op{args.Key, args.Value, args.ClientId, args.Op, args.OpIndex})
		
		select {
		case index := <- kv.callbackCh[args.ClientId]:
			if index >= args.OpIndex {
				reply.Err = OK
			} else {
				reply.Err = DirtyRead
			}
		case <- time.After(5 * time.Second):
			reply.Err = TimeOut
		}
	} else {
		reply.Err = OK
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

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.callbackCh = make(map[int64]chan int64)
	kv.lastCommitedOpIndex = make(map[int64]int64)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// for _, entity := range *(kv.rf).log {
	// 	kv.ApplyCommand(entity.Command.(Op))
	// }
	go kv.LoopForApplyMsg()

	return kv
}

func (kv *RaftKV) ApplyCommand(command Op) {
	if kv.lastCommitedOpIndex[command.ClientId] < command.OpIndex {
		switch command.OpType {
		case "Get":
			// pass.
		case "Put":
			kv.data[command.Key] = command.Value
		case "Append":
			kv.data[command.Key] += command.Value
		}
		
		kv.lastCommitedOpIndex[command.ClientId] = command.OpIndex
		DPrintf("# [%d]: Applied %v.", kv.me, command)
	}
}

func (kv *RaftKV) LoopForApplyMsg() {
	for {
		select {
		case msg := <- kv.applyCh:
			command := msg.Command.(Op)
			kv.ApplyCommand(command)

			select {
			case kv.callbackCh[command.ClientId] <- command.OpIndex:
				DPrintf("# [%d]: Callback success.", kv.me)
			default:
				DPrintf("# [%d]: Callback failed.", kv.me)
			}
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}