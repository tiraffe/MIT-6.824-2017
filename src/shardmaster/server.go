package shardmaster

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sort"
	"sync"
	"time"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	callbackCh           map[int64]chan Result // [clientId] result
	lastCommitedOpIndex  map[int64]int64
	lastCommitedLogIndex int

	configs []Config // indexed by config num
}

type MovePair struct {
	Shard int
	GID   int
}

type Op struct {
	Servers map[int][]string // Join
	GIDs    []int            // Leave
	Shard   int              // Move
	GID     int              // Move
	Num     int              // Query

	ClientId int64
	OpType   string
	OpIndex  int64
}

type Result struct {
	OpIndex int64
	Value   Config
}

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func (sm *ShardMaster) createCallbackChan(clientId int64) {
	sm.mu.Lock()
	if _, ok := sm.callbackCh[clientId]; !ok {
		sm.callbackCh[clientId] = make(chan Result)
	}
	sm.mu.Unlock()
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	DPrintf("# [%d]: Join(%v, %v): start...", sm.me, args, reply)
	_, isLeader := sm.rf.GetState()
	if isLeader {
		reply.WrongLeader = false
		sm.createCallbackChan(args.ClientId)

		sm.rf.Start(Op{
			Servers:  args.Servers,
			ClientId: args.ClientId,
			OpType:   "Join",
			OpIndex:  args.OpIndex,
		})

		select {
		case res := <-sm.callbackCh[args.ClientId]:
			if res.OpIndex == args.OpIndex {
				reply.Err = OK
			}
		case <-time.After(2 * time.Second):
			reply.Err = TimeOut
		}
	} else {
		reply.WrongLeader = true
	}

	DPrintf("# [%d]: Join(%v, %v): End...", sm.me, args, reply)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	DPrintf("# [%d]: Leave(%v, %v): start...", sm.me, args, reply)
	_, isLeader := sm.rf.GetState()
	if isLeader {
		reply.WrongLeader = false
		sm.createCallbackChan(args.ClientId)

		sm.rf.Start(Op{
			GIDs:     args.GIDs,
			ClientId: args.ClientId,
			OpType:   "Leave",
			OpIndex:  args.OpIndex,
		})

		select {
		case res := <-sm.callbackCh[args.ClientId]:
			if res.OpIndex == args.OpIndex {
				reply.Err = OK
			}
		case <-time.After(2 * time.Second):
			reply.Err = TimeOut
		}
	} else {
		reply.WrongLeader = true
	}

	DPrintf("# [%d]: Leave(%v, %v): End...", sm.me, args, reply)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	DPrintf("# [%d]: Move(%v, %v): start...", sm.me, args, reply)
	_, isLeader := sm.rf.GetState()
	if isLeader {
		reply.WrongLeader = false
		sm.createCallbackChan(args.ClientId)

		sm.rf.Start(Op{
			Shard:    args.Shard,
			GID:      args.GID,
			ClientId: args.ClientId,
			OpType:   "Move",
			OpIndex:  args.OpIndex,
		})

		select {
		case res := <-sm.callbackCh[args.ClientId]:
			if res.OpIndex == args.OpIndex {
				reply.Err = OK
			}
		case <-time.After(2 * time.Second):
			reply.Err = TimeOut
		}
	} else {
		reply.WrongLeader = true
	}

	DPrintf("# [%d]: Move(%v, %v): End...", sm.me, args, reply)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	DPrintf("# [%d]: Query(%v, %v): start...", sm.me, args, reply)
	_, isLeader := sm.rf.GetState()
	if isLeader {
		reply.WrongLeader = false
		sm.createCallbackChan(args.ClientId)

		sm.rf.Start(Op{
			Num:      args.Num,
			ClientId: args.ClientId,
			OpType:   "Query",
			OpIndex:  args.OpIndex,
		})

		select {
		case res := <-sm.callbackCh[args.ClientId]:
			if res.OpIndex == args.OpIndex {
				reply.Err = OK
				reply.Config = res.Value
			}
		case <-time.After(2 * time.Second):
			reply.Err = TimeOut
		}
	} else {
		reply.WrongLeader = true
	}

	DPrintf("# [%d]: Query(%v, %v): End...", sm.me, args, reply)
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	DPrintf("# [%d]: Killed...", sm.me)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) ApplyCommand(command Op) Result {
	result := Result{OpIndex: command.OpIndex}

	if sm.lastCommitedOpIndex[command.ClientId] < command.OpIndex {
		switch command.OpType {
		case "Join":
			var Servers map[int][]string = command.Servers
			config := sm.getConfig(-1)
			for GID, servers := range Servers {
				config.Groups[GID] = servers
			}
			sm.updateConfig(config)
		case "Leave":
			var GIDs []int = command.GIDs
			config := sm.getConfig(-1)
			for _, GID := range GIDs {
				delete(config.Groups, GID)
			}
			sm.updateConfig(config)
		case "Move":
			config := sm.getConfig(-1)
			config.Shards[command.Shard] = command.GID
			config.Num += 1
			sm.configs = append(sm.configs, config)
		case "Query":
			result.Value = sm.getConfig(command.Num)
			DPrintf("------------ Query %d: %v", command.Num, result.Value)
		}
		DPrintf("# [%d]: Applied %v. = %v", sm.me, command, sm.configs)
		sm.lastCommitedOpIndex[command.ClientId] = command.OpIndex
	}

	return result
}

func (sm *ShardMaster) getConfig(index int) Config {
	var oldConfig *Config
	if index < 0 || index >= len(sm.configs) {
		oldConfig = &sm.configs[len(sm.configs)-1]
	} else {
		oldConfig = &sm.configs[index]
	}
	newConfig := *oldConfig
	newConfig.Groups = make(map[int][]string)
	for k, v := range oldConfig.Groups {
		newConfig.Groups[k] = v
	}
	return newConfig
}

func (sm *ShardMaster) loopForApplyMsg() {
	for {
		msg := <-sm.applyCh

		if msg.UseSnapshot {
			data := msg.Snapshot
			sm.deserializeSnapshot(data)
			sm.lastCommitedLogIndex = msg.Index
		} else {
			command := msg.Command.(Op)
			res := sm.ApplyCommand(command)
			if msg.Index > sm.lastCommitedLogIndex {
				sm.lastCommitedLogIndex = msg.Index
			}

			select {
			case sm.callbackCh[command.ClientId] <- res:
				DPrintf("# [%d]: Callback succeeded.", sm.me)
			default:
				DPrintf("# [%d]: Callback failed.", sm.me)
			}

			if sm.maxraftstate != -1 && sm.rf.GetStateSize() >= sm.maxraftstate {
				DPrintf("# [%d]: Start SaveSnapshot.", sm.me)
				data := sm.serializeSnapshot()
				go sm.rf.UpdateSnapshot(data, sm.lastCommitedLogIndex)
			}
		}
	}
}

func (sm *ShardMaster) serializeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(sm.configs)
	e.Encode(sm.lastCommitedOpIndex)
	return w.Bytes()
}

func (sm *ShardMaster) deserializeSnapshot(data []byte) {
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&sm.configs)
	d.Decode(&sm.lastCommitedOpIndex)
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	gob.Register(Op{})
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	sm.maxraftstate = -1
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.callbackCh = make(map[int64]chan Result)
	sm.lastCommitedOpIndex = make(map[int64]int64)

	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	go sm.loopForApplyMsg()
	DPrintf("# [%d]: Server started.", sm.me)
	return sm
}

type ShardCountPair struct {
	GID        int
	ShardCount int
}

type ShardCountPairList []ShardCountPair

func (p ShardCountPairList) Len() int           { return len(p) }
func (p ShardCountPairList) Less(i, j int) bool { return p[i].ShardCount < p[j].ShardCount }
func (p ShardCountPairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (sm *ShardMaster) updateConfig(config Config) {
	groupNum := len(config.Groups)
	movingShard := make(chan int, NShards)

	// group by GID and add the shards which in a deleted GID to moving list.
	shardsList := make(map[int][]int)
	for shard := 0; shard < NShards; shard++ {
		GID := config.Shards[shard]
		if _, ok := config.Groups[GID]; ok {
			shardsList[GID] = append(shardsList[GID], shard)
		} else {
			movingShard <- shard
		}
	}
	// sort by each GID's shard count desc
	var kvs ShardCountPairList
	for GID, _ := range config.Groups {
		length := 0
		if _, ok := shardsList[GID]; ok {
			length = len(shardsList[GID])
		}
		kvs = append(kvs, ShardCountPair{GID, length})
	}
	sort.Sort(sort.Reverse(kvs))

	for i := 0; i < len(kvs); i++ {
		expected := NShards / groupNum
		if i < NShards%groupNum {
			expected++
		}
		// re-balance shard
		for k := 0; kvs[i].ShardCount > expected; k++ {
			kvs[i].ShardCount--
			movingShard <- shardsList[kvs[i].GID][k]
		}
		for kvs[i].ShardCount < expected {
			kvs[i].ShardCount++
			thisShard := <-movingShard
			config.Shards[thisShard] = kvs[i].GID
		}
	}
	config.Num += 1
	sm.configs = append(sm.configs, config)
}
