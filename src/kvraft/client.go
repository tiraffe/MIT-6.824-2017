package raftkv

import (
	"time"
	"labrpc"
	"crypto/rand"
	"math/big"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	lastLeaderId int
	clientId int64
	opIndex int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.lastLeaderId = 0
	ck.clientId = nrand()
	ck.opIndex = 0

	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	DPrintf("Get(%s): Strat...", key)
	ck.opIndex ++
	
	var result string
	leaderId := ck.lastLeaderId
	for {
		args := GetArgs{key, ck.clientId, ck.opIndex}
		reply := GetReply{}
		ok := ck.servers[leaderId].Call("RaftKV.Get", &args, &reply)
		if ok && !reply.WrongLeader {
			if reply.Err == OK {
				result = reply.Value
				break
			} else if reply.Err == ErrNoKey {
				result = ""
				break
			}
		}
		if reply.Err != TimeOut {
			leaderId = (leaderId + 1) % len(ck.servers)
		}
		time.Sleep(10 * time.Millisecond)
	}
	ck.lastLeaderId = leaderId

	DPrintf("Get(%s): End[%v]...", key, result)
	return result
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	DPrintf("PutAppend(%s, %s, %s): Strat...", key, value, op)
	ck.opIndex ++

	leaderId := ck.lastLeaderId
	for {
		args := PutAppendArgs{key, value, op, ck.clientId, ck.opIndex}
		reply := PutAppendReply{}
		ok := ck.servers[leaderId].Call("RaftKV.PutAppend", &args, &reply)
		if ok && !reply.WrongLeader && reply.Err == OK {
			break
		}
		if reply.Err != TimeOut {
			leaderId = (leaderId + 1) % len(ck.servers)
		}
		time.Sleep(10 * time.Millisecond)
	}
	ck.lastLeaderId = leaderId

	DPrintf("PutAppend(%s, %s, %s): End...", key, value, op)
	return
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
