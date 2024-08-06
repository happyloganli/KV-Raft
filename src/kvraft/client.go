package kvraft

import (
	"6.5840/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers        []*labrpc.ClientEnd
	leader         int
	clientId       int64
	sequenceNumber int
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
	ck.leader = 0
	ck.clientId = nrand()
	ck.sequenceNumber = 0
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.sequenceNumber++
	args := GetArgs{Key: key, ClientId: ck.clientId, RequestId: ck.sequenceNumber}

	for {
		for i := 0; i < len(ck.servers); i++ {
			server := (ck.leader + i) % len(ck.servers)
			var reply GetReply
			ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
			if ok && reply.Err == OK {
				ck.leader = server
				return reply.Value
			} else if ok && (reply.Err == ErrNoKey || reply.Err == ErrWrongLeader) {
				ck.leader = (server + 1) % len(ck.servers)
			}
		}
		time.Sleep(100 * time.Millisecond) // Retry after a short delay
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.sequenceNumber++
	args := PutAppendArgs{Key: key, Value: value, ClientId: ck.clientId, RequestId: ck.sequenceNumber}

	for {
		switch op {
		case "Put":
			for i := 0; i < len(ck.servers); i++ {
				server := (ck.leader + i) % len(ck.servers)
				var reply PutAppendReply
				ok := ck.servers[server].Call("KVServer.Put", &args, &reply)
				if ok && reply.Err == OK {
					ck.leader = server
					return
				} else if ok && reply.Err == ErrWrongLeader {
					ck.leader = (server + 1) % len(ck.servers)
				}
			}
		case "Append":
			for i := 0; i < len(ck.servers); i++ {
				server := (ck.leader + i) % len(ck.servers)
				var reply PutAppendReply
				ok := ck.servers[server].Call("KVServer.Append", &args, &reply)
				if ok && reply.Err == OK {
					ck.leader = server
					return
				} else if ok && reply.Err == ErrWrongLeader {
					ck.leader = (server + 1) % len(ck.servers)
				}
			}
		}

		time.Sleep(100 * time.Millisecond) // Retry after a short delay
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
